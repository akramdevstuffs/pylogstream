import asyncio
import logging
import traceback
from pylogstream_protocol.framer import PREFIX_SIZE, decode_length
from pylogstream_protocol.parser import parse_command
from pylogstream_protocol.encoder import encode_response
from pylogstream_protocol.commands import (
    RegisterTopicCommand,
    MetadataRequestCommand,
    BrokerRegisterCommand,
    ControllerPingCommand,
    ISRChangeCommand,
)
from pylogstream_protocol.response import (
    TopicMetaDataResponse,
    TopicMetaDataListResponse,
    ControllerPingResponse,
    ErrorResponse,
    NotLeaderControllerResponse,
)
from .metastore.manager import MetastoreManager
from .metastore.storage import ZKMetastoreStorage
from .zk.client import ZKClient, NodeExistsError

logger = logging.getLogger(__name__)

_CONTROLLER_PATH = "/pylogstream/controller"


class ControllerServer:
    def __init__(
        self,
        host: str,
        port: int,
        replicas_per_topic: int = 3,
        heartbeat_timeout: float = 15.0,
        zk_hosts: str | None = None,
        controller_id: str = "",
    ):
        self.host = host
        self.port = port
        self.heartbeat_timeout = heartbeat_timeout
        self.controller_id = controller_id or f"{host}:{port}"

        # ZooKeeper — optional; if zk_hosts is None the server runs without ZK
        # (backward-compatible with existing unit tests).
        self._zk_hosts = zk_hosts
        self._zk: ZKClient | None = None

        # Leader-election state
        self.is_leader: bool = True   # True when running without ZK
        self._election_lock = asyncio.Lock()

        # Metastore — ZK-backed in production, in-memory without ZK
        self.manager = MetastoreManager(replicas_per_topic=replicas_per_topic)

        self.active_brokers: dict[str, tuple] = {}  # broker_id -> (writer, last_heartbeat, host, port)
        self.server = None
        self.running = False
        self._tasks: set[asyncio.Task] = set()

    # ------------------------------------------------------------------
    # Leader election
    # ------------------------------------------------------------------

    async def _run_election(self) -> None:
        """Try to acquire the /controller ephemeral node.

        If the node already exists another controller is the leader; we watch
        for its deletion and re-run the election when it disappears.
        """
        async with self._election_lock:
            data = {
                "controller_id": self.controller_id,
                "host": self.host,
                "port": self.port,
            }
            try:
                await self._zk.create_ephemeral(_CONTROLLER_PATH, data)
                self.is_leader = True
                logger.info("[%s] Elected as leader", self.controller_id)
            except NodeExistsError:
                self.is_leader = False
                leader = await self.get_leader()
                logger.info(
                    "[%s] Not leader — current leader: %s",
                    self.controller_id,
                    leader,
                )
                # Watch for leader departure so we can re-run election
                self._zk.watch_exists(_CONTROLLER_PATH, self._on_leader_gone)

    async def _on_leader_gone(self) -> None:
        """Called (from a ZK watcher thread via run_coroutine_threadsafe) when
        the /controller node is deleted.  Re-run the election."""
        logger.info("[%s] Leader node gone — starting re-election", self.controller_id)
        # Mark as follower first so handle_connection rejects new connections
        self.is_leader = False
        await self._broadcast_not_leader()
        await self._run_election()

    async def _broadcast_not_leader(self) -> None:
        """Send NotLeaderResponse to every connected broker and close their
        connections. Called when this controller loses leadership."""
        leader = await self.get_leader()
        resp = NotLeaderControllerResponse(
            leader_id=leader.get("controller_id", "") if leader else "",
            leader_host=leader.get("host", "") if leader else "",
            leader_port=leader.get("port", 0) if leader else 0,
        )
        frame = encode_response(resp)

        broker_ids = list(self.active_brokers.keys())
        for broker_id in broker_ids:
            entry = self.active_brokers.pop(broker_id, None)
            if entry is None:
                continue
            writer = entry[0]
            try:
                writer.write(frame.header)
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            logger.info("Sent NotLeader to broker %s and closed connection", broker_id)

    async def get_leader(self) -> dict | None:
        """Return the current leader's info dict, or None if no leader."""
        if self._zk is None:
            return {"controller_id": self.controller_id, "host": self.host, "port": self.port}
        return await self._zk.get(_CONTROLLER_PATH)

    # ------------------------------------------------------------------
    # Broadcast helpers
    # ------------------------------------------------------------------

    async def broadcast_metadata(self) -> None:
        all_topics = await self.manager.get_all_topics()
        meta_list = []
        for t in all_topics:
            leader_node = await self.manager.get_node(t.leader_id)
            meta_list.append(TopicMetaDataResponse(
                topic=t.topic,
                leader_id=t.leader_id,
                leader_addr=leader_node.host if leader_node else "0.0.0.0",
                leader_port=leader_node.port if leader_node else 0,
                replica_list=t.replica_list,
                version=t.version,
            ))

        resp = TopicMetaDataListResponse(meta_list=meta_list)
        frame = encode_response(resp)

        disconnected = []
        for broker_id, (writer, _, _, _) in list(self.active_brokers.items()):
            try:
                writer.write(frame.header)
                if frame.payload is not None:
                    writer.write(frame.payload)
                await writer.drain()
            except Exception:
                disconnected.append(broker_id)

        for b_id in disconnected:
            await self._handle_broker_disconnect(b_id)

    # ------------------------------------------------------------------
    # Broker connection lifecycle
    # ------------------------------------------------------------------

    async def _handle_broker_disconnect(self, broker_id: str) -> None:
        if broker_id in self.active_brokers:
            logger.info("Broker %s disconnected or heartbeat timed out", broker_id)
            writer = self.active_brokers[broker_id][0]
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            await self.manager.deregister_broker(broker_id)
            self.active_brokers.pop(broker_id, None)
            await self.broadcast_metadata()

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Non-leaders reject incoming broker connections immediately
        if not self.is_leader:
            leader = await self.get_leader()
            resp = NotLeaderControllerResponse(
                leader_id=leader.get("controller_id", "") if leader else "",
                leader_host=leader.get("host", "") if leader else "",
                leader_port=leader.get("port", 0) if leader else 0,
            )
            frame = encode_response(resp)
            try:
                writer.write(frame.header)
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            return

        broker_id = None
        try:
            while self.running:
                try:
                    prefix = await asyncio.wait_for(reader.readexactly(PREFIX_SIZE), timeout=self.heartbeat_timeout)
                    length = decode_length(prefix)
                    data = await asyncio.wait_for(reader.readexactly(length), timeout=5.0)
                except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionResetError):
                    break

                cmd = parse_command(data)

                if isinstance(cmd, BrokerRegisterCommand):
                    broker_id = cmd.broker_id
                    logger.info("Broker %s registered at %s:%s", broker_id, cmd.host, cmd.port)
                    self.active_brokers[broker_id] = (writer, asyncio.get_event_loop().time(), cmd.host, cmd.port)
                    await self.manager.register_broker(broker_id, cmd.host, cmd.port)

                    all_topics = await self.manager.get_all_topics()
                    meta_list = []
                    for t in all_topics:
                        leader_node = await self.manager.get_node(t.leader_id)
                        meta_list.append(TopicMetaDataResponse(
                            topic=t.topic,
                            leader_id=t.leader_id,
                            leader_addr=leader_node.host if leader_node else "0.0.0.0",
                            leader_port=leader_node.port if leader_node else 0,
                            replica_list=t.replica_list,
                            version=t.version,
                        ))
                    resp = TopicMetaDataListResponse(meta_list=meta_list)
                    frame = encode_response(resp)
                    writer.write(frame.header)
                    if frame.payload is not None:
                        writer.write(frame.payload)
                    await writer.drain()
                    await self.broadcast_metadata()

                elif isinstance(cmd, ControllerPingCommand):
                    if broker_id in self.active_brokers:
                        self.active_brokers[broker_id] = (
                            writer,
                            asyncio.get_event_loop().time(),
                            self.active_brokers[broker_id][2],
                            self.active_brokers[broker_id][3],
                        )
                    resp = ControllerPingResponse()
                    frame = encode_response(resp)
                    writer.write(frame.header)
                    await writer.drain()

                elif isinstance(cmd, RegisterTopicCommand):
                    logger.info("Register topic: %s", cmd.topic)
                    topic_meta = await self.manager.register_topic(cmd.topic)
                    leader_node = await self.manager.get_node(topic_meta.leader_id)
                    resp = TopicMetaDataResponse(
                        topic=topic_meta.topic,
                        leader_id=topic_meta.leader_id,
                        leader_addr=leader_node.host if leader_node else "0.0.0.0",
                        leader_port=leader_node.port if leader_node else 0,
                        replica_list=topic_meta.replica_list,
                        version=topic_meta.version,
                    )
                    frame = encode_response(resp)
                    writer.write(frame.header)
                    await writer.drain()
                    await self.broadcast_metadata()

                elif isinstance(cmd, MetadataRequestCommand):
                    topic_meta = await self.manager.get_topic_metadata(cmd.topic)
                    if topic_meta:
                        leader_node = await self.manager.get_node(topic_meta.leader_id)
                        resp = TopicMetaDataResponse(
                            topic=topic_meta.topic,
                            leader_id=topic_meta.leader_id,
                            leader_addr=leader_node.host if leader_node else "0.0.0.0",
                            leader_port=leader_node.port if leader_node else 0,
                            replica_list=topic_meta.replica_list,
                            version=topic_meta.version,
                        )
                    else:
                        resp = ErrorResponse(code=404, message=f"Topic {cmd.topic} not found")
                    frame = encode_response(resp)
                    writer.write(frame.header)
                    await writer.drain()

                elif isinstance(cmd, ISRChangeCommand):
                    await self.manager.update_topic_isr(cmd.topic, cmd.isr_list)

                else:
                    resp = ErrorResponse(code=400, message="Invalid command for controller")
                    frame = encode_response(resp)
                    writer.write(frame.header)
                    await writer.drain()

        except Exception:
            traceback.print_exc()
        finally:
            if broker_id:
                await self._handle_broker_disconnect(broker_id)
            else:
                writer.close()
                await writer.wait_closed()

    # ------------------------------------------------------------------
    # Startup / shutdown
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._zk_hosts:
            self._zk = ZKClient(self._zk_hosts)
            await self._zk.connect()

            # Wire up ZK-backed storage
            zk_storage = ZKMetastoreStorage(self._zk)
            await zk_storage.setup()
            self.manager = MetastoreManager(
                replicas_per_topic=self.manager.replicas_per_topic,
                storage=zk_storage,
            )

            await self._run_election()

        self.running = True
        self.server = await asyncio.start_server(self.handle_connection, self.host, self.port)
        self._tasks.add(asyncio.create_task(self._heartbeat_checker_loop()))
        logger.info(
            "Controller server started on %s:%s (leader=%s)",
            self.host, self.port, self.is_leader,
        )

    async def _heartbeat_checker_loop(self) -> None:
        while self.running:
            await asyncio.sleep(1.0)
            now = asyncio.get_event_loop().time()
            stale = [
                bid for bid, (_, last_hb, _, _) in list(self.active_brokers.items())
                if now - last_hb > self.heartbeat_timeout
            ]
            for b_id in stale:
                await self._handle_broker_disconnect(b_id)

    async def close(self) -> None:
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for task in self._tasks:
            task.cancel()
        for _, (writer, _, _, _) in list(self.active_brokers.items()):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        self.active_brokers.clear()
        if self._zk:
            await self._zk.close()
