import asyncio
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
)
from .metastore.manager import MetastoreManager

class ControllerServer:
    def __init__(self, host: str, port: int, replicas_per_topic=3, heartbeat_timeout=15.0):
        self.host = host
        self.port = port
        self.manager = MetastoreManager(replicas_per_topic=replicas_per_topic)
        self.heartbeat_timeout = heartbeat_timeout
        self.active_brokers = {}  # broker_id -> (writer, last_heartbeat_time, host, port)
        self.server = None
        self.running = False
        self._tasks = set()

    async def broadcast_metadata(self):
        all_topics = await self.manager.get_all_topics()
        meta_list = []
        for t in all_topics:
            leader_node = await self.manager.get_node(t.leader_id)
            leader_addr = leader_node.host if leader_node else "0.0.0.0"
            leader_port = leader_node.port if leader_node else 0
            
            meta_list.append(TopicMetaDataResponse(
                topic=t.topic,
                leader_id=t.leader_id,
                leader_addr=leader_addr,
                leader_port=leader_port,
                replica_list=t.replica_list,
                version=t.version
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

    async def _handle_broker_disconnect(self, broker_id: str):
        if broker_id in self.active_brokers:
            print(f"Broker {broker_id} disconnected or heartbeat timed out")
            writer = self.active_brokers[broker_id][0]
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            
            updated_topics = await self.manager.deregister_broker(broker_id)
            self.active_brokers.pop(broker_id, None)
            
            # Always broadcast to inform other brokers
            await self.broadcast_metadata()

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
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
                    print(f"Broker {broker_id} registered at {cmd.host}:{cmd.port}")
                    self.active_brokers[broker_id] = (writer, asyncio.get_event_loop().time(), cmd.host, cmd.port)
                    await self.manager.register_broker(broker_id, cmd.host, cmd.port)
                    
                    # Send initial metadata
                    all_topics = await self.manager.get_all_topics()
                    meta_list = []
                    for t in all_topics:
                        leader_node = await self.manager.get_node(t.leader_id)
                        leader_addr = leader_node.host if leader_node else "0.0.0.0"
                        leader_port = leader_node.port if leader_node else 0
                        meta_list.append(TopicMetaDataResponse(
                            topic=t.topic,
                            leader_id=t.leader_id,
                            leader_addr=leader_addr,
                            leader_port=leader_port,
                            replica_list=t.replica_list,
                            version=t.version
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
                            self.active_brokers[broker_id][3]
                        )
                    resp = ControllerPingResponse()
                    frame = encode_response(resp)
                    writer.write(frame.header)
                    await writer.drain()
                    
                elif isinstance(cmd, RegisterTopicCommand):
                    print(f"Register topic command received for: {cmd.topic}")
                    topic_meta = await self.manager.register_topic(cmd.topic)
                    
                    leader_node = await self.manager.get_node(topic_meta.leader_id)
                    leader_addr = leader_node.host if leader_node else "0.0.0.0"
                    leader_port = leader_node.port if leader_node else 0
                    
                    resp = TopicMetaDataResponse(
                        topic=topic_meta.topic,
                        leader_id=topic_meta.leader_id,
                        leader_addr=leader_addr,
                        leader_port=leader_port,
                        replica_list=topic_meta.replica_list,
                        version=topic_meta.version
                    )
                    frame = encode_response(resp)
                    writer.write(frame.header)
                    await writer.drain()
                    
                    await self.broadcast_metadata()
                    
                elif isinstance(cmd, MetadataRequestCommand):
                    topic_meta = await self.manager.get_topic_metadata(cmd.topic)
                    if topic_meta:
                        leader_node = await self.manager.get_node(topic_meta.leader_id)
                        leader_addr = leader_node.host if leader_node else "0.0.0.0"
                        leader_port = leader_node.port if leader_node else 0
                        resp = TopicMetaDataResponse(
                            topic=topic_meta.topic,
                            leader_id=topic_meta.leader_id,
                            leader_addr=leader_addr,
                            leader_port=leader_port,
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
                    
        except Exception as e:
            traceback.print_exc()
        finally:
            if broker_id:
                await self._handle_broker_disconnect(broker_id)
            else:
                writer.close()
                await writer.wait_closed()

    async def start(self):
        self.running = True
        self.server = await asyncio.start_server(self.handle_connection, self.host, self.port)
        self._tasks.add(asyncio.create_task(self._heartbeat_checker_loop()))
        print(f"Controller server started on {self.host}:{self.port}")

    async def _heartbeat_checker_loop(self):
        while self.running:
            await asyncio.sleep(1.0)
            now = asyncio.get_event_loop().time()
            stale_brokers = []
            for broker_id, (_, last_heartbeat, _, _) in list(self.active_brokers.items()):
                if now - last_heartbeat > self.heartbeat_timeout:
                    stale_brokers.append(broker_id)
            for b_id in stale_brokers:
                await self._handle_broker_disconnect(b_id)

    async def close(self):
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for task in self._tasks:
            task.cancel()
        for broker_id, (writer, _, _, _) in list(self.active_brokers.items()):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        self.active_brokers.clear()
