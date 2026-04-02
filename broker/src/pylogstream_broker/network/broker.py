from pylogstream_broker.config import load_config, Config, BrokerConfig
from pylogstream_broker.network.writer import Writer, WriteRequest
from pylogstream_protocol.framer import PREFIX_SIZE, decode_length
from pylogstream_protocol.commands import (
    RegisterCommand,
    ClientIdCommand,
    SubscribeCommand,
    PullCommand,
    FetchOffsetCommand,
    CommitOffsetCommand,
    PublishCommand,
    PingCommand,
    ReplicaCommand,
    ReplicaFetchCommand
)
from pylogstream_protocol.response import (
    Response,
    ClientIDResponse,
    FetchOffsetResponse,
    OffsetAckResponse,
    PubAckResponse,
    PingResponse,
    MessageResponse,
    FileResponse,
    MmapResponse,
    ControllerResponse,
    TopicMetaDataListResponse,
    TopicMetaDataResponse,
)
from pylogstream_protocol.encoder import encode_response, EncodedFrame
from pylogstream_protocol.parser import parse_command 
from pylogstream_broker.log.log_manager import LogManager
from pylogstream_broker.replication.manager import ReplicaManager
from typing import Dict
from collections import defaultdict
import asyncio
import socket
import uuid
import time

class Broker:

    def __init__(self, config: Config):
        self.config: Config = config
        self.log_manager = LogManager(config.log)
        self.writer = Writer(self.log_manager, self.config.broker.writer_config)
        self.__client_heartbeats: Dict[str, float] = defaultdict(lambda:0.0)
        self.__replica_manager: ReplicaManager = ReplicaManager(
            log_manager=self.log_manager,
            config=self.config.replica
        )
        self.running = False
    
    async def _handle_controller(self):
        # WIP: This is a placeholder for the controller communication logic. 
        # The broker will maintain a persistent connection to the controller to receive metadata updates and other control messages. 
        # The actual implementation will depend on the protocol defined for communication between the broker and the controller.
        controller_host = self.config.broker.controller_host
        controller_port = self.config.broker.controller_port
        # TODO: Add retry logic with backoff here for connection to controller
        # reader, writer = await asyncio.open_connection(controller_host, controller_port)
        while self.running:
            # TODO: Implement data fetch from reader then convert it into topic metadata
            # Temp code to mimic fetching
            meta = TopicMetaDataResponse(
                topic='test-topic',
                leader_id='1',
                leader_addr='0.0.0.0',
                leader_port=9092,
                replica_list=['1','2','3'],
                version=1
            )
            await self.__replica_manager.apply_metadata(meta)
            await asyncio.sleep(5)

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client_id = None
        sock = writer.get_extra_info("socket")
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while True:
            try:
                prefix = await reader.readexactly(PREFIX_SIZE)
                data = await reader.readexactly(decode_length(prefix))
            except asyncio.exceptions.IncompleteReadError:
                # TODO: Add proper cleanup here for client disconnection
                return
            command = parse_command(data, self.config.broker.checksum_enable)
            if isinstance(command, ReplicaCommand):
                try:
                    resp = self.__replica_manager.handle(command)
                except Exception:
                    # TODO: Implement error catching and give them to client
                    return
                if resp is not None:
                    await self._send_response(resp, writer)
            elif isinstance(command, RegisterCommand):
                client_id = str(uuid.uuid4())
                self.__client_heartbeats[client_id] = time.time()
                resp = ClientIDResponse(client_id=client_id)
                await self._send_response(resp,writer)
            elif isinstance(command, ClientIdCommand):
                # TODO: Add the cleanup here if user reconnects here
                client_id = command.client_id
                self.__client_heartbeats[client_id] = time.time()
            elif client_id is None:
                # Client must register first
                return
            elif isinstance(command, SubscribeCommand):
                pass
            elif isinstance(command, PullCommand):
                # Check if offset is below high watermark
                # TODO: Add proper error handling here
                leader = self.__replica_manager.get_leader(command.topic)
                if leader is None:
                    # TODO: Send proper error response to client
                    return
                high_watermark = leader.get_high_watermark()
                if command.offset > high_watermark:
                    # TODO: Add proper error handling here
                    return
                await self._push_message(command,writer)
            elif isinstance(command, FetchOffsetCommand):
                # TODO: Add fetch offset logic
                offset = 0
                resp = FetchOffsetResponse(command.topic, offset)
                await self._send_response(resp,writer)
            # For setting offsets from clients side
            elif isinstance(command, CommitOffsetCommand):
                fut = None
                if command.acks != 0:
                    fut = asyncio.get_running_loop().create_future()
                payload = f"{time.time()} {client_id} {command.topic} {command.offset}".encode()
                offset = await self._handle_write(client_id, command.topic, payload, command.acks)
                if command.acks == 0 or offset is None:
                    return
                resp = OffsetAckResponse(command.topic, command.acks, offset)
                await self._send_response(resp,writer)
            elif isinstance(command, PublishCommand):

                offset = await self._handle_write(
                    client_id=client_id,
                    topic=command.topic,
                    payload=command.payload,
                    acks=command.acks
                )
                if command.acks == 0 or offset is None:
                    return
                resp = PubAckResponse(command.topic, command.acks, offset)
                await self._send_response(resp,writer)
            # Heart beat from client
            elif isinstance(command, PingCommand):
                # TODO: Add heartbeats save logic
                #client_heartbeats[client_id] = time.time()
                self.__client_heartbeats[client_id] = time.time()
                resp = PingResponse()
                await self._send_response(resp,writer)
    
    async def _handle_write(self, 
                            client_id: str,
                            topic:str,
                            payload: bytes,
                            acks: int
                            ):
        fut = None

        # Create future only if we need it
        if acks != 0:
            fut = asyncio.get_running_loop().create_future()
        req = WriteRequest(
            client_id,
            topic,
            payload,
            time.time(),
            fut
        )
        self.writer.submit(req)
        if acks == 0 or fut is None:
            return
        await fut
        offset = fut.result()
        if acks == -1:
            # Wait for the offset to be replicated to all in-sync replicas
            leader = self.__replica_manager.get_leader(topic)
            if leader is not None:
                await leader.wait_for_hw(offset)
        return offset
        
    async def _push_message(self, cmd: PullCommand, writer: asyncio.StreamWriter):
        # If message size is less than 1KB directly copy bytes to send
        if(cmd.size <= 1024):
            res = self.log_manager.read_bytes(cmd.topic,cmd.offset,cmd.size)
            resp = MessageResponse(cmd.topic, res.data)
            await self._send_response(resp,writer)
        else:
            with  self.log_manager.read(cmd.topic, cmd.offset, cmd.size) as res:
                ## Note: memoryview only valid till res is in scope
                
                # Prioritise sendfile method
                if(res.file_slice is not None):
                    resp = FileResponse(
                        cmd.topic, 
                        res.file_slice.filepath, 
                        res.file_slice.offset, 
                        res.file_slice.batch_size
                    )
                else:
                    resp = MmapResponse(
                        cmd.topic,
                        res.result
                    )
                await self._send_response(resp,writer)

        
    async def _send_response(self, resp: Response, writer: asyncio.StreamWriter):

        frame = encode_response(resp)

        writer.write(frame.header)

        if(frame.payload is not None):
            writer.write(frame.payload)

        # File send
        if frame.file_path:
            assert frame.offset is not None
            assert frame.length is not None
            # Get the underlying fd of the socket
            with open(frame.file_path, "rb") as f:
                await asyncio.get_running_loop().sendfile(writer.transport, f, frame.offset, frame.length)
        await writer.drain()

    async def start_server(self):

        self.running = True
        self.controller_task = asyncio.create_task(self._handle_controller())

        server = await asyncio.start_server(
            self.handle_client,
            self.config.broker.host,
            self.config.broker.port
        )
        for sock in server.sockets:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
        print(f'Serving on {addrs}')

        async with server:
            await server.serve_forever()
    
    async def close(self) -> None:
        self.running = False
        self.controller_task.cancel()
        await self.controller_task
        await self.writer.close()
        await self.__replica_manager.close_all()


def main() -> None:
    config = load_config("config.yaml")
    broker = Broker(config=config)
    asyncio.run(broker.start_server())