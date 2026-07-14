from pylogstream_protocol.parser import parse_response
from pylogstream_broker.config import load_config, Config, BrokerConfig
from pylogstream_broker.network.writer import Writer, WriteRequest
from pylogstream_protocol.error import (
    ChecksumFailed as ChecksumFailedError, 
    UnknownCommand as UnknownCommandError
)  
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
    ReplicaFetchCommand,
    BrokerRegisterCommand,
    ControllerPingCommand,
    ISRChangeCommand
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
    ErrorResponse,
    MmapResponse,
    ControllerResponse,
    TopicMetaDataListResponse,
    TopicMetaDataResponse,
    TopicMetaDataListHeaderResponse,
    ControllerPingResponse,
)
from pylogstream_protocol.encoder import encode_response, encode_command, EncodedFrame
from pylogstream_protocol.parser import parse_command, parse_metadata_list_payload 
from pylogstream_broker.log.log_manager import LogManager
from pylogstream_broker.replication.manager import ReplicaManager
from pylogstream_broker.network.controller import ControllerConnection
from pylogstream_protocol.commands import RegisterTopicCommand, MetadataRequestCommand
from typing import Dict
from collections import defaultdict
import asyncio
import socket
import uuid
import time
import contextlib

class Broker:

    def __init__(self, config: Config, log_manager: LogManager|None = None, writer: Writer|None = None, replica_manager: ReplicaManager|None = None):
        self.config: Config = config

        self.log_manager = log_manager if log_manager is not None else LogManager(config.log)

        self.writer = writer if writer is not None else Writer(self.log_manager, self.config.broker.writer_config)

        self.__client_heartbeats: Dict[str, float] = defaultdict(float)

        self._replica_manager = replica_manager if replica_manager is not None else ReplicaManager(
            log_manager=self.log_manager,
            config=self.config.replica
        )
        self.__replica_manager = self._replica_manager

        self.controller: ControllerConnection|None = None

        self._client_tasks = set()

        self.running = False
        self._ready: asyncio.Event = asyncio.Event()
    
    @property
    def ready(self) -> bool:
        return self._ready.is_set()
    
    async def wait_ready(self):
        await self._ready.wait()

    @property
    def controller_connected(self) -> bool:
        return self.controller is not None and self.controller.connected


    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):

        self._client_tasks.add((asyncio.current_task(), writer))

        client_id = None
        sock = writer.get_extra_info("socket")
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while self.running:
            try:
                prefix = await asyncio.wait_for(reader.readexactly(PREFIX_SIZE), timeout=30)
                data = await asyncio.wait_for(reader.readexactly(decode_length(prefix)), timeout=5.0)
            except asyncio.exceptions.IncompleteReadError:
                # TODO: Add proper cleanup here for client disconnection
                # check if connection is alive
                if not reader.at_eof():
                    resp = ErrorResponse(code=408, message="Request timeout")
                    await self._send_response(resp, writer)
                break
            except asyncio.exceptions.TimeoutError:
                resp = ErrorResponse(code=408, message="Request timeout")
                await self._send_response(resp, writer)
                break
            except ConnectionResetError:
                break
            except Exception as e:
                print(f"Unexpected error: {e}")
                break
            try:
                command = parse_command(data, self.config.broker.checksum_enable)
            except ChecksumFailedError:
                resp = ErrorResponse(code=400, message="Checksum failed")
                await self._send_response(resp, writer)
                break
            except UnknownCommandError:
                resp = ErrorResponse(code=400, message="Unknown command")
                await self._send_response(resp, writer)
                break

            if isinstance(command, ReplicaCommand):
                try:
                    resp = self.__replica_manager.handle(command)
                except Exception:
                    # TODO: Implement error catching and give them to client
                    break
                if resp is not None:
                    await self._send_response(resp, writer)
            elif isinstance(command, RegisterTopicCommand):
                if self.controller:
                    resp = await self.controller.register_topic(command.topic)
                    if resp is None:
                        resp = ErrorResponse(code=500, message="Failed to register topic")
                    await self._send_response(resp, writer)
                else:
                    resp = ErrorResponse(code=500, message="Controller not connected")
                    await self._send_response(resp, writer)
            elif isinstance(command, MetadataRequestCommand):
                if self.controller:
                    resp = await self.controller.request_metadata(command.topic)
                    if resp is None:
                        resp = ErrorResponse(code=500, message="Failed to get metadata")
                    await self._send_response(resp, writer)
                else:
                    resp = ErrorResponse(code=500, message="Controller not connected")
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
                break
            elif isinstance(command, SubscribeCommand):
                pass
            elif isinstance(command, PullCommand):
                # Check if offset is below high watermark
                # TODO: Add proper error handling here
                try:
                    leader = self.__replica_manager.get_leader(command.topic)
                except Exception:
                    # TODO: modify the get_leader method to raise specific exception
                    err = ErrorResponse(code=400, message="Topic not found or not leader")
                    await self._send_response(err, writer)
                    continue
                high_watermark = leader.get_high_watermark()
                if command.offset > high_watermark:
                    # TODO: Add proper error handling here
                    err = ErrorResponse(code=400, message=f"Offset out of range. High watermark is {high_watermark}")
                    await self._send_response(err, writer)
                    continue
                elif command.offset == high_watermark:
                    # Send an empty response
                    resp = MessageResponse(topic=command.topic, payload=b"")
                    await self._send_response(resp, writer)
                    continue

                await self._push_message(command,writer)
            elif isinstance(command, FetchOffsetCommand):
                # TODO: Add fetch offset logic
                offset = 0
                resp = FetchOffsetResponse(command.topic, offset)
                await self._send_response(resp,writer)
            # For setting offsets from clients side
            elif isinstance(command, CommitOffsetCommand):
                payload = f"{time.time()} {client_id} {command.topic} {command.offset}".encode()
                log_offset = await self._handle_write(client_id, command.topic, payload, command.acks)

                if command.acks == 0:
                    continue
                resp = OffsetAckResponse(topic=command.topic, acks=command.acks, offset=command.offset)
                await self._send_response(resp,writer)
            elif isinstance(command, PublishCommand):

                try:
                    offset = await self._handle_write(
                        client_id=client_id,
                        topic=command.topic,
                        payload=command.payload,
                        acks=command.acks
                    )
                except ValueError:
                    err = ErrorResponse(code=400, message="Topic not found or not leader")
                    await self._send_response(err, writer)
                    continue
                if command.acks == 0 or offset is None:
                    continue
                resp = PubAckResponse(topic=command.topic, offset=offset,acks=command.acks)
                await self._send_response(resp,writer)
            # Heart beat from client
            elif isinstance(command, PingCommand):
                # TODO: Add heartbeats save logic
                #client_heartbeats[client_id] = time.time()
                self.__client_heartbeats[client_id] = time.time()
                resp = PingResponse()
                await self._send_response(resp,writer)
            
            else:
                # TODO: Handle unimplemented command
                err = ErrorResponse(code=403, message="Unimplemented command")
                await self._send_response(err, writer)

        self._client_tasks.discard((asyncio.current_task(), writer))
        writer.close()
        await writer.wait_closed()
    
    async def _handle_write(self, 
                            client_id: str,
                            topic:str,
                            payload: bytes,
                            acks: int
                            ):

        try:
            leader = self.__replica_manager.get_leader(topic)
        except Exception as e:
            # TODO: modify the get_leader method to raise specific exception
            raise ValueError(f"Topic doesn't found")

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
            with self.log_manager.read(cmd.topic, cmd.offset, cmd.size) as res:
                
                # Prioritise sendfile method
                resp = FileResponse(
                    cmd.topic, 
                    res.file_slice.filepath, 
                    res.file_slice.offset, 
                    res.file_slice.batch_size
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

            with open(frame.file_path, "rb") as f:
                await asyncio.get_running_loop().sendfile(writer.transport, f, frame.offset, frame.length)

        await writer.drain()

    async def _create_controller_connection(self) -> ControllerConnection:
        if not self.config.broker.controller_list:
            raise ValueError("Controller list is empty")
        return await ControllerConnection.connect(self.config.broker, self.__replica_manager)

    async def start_server(self):

        self.running = True
        if self.config.broker.controller_list:
            self.controller = await self._create_controller_connection()
        else:
            print("No controller configured, running in standalone mode")

        self.server = await asyncio.start_server(
            self.handle_client,
            self.config.broker.host,
            self.config.broker.port
        )
        for sock in self.server.sockets:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        addrs = ', '.join(str(sock.getsockname()) for sock in self.server.sockets)
        print(f'Serving on {addrs}')

        self._ready.set()

        try:
            await self.server.serve_forever()
        except asyncio.CancelledError:
            # Excpected during shutdown
            pass
    
    async def close(self) -> None:
        self.running = False
        if self.controller:
            await self.controller.close()
            print('Controller connection closed')

        self.server.close()

        for (task,writer) in self._client_tasks:
            writer.close()
            task.cancel()
        
        for (task,_) in self._client_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        await self.server.wait_closed()
        await self.writer.close()
        await self.__replica_manager.close_all()


def main() -> None:
    config = load_config("config.yaml")
    broker = Broker(config=config)
    asyncio.run(broker.start_server())