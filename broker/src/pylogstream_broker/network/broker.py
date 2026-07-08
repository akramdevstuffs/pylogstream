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
from typing import Dict
from collections import defaultdict
import asyncio
import socket
import uuid
import time
import contextlib

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

        self.controller_connected: bool = False

        self._client_tasks = set()

        self.running = False
    
    async def _handle_controller(self):
        controller_host = self.config.broker.controller_host
        controller_port = self.config.broker.controller_port
        
        while self.running:
            try:
                reader, writer = await asyncio.open_connection(controller_host, controller_port)
                
                # Register self
                reg_cmd = BrokerRegisterCommand(
                    broker_id=self.__replica_manager.broker_id,
                    host=self.config.broker.host,
                    port=self.config.broker.port
                )
                frame = encode_command(reg_cmd)
                writer.write(frame.header)
                if frame.payload is not None:
                    writer.write(frame.payload)
                await writer.drain()

                resp = await reader.readexactly(PREFIX_SIZE)
                length = decode_length(resp)
                data = await reader.readexactly(length)
                resp = parse_response(data)
                if not isinstance(resp, TopicMetaDataListHeaderResponse):
                    writer.close()
                    await writer.wait_closed()
                    await asyncio.sleep(2.0)
                    continue
                # Fetch the metadata list payload
                payload = await reader.readexactly(resp.payload_length)
                meta_list = parse_metadata_list_payload(payload)
                await self.__replica_manager.apply_metadata_list(TopicMetaDataListResponse(meta_list=meta_list))
                
                heartbeat_task = asyncio.create_task(self._controller_heartbeat_loop(writer))

                # Subscribe to on_update_isr
                async def on_isr_change(topic: str, isr_list: list[str]):
                    isr_cmd = ISRChangeCommand(topic=topic, isr_list=isr_list)
                    frame = encode_command(isr_cmd)
                    try:
                        writer.write(frame.header)
                        if frame.payload is not None:
                            writer.write(frame.payload)
                        await writer.drain()
                    except Exception:
                        pass
                
                self.__replica_manager.on_isr_change = on_isr_change

                self.controller_connected = True
                
                try:
                    while self.running:
                        prefix = await reader.readexactly(PREFIX_SIZE)
                        length = decode_length(prefix)
                        data = await reader.readexactly(length)
                        
                        resp = parse_response(data)
                        
                        if isinstance(resp, TopicMetaDataListHeaderResponse):
                            payload = await reader.readexactly(resp.payload_length)
                            meta_list = parse_metadata_list_payload(payload)
                            await self.__replica_manager.apply_metadata_list(
                                TopicMetaDataListResponse(meta_list=meta_list)
                            )
                        elif isinstance(resp, TopicMetaDataResponse):
                            await self.__replica_manager.apply_metadata(resp)
                        elif isinstance(resp, ControllerPingResponse):
                            pass
                except (asyncio.IncompleteReadError, ConnectionResetError, asyncio.CancelledError):
                    pass
                finally:
                    self.__replica_manager.on_isr_change = None
                    heartbeat_task.cancel()
                    with contextlib.suppress(Exception):
                        await heartbeat_task
                    writer.close()
                    with contextlib.suppress(Exception):
                        await writer.wait_closed()
                    
                    self.controller_connected = False
            except Exception:
                pass
            await asyncio.sleep(2.0)

    async def _controller_heartbeat_loop(self, writer: asyncio.StreamWriter):
        ping_cmd = ControllerPingCommand(broker_id=self.__replica_manager.broker_id)
        frame = encode_command(ping_cmd)
        while self.running:
            await asyncio.sleep(3.0)
            try:
                writer.write(frame.header)
                await writer.drain()
            except Exception:
                break

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

    async def start_server(self):

        self.running = True
        self.controller_task = asyncio.create_task(self._handle_controller())

        self.server = await asyncio.start_server(
            self.handle_client,
            self.config.broker.host,
            self.config.broker.port
        )
        for sock in self.server.sockets:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        addrs = ', '.join(str(sock.getsockname()) for sock in self.server.sockets)
        print(f'Serving on {addrs}')

        try:
            await self.server.serve_forever()
        except asyncio.CancelledError:
            # Excpected during shutdown
            pass
    
    async def close(self) -> None:
        self.running = False
        self.controller_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.controller_task

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