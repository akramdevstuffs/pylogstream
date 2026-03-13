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
    PingCommand
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
    MmapResponse
)
from pylogstream_protocol.encoder import encode_response, EncodedFrame
from pylogstream_protocol.parser import parse_command 
from pylogstream_broker.log.log_manager import LogManager
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
                #clean up
                return
            command = parse_command(data, self.config.broker.checksum_enable)
            if isinstance(command, RegisterCommand):
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
                await self._push_message(command,writer)
            elif isinstance(command, FetchOffsetCommand):
                # TODO: Add fetch offset logic
                offset = 0
                resp = FetchOffsetResponse(command.topic, offset)
                await self._send_response(resp,writer)
            # For setting offsets from clients side
            elif isinstance(command, CommitOffsetCommand):
                fut = asyncio.get_running_loop().create_future()
                payload = f"{time.time()} {client_id} {command.topic} {command.offset}".encode()
                req = WriteRequest(
                    client_id,
                    self.config.broker.offset_topic,
                    payload,
                    time.time(),
                    fut
                )
                self.writer.submit(req)
                await fut
                # update_client_offset(client_id, command.topic, command.offset)
                resp = OffsetAckResponse(command.topic, command.offset)
                await self._send_response(resp,writer)
            elif isinstance(command, PublishCommand):

                fut = asyncio.get_running_loop().create_future()
                req = WriteRequest(
                    client_id, 
                    command.topic, 
                    command.payload, 
                    time.time(), 
                    fut
                )

                self.writer.submit(req)
                # Wait for the request to complete
                await fut
                offset = fut.result()
                resp = PubAckResponse(command.topic, offset)
                await self._send_response(resp,writer)
            # Heart beat from client
            elif isinstance(command, PingCommand):
                # TODO: Add heartbeats save logic
                #client_heartbeats[client_id] = time.time()
                self.__client_heartbeats[client_id] = time.time()
                resp = PingResponse()
                await self._send_response(resp,writer)
        
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


def main() -> None:
    config = load_config("config.yaml")
    broker = Broker(config=config)
    asyncio.run(broker.start_server())