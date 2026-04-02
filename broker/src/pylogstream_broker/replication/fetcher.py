from pylogstream_protocol.commands import ReplicaCommand,ReplicaFetchCommand
from pylogstream_protocol.response import ReplicaFetchHeaderResponse
from pylogstream_protocol.encoder import encode_command 
from pylogstream_protocol.framer import decode_length, PREFIX_SIZE
from pylogstream_protocol.parser import parse_response
from pylogstream_broker.log.log_manager import LogManager
import asyncio

class ReplicaFetcher:
    def __init__(self, 
                topic: str, 
                broker_id: str,
                fetch_size: int,
                reader: asyncio.StreamReader,
                writer: asyncio.StreamWriter,
                log_manager: LogManager,
                pool_wait: int
                       ):
        self.topic = topic
        # TODO: Fetch the Log End offset from log
        try:
            self.log_end_offset = self.log_manager.get_latest_offset(topic)
        except KeyError:
            # Topic deosn't exists local log
            self.log_end_offset = 0
        except ValueError:
            # Topic is emtpy
            self.log_end_offset = 0
        self.reader = reader
        self.writer = writer
        self.log_manager = log_manager
        self.fetch_size = fetch_size
        self.broker_id = broker_id
        self.pool_wait = pool_wait

        self._running = True

        asyncio.create_task(self.run())

    @classmethod
    async def create(
            cls,
            topic: str, 
            leader_addr: str, 
            leader_port: int, 
            broker_id: str,
            fetch_size: int,
            log_manager: LogManager,
            pool_wait: int
            ):
        reader, writer = await asyncio.open_connection(leader_addr, leader_port)
        return cls(topic, broker_id,fetch_size, reader, writer, log_manager, pool_wait)
    
    async def close(self):
        self._running = False
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
    
    async def run(self):
        loop = asyncio.get_running_loop()
        while self._running:
            data = await self.fetch_data()

            if data is None or len(data)==0:
                # Reached LEO of the leader wait to pool again
                await asyncio.sleep(self.pool_wait)
                
            # The log offset at which data is appended
            res_offset = await asyncio.to_thread(self.log_manager.append, self.topic, data)
            # Sync it with the local log
            self.log_end_offset = res_offset + len(data)
    
    async def fetch_data(self) -> bytes:
        cmd = ReplicaFetchCommand(self.topic, self.log_end_offset, self.fetch_size, self.broker_id)

        await self._send_command(cmd)

        prefix = await self.reader.readexactly(PREFIX_SIZE)
        length = decode_length(prefix)
        data = await self.reader.readexactly(length)
        header_resp = parse_response(data)
        assert isinstance(header_resp, ReplicaFetchHeaderResponse)
        payload = await self.reader.readexactly(header_resp.payload_length)
        return payload
    
    async def _send_command(self, cmd: ReplicaCommand):
        frame = encode_command(cmd) 
        
        self.writer.write(frame.header)

        if(frame.payload is not None):
            self.writer.write(frame.payload)

        # File send
        if frame.file_path:
            assert frame.offset is not None
            assert frame.length is not None
            # Get the underlying fd of the socket
            with open(frame.file_path, "rb") as f:
                await asyncio.get_running_loop().sendfile(self.writer.transport, f, frame.offset, frame.length)
        await self.writer.drain()