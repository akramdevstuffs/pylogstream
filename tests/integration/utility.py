import asyncio
import socket
from pylogstream_protocol import commands, parser, framer, response, encoder
import pytest

async def read_response(reader: asyncio.StreamReader, timeout: float = 30.0) -> response.Response:
    try:
        data = await asyncio.wait_for(reader.readexactly(framer.PREFIX_SIZE), timeout=timeout)
        length = framer.decode_length(data)
        response_data = await asyncio.wait_for(reader.readexactly(length), timeout=timeout)
        return parser.parse_response(response_data)
    except asyncio.TimeoutError:
        raise pytest.fail("Timeout waiting for response")

async def write_command(writer: asyncio.StreamWriter, cmd: commands.Command):
    encoded_cmd = encoder.encode_command(cmd, checksum_enable=True)
    writer.write(encoded_cmd.header)
    writer.write(encoded_cmd.payload or b"")
    await writer.drain()

def get_free_port() -> int:
    """Get a free port from the OS."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 0))
        return s.getsockname()[1]