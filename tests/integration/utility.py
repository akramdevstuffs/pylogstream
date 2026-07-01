import asyncio
from pylogstream_protocol import commands, parser, framer, response, encoder

async def read_response(reader: asyncio.StreamReader) -> response.Response:
    data = await reader.readexactly(framer.PREFIX_SIZE)
    length = framer.decode_length(data)
    response_data = await reader.readexactly(length)
    return parser.parse_response(response_data)

async def write_command(writer: asyncio.StreamWriter, cmd: commands.Command):
    encoded_cmd = encoder.encode_command(cmd, checksum_enable=True)
    writer.write(encoded_cmd.header)
    writer.write(encoded_cmd.payload or b"")
    await writer.drain()