import asyncio
import zlib
from pylogstream_protocol.encoder import encode_command
from pylogstream_protocol.framer import PREFIX_SIZE, decode_length
from pylogstream_protocol.parser import parse_response

from pylogstream_protocol.commands import (
    RegisterCommand,
    PublishCommand,
    CommitOffsetCommand,
    PingCommand,
)

from pylogstream_protocol.response import (
    ClientIDResponse,
    PubAckResponse,
    OffsetAckResponse
)

HOST = "127.0.0.1"
PORT = 9092


async def send_command(writer, cmd):
    frame = encode_command(cmd)
    writer.write(frame.header)

    if frame.payload:
        writer.write(frame.payload)

    await writer.drain()


async def read_response(reader: asyncio.StreamReader):
    prefix = await reader.readexactly(PREFIX_SIZE)
    data = await reader.readexactly(decode_length(prefix))
    resp = parse_response(data)
    return resp


async def main():
    reader, writer = await asyncio.open_connection(HOST, PORT)

    print("Connected to broker")

    # 1 Register client
    await send_command(writer, RegisterCommand())
    resp = await read_response(reader)

    assert isinstance(resp, ClientIDResponse)

    client_id = resp.client_id
    print("Client ID:", client_id)

    # 2 Publish message
    msg = "hello logstream"
    hash = zlib.crc32(msg.encode())

    payload = f"{hash} {msg}".encode()

    await send_command(
        writer,
        PublishCommand(
            topic="test-topic",
            payload=payload,
            acks=-1,
            contains_checksum=True
        )
    )

    resp = await read_response(reader)
    assert isinstance(resp, PubAckResponse)
    offset = resp.offset

    print("Published at offset:", offset)

    # 3 Commit offset
    await send_command(
        writer,
        CommitOffsetCommand(
            topic="test-topic",
            offset=offset,
            acks=1
        )
    )

    resp = await read_response(reader)
    assert isinstance(resp, OffsetAckResponse)
    print("Offset committed:", resp.offset)

    # 4 Heartbeat
    await send_command(writer, PingCommand())
    resp = await read_response(reader)

    print("Heartbeat response:", resp)

    writer.close()
    await writer.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())