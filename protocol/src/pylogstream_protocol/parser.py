from .commands import *
from .response import *
from .error import ChecksumFailed, UnknownCommand
import zlib

def parse_command(data: bytes, checksum_enable=False):

    cmd = data[:3].decode()

    if cmd == "REG":
        return RegisterCommand()

    if cmd == "CID":
        msg =data.decode()
        return ClientIdCommand(msg.split(" ", 1)[1])

    if cmd=="SUB":
        topic = data.decode().split(" ", 1)[-1]
        return SubscribeCommand(topic)
    
    if cmd == "CMT":
        msg =data.decode()
        parts = msg.split(" ", 2)
        return CommitOffsetCommand(parts[1], int(parts[2]))
    
    if cmd=="FCH":
        msg = data.decode()
        parts = msg.split(" ", 1)
        return FetchOffsetCommand(parts[1])
    
    if cmd=='PUB':
        msg = data.decode()
        parts = msg.split(" ", 2)
        topic = parts[1]
        payload = parts[2]
        if checksum_enable:
            payload_parts = payload.split(" ",1)
            hash = int(payload_parts[0])
            msg = payload_parts[1].encode()
            if not checksum_verify(msg, hash):
                raise ChecksumFailed(msg, hash)
        return PublishCommand(topic, payload.encode(), checksum_enable)

    if cmd=="PUL":
        msg = data.decode()
        parts = msg.split(" ", 3)
        topic = parts[1]
        offset = parts[2]
        size = parts[3]
        return PullCommand(topic, int(offset), int(size))

    if cmd=='PNG':
        return PingCommand()

    raise UnknownCommand(cmd)

def parse_response(data: bytes):

    msg = data.decode()
    cmd = msg[:3]

    if cmd == "CID":
        parts = msg.split(" ", 1)
        return ClientIDResponse(parts[1])

    if cmd == "PNG":
        return PingResponse()

    if cmd == "ACK":
        parts = msg.split(" ", 2)
        return PubAckResponse(parts[1], int(parts[2]))

    if cmd == "OAK":
        parts = msg.split(" ", 2)
        return OffsetAckResponse(parts[1], int(parts[2]))

    if cmd == "ERR":
        parts = msg.split(" ", 2)
        message = parts[2] if len(parts) > 2 else ""
        return ErrorResponse(int(parts[1]), message)

    if cmd == "MSG":
        parts = msg.split(" ", 2)
        topic = parts[1]
        payload_len = int(parts[2])

        return MessageResponseHeader(topic, payload_len)

    raise UnknownCommand(cmd)

def parse_records(payload: bytes):

    i = 0
    n = len(payload)

    while i < n:
        length = int.from_bytes(payload[i:i+4], "big")
        i += 4

        record = payload[i:i+length]
        i += length

        yield record

def decode_record(record: bytes) -> tuple[int,str]:

    parts = record.split(b" ", 1)
    checksum = int(parts[0])
    message = parts[1].decode() if len(parts) > 1 else ""

    if not checksum_verify(message, checksum):
        raise RuntimeError("Message integrity failed")

    return checksum, message

def checksum_verify(msg_bytes, checksum) -> bool:
    curr = zlib.crc32(msg_bytes)
    return curr == checksum