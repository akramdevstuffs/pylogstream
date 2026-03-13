from .response import *
from dataclasses import dataclass
from .commands import *
import zlib
from .framer import encode_frame

@dataclass
class EncodedFrame:
    header: bytes
    payload: bytes| memoryview | None
    file_path: str | None = None
    offset: int | None = None
    length: int | None = None

def encode_response(resp):

    if isinstance(resp,ClientIDResponse):
        header = f"CID {resp.client_id}".encode()
        return EncodedFrame(
            header = encode_frame(header),
            payload=None
        )
    
    if isinstance(resp, PingResponse):
        header = f"PNG".encode()
        return EncodedFrame(
            header = encode_frame(header),
            payload=None
        )
    
    if isinstance(resp, OffsetAckResponse):
        header = f"OAK {resp.topic} {resp.offset}".encode()
        return EncodedFrame(
            header = encode_frame(header),
            payload=None
        )

    if isinstance(resp, PubAckResponse):

        header = f"ACK {resp.topic} {resp.offset}".encode()
        return EncodedFrame(
            header = encode_frame(header),
            payload=None,
        )
    
    if isinstance(resp, ErrorResponse):
        header = f"ERR {resp.code} {resp.message}".encode()
        return EncodedFrame(
            header = encode_frame(header),
            payload=None
        )
    
    if isinstance(resp, MessageResponse):

        header = f"MSG {resp.topic} {len(resp.payload)}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=resp.payload
        )
    
    if isinstance(resp, FileResponse):

        header = f"MSG {resp.topic} {resp.length}".encode()

        return EncodedFrame(
            header=encode_frame(header),
            payload=None,
            file_path=resp.file_path,
            offset=resp.offset,
            length=resp.length
        )

    if isinstance(resp, MmapResponse):

        header = f"MSG {resp.topic} {len(resp.buffer)}".encode()
        payload = resp.buffer

        return EncodedFrame(
            header=encode_frame(header),
            payload=payload
        )

    raise ValueError("Unknown response type")

def encode_command(cmd: Command, checksum_enable: bool = True):

    if isinstance(cmd, RegisterCommand):
        header = b"REG"
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(cmd, ClientIdCommand):
        header = f"CID {cmd.client_id}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(cmd, SubscribeCommand):
        header = f"SUB {cmd.topic}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(cmd, CommitOffsetCommand):
        header = f"CMT {cmd.topic} {cmd.offset}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(cmd, FetchOffsetCommand):
        header = f"FCH {cmd.topic}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(cmd, PublishCommand):

        if checksum_enable:
            checksum = zlib.crc32(cmd.payload)
            header = f"PUB {cmd.topic} {checksum} ".encode() + cmd.payload
        else:
            header = f"PUB {cmd.topic} ".encode() + cmd.payload

        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(cmd, PullCommand):
        header = f"PUL {cmd.topic} {cmd.offset} {cmd.size}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(cmd, PingCommand):
        header = b"PNG"
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    raise ValueError("Unknown command type")