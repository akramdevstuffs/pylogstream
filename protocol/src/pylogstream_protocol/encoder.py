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
        header = f"OAK {resp.topic} {resp.acks} {resp.offset}".encode()
        return EncodedFrame(
            header = encode_frame(header),
            payload=None
        )
    
    if isinstance(resp, FetchOffsetResponse):
        header = f"FCR {resp.topic} {resp.offset}".encode()
        return EncodedFrame(
            header = encode_frame(header),
            payload=None
        )

    if isinstance(resp, PubAckResponse):

        header = f"ACK {resp.topic} {resp.acks} {resp.offset}".encode()
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
    
    if isinstance(resp, ReplicaFetchBytesResponse):

        header = (f"RPL {resp.topic} {resp.log_end_offset} "+\
                 f"{resp.high_watermark} {len(resp.payload)}").encode()
        payload = resp.payload

        return EncodedFrame(
            header=encode_frame(header),
            payload=payload
        )
    
    if isinstance(resp, ReplicaFetchFileResponse):

        header = (f"RPL {resp.topic} {resp.log_end_offset} "+\
                 f"{resp.high_watermark} {resp.length}").encode()

        return EncodedFrame(
            header=encode_frame(header),
            payload=None,
            file_path=resp.file_path,
            offset=resp.offset,
            length=resp.length
        )
    
    if isinstance(resp, ReplicaFetchMmapResponse):

        header = (f"RPL {resp.topic} {resp.log_end_offset} "+\
                 f"{resp.high_watermark} {len(resp.buffer)}").encode()
        payload = resp.buffer

        return EncodedFrame(
            header=encode_frame(header),
            payload=payload
        )

    if isinstance(resp, TopicMetaDataResponse):
        replicas_str = ",".join(resp.replica_list) if resp.replica_list else "-"
        header = f"TMD {resp.topic} {resp.leader_id} {resp.leader_addr} {resp.leader_port} {replicas_str} {resp.version}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(resp, TopicMetaDataListResponse):
        meta_lines = []
        for meta in resp.meta_list:
            replicas_str = ",".join(meta.replica_list) if meta.replica_list else "-"
            meta_lines.append(f"{meta.topic} {meta.leader_id} {meta.leader_addr} {meta.leader_port} {replicas_str} {meta.version}")
        payload_str = "\n".join(meta_lines)
        header = f"TML {len(payload_str)}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=payload_str.encode()
        )

    if isinstance(resp, ControllerPingResponse):
        header = b"CPR"
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
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
        header = f"CMT {cmd.topic} {cmd.acks} {cmd.offset}".encode()
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
            header = f"PUB {cmd.topic} {cmd.acks} {checksum} ".encode() + cmd.payload
        else:
            header = f"PUB {cmd.topic} {cmd.acks} ".encode() + cmd.payload

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
    
    if isinstance(cmd, ReplicaFetchCommand):
        header = f"RFH {cmd.replica_id} {cmd.topic} {cmd.offset} {cmd.size}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(cmd, RegisterTopicCommand):
        header = f"RGT {cmd.topic}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(cmd, MetadataRequestCommand):
        header = f"MTR {cmd.topic}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(cmd, BrokerRegisterCommand):
        header = f"BRK {cmd.broker_id} {cmd.host} {cmd.port}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    if isinstance(cmd, ControllerPingCommand):
        header = f"CPG {cmd.broker_id}".encode()
        return EncodedFrame(
            header=encode_frame(header),
            payload=None
        )

    raise ValueError("Unknown command type")