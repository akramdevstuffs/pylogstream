from pylogstream_protocol.response import Response
from pylogstream_protocol.common import PacketHeader
from .commands import *
from .response import *
from .error import ChecksumFailed, UnknownCommand
import zlib
import re

# TODO: Update regex when correlation_id format is finalized.
_HEADER_RE = re.compile(r"^\[CRR=([^\]]+)\]\s*")


def parse_packet_header(data: bytes) -> tuple[PacketHeader | None, bytes]:
    """
    Returns:
        (header, remaining_packet_bytes)
    """
    text = data.decode()

    match = _HEADER_RE.match(text)
    if match is None:
        return None, data

    header = PacketHeader(correlation_id=str(match.group(1)))
    remaining = text[match.end():].encode()

    return header, remaining

def parse_command(data: bytes, checksum_enable=False):
    header, data = parse_packet_header(data)
    cmd_obj:Command = _parse_command(data, checksum_enable)
    if header is not None:
        cmd_obj.header = header
    return cmd_obj

def _parse_command(data: bytes, checksum_enable=False):

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
        parts = msg.split(" ", 3)
        # Wire format: CMT {topic} {acks} {offset}
        # CommitOffsetCommand dataclass: (topic: str, offset: int, acks: int)
        acks = parts[2]
        if not acks in {'-1', '0', '1'}:
            raise ValueError("Invalid acks parameter")
        return CommitOffsetCommand(parts[1], int(parts[3]), int(acks))
    
    if cmd=="FCH":
        msg = data.decode()
        parts = msg.split(" ", 1)
        return FetchOffsetCommand(parts[1])
    
    if cmd=='PUB':
        msg = data.decode()
        parts = msg.split(" ", 3)
        topic = parts[1]
        acks = parts[2]
        # Validate acks that it's in range
        if not acks in {'-1','0','1'}:
            raise ValueError("Invalid acks parameter")
        payload = parts[3]
        if checksum_enable:
            payload_parts = payload.split(" ",1)
            hash = int(payload_parts[0])
            msg = payload_parts[1].encode()
            if not checksum_verify(msg, hash):
                raise ChecksumFailed(msg, hash)
        return PublishCommand(topic, payload.encode(), int(acks),checksum_enable)

    if cmd=="PUL":
        msg = data.decode()
        parts = msg.split(" ", 3)
        topic = parts[1]
        offset = parts[2]
        size = parts[3]
        return PullCommand(topic, int(offset), int(size))
    
    if cmd=="RFH":
        msg = data.decode()
        parts = msg.split(" ", 4)
        replica_id = parts[1]
        topic = parts[2]
        offset = parts[3]
        size = parts[4]
        return ReplicaFetchCommand(topic, int(offset), int(size), replica_id)

    if cmd=='PNG':
        return PingCommand()

    if cmd == 'RGT':
        msg = data.decode()
        parts = msg.split(" ", 1)
        return RegisterTopicCommand(parts[1])

    if cmd == 'MTR':
        msg = data.decode()
        parts = msg.split(" ", 1)
        return MetadataRequestCommand(parts[1])

    if cmd == 'BRK':
        msg = data.decode()
        parts = msg.split(" ", 3)
        return BrokerRegisterCommand(parts[1], parts[2], int(parts[3]))

    if cmd == 'CPG':
        msg = data.decode()
        parts = msg.split(" ", 1)
        return ControllerPingCommand(parts[1])
    
    if cmd=='ISR':
        msg = data.decode()
        parts = msg.split(" ", 2)
        topic = parts[1]
        isr = parts[2].split(",") if parts[2] != "-" else []
        return ISRChangeCommand(topic, isr)

    raise UnknownCommand(cmd)

def parse_response(data: bytes) -> Response:
    header, data = parse_packet_header(data)
    resp_obj:Response = _parse_response(data)
    if header is not None:
        resp_obj.header = header
    return resp_obj

def _parse_response(data: bytes) -> Response:

    msg = data.decode()
    cmd = msg[:3]

    if cmd == "CID":
        parts = msg.split(" ", 1)
        return ClientIDResponse(parts[1])

    if cmd == "PNG":
        return PingResponse()

    if cmd == "ACK":
        parts = msg.split(" ", 3)
        # Wire format: ACK {topic} {acks} {offset}
        # PubAckResponse dataclass: (topic: str, offset: int, acks: int)
        acks = parts[2]
        if not acks in {'-1', '0', '1'}:
            raise ValueError("Invalid acks parameter in ACK response")
        return PubAckResponse(parts[1], int(parts[3]), int(acks))

    if cmd == "OAK":
        parts = msg.split(" ", 3)
        # Wire format: OAK {topic} {acks} {offset}
        # OffsetAckResponse dataclass: (topic: str, offset: int, acks: int)
        acks = parts[2]
        if not acks in {'-1', '0', '1'}:
            raise ValueError(f"Invalid acks parameter in OAK response {msg}")
        return OffsetAckResponse(parts[1], int(parts[3]), int(acks))
    
    if cmd=="FCR":
        parts = msg.split(" ", 2)
        topic = parts[1]
        offset = int(parts[2])
        return FetchOffsetResponse(topic, offset)

    if cmd == "ERR":
        parts = msg.split(" ", 2)
        message = parts[2] if len(parts) > 2 else ""
        return ErrorResponse(int(parts[1]), message)

    if cmd == "MSG":
        parts = msg.split(" ", 2)
        topic = parts[1]
        payload_len = int(parts[2])

        return MessageResponseHeader(topic, payload_len)
    
    if cmd == "RPL":
        parts = msg.split(" ", 4)
        topic = parts[1]
        log_end_offset = int(parts[2])
        high_watermark = int(parts[3])
        payload_len = int(parts[4])
        return ReplicaFetchHeaderResponse(topic, log_end_offset, high_watermark, payload_len)

    if cmd == "CPR":
        return ControllerPingResponse()

    if cmd == "TMD":
        parts = msg.split(" ", 6)
        topic = parts[1]
        leader_id = parts[2]
        leader_addr = parts[3]
        leader_port = int(parts[4])
        replicas = parts[5].split(",") if parts[5] != "-" else []
        version = int(parts[6])
        return TopicMetaDataResponse(topic, leader_id, leader_addr, leader_port, replicas, version)

    if cmd == "TML":
        parts = msg.split(" ", 1)
        return TopicMetaDataListHeaderResponse(int(parts[1]))

    if cmd == "NLC":
        parts = msg.split(" ", 3)
        return NotLeaderControllerResponse(
            leader_id=parts[1],
            leader_host=parts[2],
            leader_port=int(parts[3]),
        )

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

    if not checksum_verify(parts[1], checksum):
        raise RuntimeError("Message integrity failed")

    return checksum, message

def checksum_verify(msg_bytes, checksum) -> bool:
    curr = zlib.crc32(msg_bytes)
    return curr == checksum


def parse_metadata_list_payload(payload: bytes) -> list[TopicMetaDataResponse]:
    if not payload:
        return []
    lines = payload.decode().split("\n")
    meta_list = []
    for line in lines:
        if not line.strip():
            continue
        parts = line.split(" ")
        topic = parts[0]
        leader_id = parts[1]
        leader_addr = parts[2]
        leader_port = int(parts[3])
        replicas = parts[4].split(",") if parts[4] != "-" else []
        version = int(parts[5])
        meta_list.append(TopicMetaDataResponse(topic, leader_id, leader_addr, leader_port, replicas, version))
    return meta_list