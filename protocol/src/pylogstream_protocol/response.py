from dataclasses import dataclass
from pylogstream_protocol.common import Packet

class Response(Packet):
    pass

@dataclass(slots=True)
class ClientIDResponse(Response):
    client_id: str

class PingResponse(Response):
    pass

@dataclass(slots=True)
class OffsetAckResponse(Response):
    topic: str
    offset: int
    acks: int # -1, 0, 1

@dataclass(slots=True)
class PubAckResponse(Response):
    topic: str
    offset: int
    acks: int # -1, 0, 1

@dataclass(slots=True)
class FetchOffsetResponse(Response):
    topic: str
    offset: int

@dataclass(slots=True)
class ErrorResponse(Response):
    code: int
    message: str

@dataclass(slots=True)
class MessageResponseHeader(Response):
    topic: str
    length: int

@dataclass(slots=True)
class MessageResponse(Response):
    topic: str
    payload: bytes

@dataclass(slots=True)
class FileResponse(Response):
    topic: str
    file_path: str
    offset: int
    length: int

@dataclass(slots=True)
class MmapResponse(Response):
    topic: str
    buffer: memoryview

class ReplicaResponse(Response):
    # Just a high level class for type hinting
    pass

@dataclass(slots=True)
class ReplicaFetchHeaderResponse(ReplicaResponse):
    topic: str
    log_end_offset: int
    high_watermark: int
    payload_length: int

@dataclass(slots=True)
class ReplicaFetchBytesResponse(ReplicaFetchHeaderResponse):
    payload: bytes
    

@dataclass(slots=True)
class ReplicaFetchMmapResponse(ReplicaFetchHeaderResponse):
    buffer: memoryview

@dataclass(slots=True)
class ReplicaFetchFileResponse(ReplicaFetchHeaderResponse):
    file_path: str
    offset: int
    length: int

class ControllerResponse(Response):
    # A high level class for all group-coordinator response
    pass

@dataclass(slots=True)
class TopicMetaDataResponse(ControllerResponse):
    topic: str
    leader_id: str
    leader_addr: str
    leader_port: int
    replica_list: list[str]
    version: int   # Epoch version of coordinator, helps to prevent node holding old-version
    state: str = "running"

@dataclass(slots=True)
class TopicMetaDataListHeaderResponse(ControllerResponse):
    payload_length: int


@dataclass(slots=True)
class TopicMetaDataListResponse(ControllerResponse):
    meta_list: list[TopicMetaDataResponse]



@dataclass(slots=True)
class ControllerPingResponse(ControllerResponse):
    pass

@dataclass(slots=True)
class NotLeaderControllerResponse(ControllerResponse):
    """Sent to brokers when this controller is no longer the leader.

    Fields carry the current leader's identity so clients can reconnect.
    """
    leader_id: str
    leader_host: str
    leader_port: int