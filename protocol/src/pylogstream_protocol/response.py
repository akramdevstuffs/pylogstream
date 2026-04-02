from dataclasses import dataclass

class Response:
    # Just a parent class all response used for type hinting
    pass

@dataclass(frozen=True)
class ClientIDResponse(Response):
    client_id: str

@dataclass(frozen=True)
class PingResponse(Response):
    pass

@dataclass(frozen=True)
class OffsetAckResponse(Response):
    topic: str
    offset: int
    acks: int # -1, 0, 1

@dataclass(frozen=True)
class PubAckResponse(Response):
    topic: str
    offset: int
    acks: int # -1, 0, 1

@dataclass(frozen=True)
class FetchOffsetResponse(Response):
    topic: str
    offset: int

@dataclass(frozen=True)
class ErrorResponse(Response):
    code: int
    message: str

@dataclass(frozen=True)
class MessageResponseHeader(Response):
    topic: str
    length: int

@dataclass(frozen=True)
class MessageResponse(Response):
    topic: str
    payload: bytes

@dataclass(frozen=True)
class FileResponse(Response):
    topic: str
    file_path: str
    offset: int
    length: int

@dataclass(frozen=True)
class MmapResponse(Response):
    topic: str
    buffer: memoryview

class ReplicaResponse(Response):
    # Just a high level class for type hinting
    pass

@dataclass(frozen=True)
class ReplicaFetchHeaderResponse(ReplicaResponse):
    topic: str
    log_end_offset: int
    high_watermark: int
    payload_length: int

@dataclass(frozen=True)
class ReplicaFetchBytesResponse(ReplicaFetchHeaderResponse):
    payload: bytes
    

@dataclass(frozen=True)
class ReplicaFetchMmapResponse(ReplicaFetchHeaderResponse):
    buffer: memoryview

@dataclass(frozen=True)
class ReplicaFetchFileResponse(ReplicaFetchHeaderResponse):
    file_path: str
    offset: int
    length: int

class ControllerResponse(Response):
    # A high level class for all group-coordinator response
    pass

@dataclass(frozen=True)
class TopicMetaDataResponse(ControllerResponse):
    topic: str
    leader_id: str
    leader_addr: str
    leader_port: int
    replica_list: list[str]
    version: int   # Epoch version of coordinator, helps to prevent node holding old-version

@dataclass(frozen=True)
class TopicMetaDataListResponse(ControllerResponse):
    meta_list: list[TopicMetaDataResponse]