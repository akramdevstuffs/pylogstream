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

@dataclass(frozen=True)
class PubAckResponse(Response):
    topic: str
    offset: int

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