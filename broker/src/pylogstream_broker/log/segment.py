from dataclasses import dataclass
from enum import Enum, auto
from time import time

class SegmentState(Enum):
    ACTIVE = auto()
    SEALED = auto()
    EXPIRED = auto()
    DELETED = auto()

@dataclass(frozen=True)
class SegmentMeta:
    topic: str 
    base_offset: int
    path: str
    state: SegmentState = SegmentState.ACTIVE
    write_offset: int = 0
    created_at: float = time()
    size: int = 0

    def get_id(self) -> str:
        return self.topic + str(self.base_offset)
    
    def get_filepath(self) -> str:
        return self.path
    
    def is_mutable(self) -> bool:
        return self.state == SegmentState.ACTIVE