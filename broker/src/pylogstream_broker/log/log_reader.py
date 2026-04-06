from dataclasses import dataclass
from typing import List, Callable
from pylogstream_broker.log.segment import SegmentMeta
from pylogstream_broker.log.segment_memory import SegmentMemory, SegmentHandle
from pylogstream_broker.log.segment_registry import SegmentRegistry

@dataclass
class FileSlice:
    filepath: str
    offset: int
    batch_size: int

@dataclass
class ReadResult:
    next_offset:int
    high_watermark:int
    file_slice: FileSlice
    _release: Callable[[],None]
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        self._release()

@dataclass
class ReadResultBytes:
    data: bytes
    next_offset:int
    high_watermark:int

class LogReader:
    """A reader class that provides different thread-safe function for reading WAL"""

    def __init__(self, registry:SegmentRegistry, memory: SegmentMemory):
        self._registry = registry
        self._memory = memory

    def read(self,topic:str, offset:int, size:int):
        """A thread-safe function that provides memoryview for each read request"""
        meta = self._registry.get_segment(topic, offset)
        filepath = meta.get_filepath()
        segment_offset = offset - meta.base_offset
        max_batch_size = min(size, meta.size - segment_offset)
        handle = self._memory.acquire(meta)
        
        batch_size = self._calc_read_size(handle, segment_offset, max_batch_size)

        next_offset = offset + batch_size
        max_offset = self._registry.get_latest_offset(topic)
        file_slice = FileSlice(
                filepath=filepath,
                offset=segment_offset,
                batch_size = batch_size
        )
        result = ReadResult(next_offset, max_offset, file_slice,handle.release)
        return result
    
    def read_bytes(self, topic:str, offset:int, size:int):
        """A thread-safe function that provides bytes for each read request"""
        meta = self._registry.get_segment(topic, offset)
        segment_offset = offset - meta.base_offset
        max_batch_size = min(size, meta.size - segment_offset)

        with self._memory.acquire(meta) as handle:
            batch_size = self._calc_read_size(handle, segment_offset, max_batch_size)
            data = handle.read_bytes(segment_offset, batch_size)
        next_offset = offset + batch_size
        max_offset = self._registry.get_latest_offset(topic)
        return ReadResultBytes(data, next_offset, max_offset)
    
    def _calc_read_size(self, handle: SegmentHandle, offset: int, max_batch_size: int) -> int:
        batch_size = 0
        roffset = offset
        while roffset < offset + max_batch_size:
            sz = int.from_bytes(handle.read_bytes(roffset, 4))
            if(batch_size + sz + 4 <= max_batch_size):
                batch_size += sz+4
            roffset += sz + 4
        
        # Atleast include 1 message
        if batch_size == 0:
            batch_size = int.from_bytes(handle.read_bytes(offset, 4)) + 4
        return batch_size