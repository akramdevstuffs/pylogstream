from pylogstream_broker.log.segment_registry import SegmentRegistry, SegmentMeta, SegmentState
from pylogstream_broker.log.segment_memory import SegmentMemory
from pylogstream_broker.log.error import TopicDoesntExists
from pylogstream_broker.log.segment_policy import SegmentPolicy
from collections import defaultdict
import threading

class LogWriter:
    """
    Provides writes methods by hiding WAL complexity
    Internally serializes the writes.
    Uses one writer per topic.
    """
    def __init__(self, registry:SegmentRegistry, memory: SegmentMemory, policy: SegmentPolicy):
        self.__registry = registry
        self.__memory = memory
        self.__policy = policy
        self.__writer_lock = defaultdict(threading.Lock)
        self.__lock = threading.Lock()
    
    def get_writer_lock(self, topic) -> threading.Lock:
        with self.__lock:
            return self.__writer_lock[topic]
    
    def write(self,topic: str, data:bytes) -> int:
        '''
        Thread-safe, writes to the disk
        
        :param self: Description
        :param topic: Topic
        :type topic: str
        :param data: Payload
        :type data: bytes
        :return: Offset where it's writen
        :rtype: int
        '''
        lock = self.get_writer_lock(topic)
        with lock:
            try:
                meta = self.__registry.get_segment(topic)
            except TopicDoesntExists:
                meta = self.__registry.create_segment(topic, 0)
            required_size:int = 4 + len(data)
            msg = len(data).to_bytes(4, 'big') + data
            if(self.__policy.should_rollover(meta, required_size) and meta.size != 0):
                meta = self._rollover(topic)
            with self.__memory.acquire(meta) as handle:
                handle.write(meta.size, msg)
            self.__registry.advance_offset(meta, len(msg))
            return meta.write_offset
    
    def write_batch(self, topic: str, batch: list[bytes]):
        offsets = []
        # TODO: Impletement batch writes to minimize syscall overhead
        for msg in batch:
            offset = self.write(topic, msg)
            offsets.append(offset)
        return offsets
    
    def _rollover(self, topic: str) -> SegmentMeta:
        """Close the active segment and open a new segment
           Returns the new SegmentMeta
        """
        base_offset: int = 0
        try:
            meta = self.__registry.get_segment(topic)
            self.__registry.progress_state(meta, SegmentState.SEALED)
            base_offset = meta.base_offset + meta.size
        except TopicDoesntExists:
            pass
        meta = self.__registry.create_segment(topic, base_offset)
        return meta