from pylogstream_broker.config import LogConfig
from pylogstream_broker.log.segment_policy import SegmentPolicy
from pylogstream_broker.log.segment_memory import SegmentMemory
from pylogstream_broker.log.segment_registry import SegmentRegistry
from pylogstream_broker.log.log_reader import LogReader
from pylogstream_broker.log.log_writer import LogWriter
from pylogstream_broker.log.error import TopicDoesntExists

class LogManager:
    def __init__(self, config: LogConfig):
        self.config = config
        self.policy = SegmentPolicy(config.retension_ms, config.rollover_ms, config.max_segment_size)
        self._memory = SegmentMemory(
            config.segment_cache_size, 
            config.init_segment_size, 
            config.segment_size_inc,
            self.policy
            )
        self._registry = SegmentRegistry(config.log_dir)
        self._reader = LogReader(self._registry, self._memory)
        self._writer = LogWriter(self._registry, self._memory, self.policy)
    
    def create_topic(self, topic):
        try:
            # Check if topic already exists
            self._registry.get_segment(topic)
        except TopicDoesntExists:
            # Topic doesn't exists, create a new one
            self._registry.create_segment(topic, 0)

    def read(self, topic, offset, size):
        return self._reader.read(topic, offset, size)

    def read_bytes(self, topic, offset,size):
        return self._reader.read_bytes(topic, offset, size)
    
    def append(self, topic,data): 
        return self._writer.write(topic, data)
    
    def append_batch(self, topic, data: list[bytes]):
        return self._writer.write_batch(topic, data)
    
    def get_latest_offset(self, topic):
        return self._registry.get_latest_offset(topic)