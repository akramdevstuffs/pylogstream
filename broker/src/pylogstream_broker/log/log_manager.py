from pylogstream_broker.config import LogConfig
from pylogstream_broker.log.segment_policy import SegmentPolicy
from pylogstream_broker.log.segment_memory import SegmentMemory
from pylogstream_broker.log.segment_registry import SegmentRegistry
from pylogstream_broker.log.log_reader import LogReader
from pylogstream_broker.log.log_writer import LogWriter

class LogManager:
    def __init__(self, config: LogConfig):
        self.config = config
        self.policy = SegmentPolicy(config.retension_ms, config.rollover_ms, config.max_segment_size)
        self.memory = SegmentMemory(
            config.segment_cache_size, 
            config.init_segment_size, 
            config.segment_size_inc,
            self.policy
            )
        self.registry = SegmentRegistry(config.log_dir)
        self.reader = LogReader(self.registry, self.memory)
        self.writer = LogWriter(self.registry, self.memory, self.policy)

    def read(self, topic, offset, size):
        return self.reader.read(topic, offset, size)

    def read_bytes(self, topic, offset,size):
        return self.reader.read_bytes(topic, offset, size)
    
    def append(self, topic,data): 
        return self.writer.write(topic, data)