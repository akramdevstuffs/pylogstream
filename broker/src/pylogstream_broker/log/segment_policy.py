from pylogstream_broker.log.segment import SegmentMeta, SegmentState
import time

class SegmentPolicy:

    def __init__(self, retension_ms, rollover_ms,max_segment_size):
        self.retension_ms = retension_ms
        self.max_segment_size = max_segment_size
        self.rollover_ms = rollover_ms

    def is_expired(self,segment: SegmentMeta, now: float) -> bool:
        return now - segment.created_at > self.retension_ms

    def should_rollover(self,meta: SegmentMeta, msg_size: int) -> bool:
        if(time.time() - meta.created_at > self.rollover_ms):
            return True
        return meta.size + msg_size > self.max_segment_size