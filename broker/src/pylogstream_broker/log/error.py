from pylogstream_broker.log.segment import SegmentMeta, SegmentState

class LogError(Exception):
    pass

class LogStateError(LogError):
    pass

class TopicDoesntExists(LogError):
    def __init__(self, topic:str):
        super().__init__(
            f"Topic doesn't exists {topic}"
        )
        self.topic = topic

class SegmentDoesntExists(LogError):
    pass

class IllegalTransitionError(LogStateError):
    def __init__(self, segment:SegmentMeta, from_state:SegmentState, to_state: SegmentState):
        super().__init__
        (
            f"Illegal segment transition {from_state.name} → {to_state.name} "
            f"for segment (topic={segment.topic}, base_offset={segment.base_offset})"
        )
        self.segment = segment
        self.from_state = from_state
        self.to_state = to_state