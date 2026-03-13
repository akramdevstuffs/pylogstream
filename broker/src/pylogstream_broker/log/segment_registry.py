from pylogstream_broker.log.segment import SegmentMeta, SegmentState
from typing import Dict,List
from pylogstream_broker.log.error import IllegalTransitionError, TopicDoesntExists

from bisect import bisect_left
import threading
from collections import defaultdict

class SegmentRegistry:
    """ A dumb class who owns SegmentMeta and keeps the registry (thread-safe)"""

    _ALLOWED_TRANSITIONS = {
        SegmentState.ACTIVE: {SegmentState.SEALED},
        SegmentState.SEALED: {SegmentState.EXPIRED},
        SegmentState.EXPIRED: {SegmentState.DELETED},
        SegmentState.DELETED: set(),
    }

    def __init__(self, log_dir:str):
        self.log_dir = log_dir
        self.__registry: Dict[str, List[SegmentMeta]] = {}
        self.__locks: Dict[str, threading.RLock] = defaultdict(threading.RLock)

    def _build_segment_path(self, topic:str,base_offset) -> str:
        return f"{self.log_dir}/{topic}/{base_offset:020d}.log"
    
    def create_segment(self, topic:str, base_offset:int) -> SegmentMeta:
        lock = self.__locks[topic]
        with lock:
            path = self._build_segment_path(topic,base_offset)
            segment: SegmentMeta = SegmentMeta(
                path=path,
                topic=topic,
                base_offset=base_offset,
            )
            if(self.__registry.get(topic) is None):
                self.__registry[topic] = []
                self.__registry[topic].append(segment)
                return segment

            # Validating if current Segment overlaping with previous segment
            previous_segment:SegmentMeta = self.__registry[topic][-1]
            previous_offset: int = previous_segment.base_offset + previous_segment.write_offset
            if(previous_offset > base_offset):
                """ Illegal offset """
                raise ValueError(f"Segment is overlapping {segment} with {previous_segment}")
            self.__registry[topic].append(segment)

            return segment
    
    def advance_offset(self, meta:SegmentMeta, length:int) -> SegmentMeta:
        """
        Increases the size attribute of the SegmentMeta (thread-safe)
        Returns the new SegmentMeta
        
        :param meta: SegmentMeta object
        :type meta: SegmentMeta
        :param length: the value of size increment
        :type length: int
        :return: New SegmentMeta object from registry
        :rtype: SegmentMeta
        """
        lock = self.__locks[meta.topic]
        with lock:
            idx: int = self._find_segment_index(meta.topic, meta.base_offset)
            old_meta:SegmentMeta = self.__registry[meta.topic][idx]
            new_meta = SegmentMeta(
                topic=old_meta.topic,
                base_offset=old_meta.base_offset,
                path=old_meta.path,
                state = old_meta.state,
                created_at=old_meta.created_at,
                size = old_meta.size + length
            )
            self.__registry[meta.topic][idx] = new_meta
            return new_meta
    
    def get_segment(self, topic: str, offset:int=-1) -> SegmentMeta:
        '''Returns the SegmentMeta from topic and containing offset.
           Returns latest segment for: offset=-1
        '''
        lock = self.__locks[topic]
        with lock:
            if(self.__registry.get(topic) is None):
                raise TopicDoesntExists(topic)
            if(len(self.__registry[topic])==0):
                raise KeyError(f"Try to access offset: {offset} in an empty topic")

            # Validate if offset exists or not
            previous_segment:SegmentMeta = self.__registry[topic][-1]
            previous_offset: int = previous_segment.base_offset + previous_segment.write_offset
            if(previous_offset <= offset):
                raise ValueError(f"Segment size is smaller than given offset: {offset}, {previous_segment}")
            
            if(offset==-1):
                # Returns the latest offset
                return self.__registry[topic][-1]
            
            # Binary search finding the right segment for the offset
            i: int = self._find_segment_index(topic, offset)
            return self.__registry[topic][i]
        
    def _find_segment_index(self,topic: str,offset:int) -> int:
        """
        Helpler function to find index of segment containing the offset\\
        Not thread-safe
        
        :param topic: 
        :type topic: str
        :param offset: base_offset or any offset contained by segment
        :type offset: int
        :return: index of the offset in __registry[topic] array
        """
        i: int = 0
        l: int = 0; r:int = len(self.__registry[topic]) - 1
        while(l<=r):
            mid: int = (l+r)//2
            if(self.__registry[topic][mid].base_offset <= offset):
                i = mid
                l = mid+1
            else:
                r = mid-1
        return i
    
    def list_segments(self, topic:str)-> List[SegmentMeta]:
        lock = self.__locks[topic]
        with lock:
            if(self.__registry.get(topic) is None):
                raise KeyError(f"Topic:({topic}) doesn't exists in registry ")
            return list(self.__registry[topic]) # Returning a snapshot
    
    def list_segments_by_state(self, topic:str, state: SegmentState) -> List[SegmentMeta]:
        if(self.__registry.get(topic) is None):
            raise KeyError(f"Topic doesn't exists {topic}")
        lock = self.__locks[topic]
        with lock:
            res: List[SegmentMeta] = list(
                    filter(
                    lambda sg: sg.state == state,
                    self.__registry[topic]
                )
            )
            return res

    def progress_state(self,segment: SegmentMeta, new_state: SegmentState) -> SegmentMeta:

        topic = segment.topic

        lock = self.__locks[topic]

        with lock:

            if(self.__registry.get(topic) is None):
                raise ValueError("Segment belongs to an invalid topic {segment}")

            segment_list:List[SegmentMeta] = self.__registry[topic]
            idx = bisect_left(segment_list, segment.base_offset, key= lambda sg: sg.base_offset)
            if idx >= len(segment_list) or segment_list[idx] != segment:
                raise ValueError("Segment doesn't exists in registry {segment}")

            if new_state not in self._ALLOWED_TRANSITIONS[segment.state]:
                raise IllegalTransitionError(segment, segment.state, new_state)

            self.__registry[topic][idx] = SegmentMeta(
                segment.topic,
                segment.base_offset,
                segment.path,
                new_state,
                segment.write_offset,
                segment.created_at,
                segment.size,
            )

            return self.__registry[topic][idx]

    def remove_segment(self, segment: SegmentMeta) -> SegmentMeta:
        return self.progress_state(segment=segment, new_state=SegmentState.DELETED)
    
    def get_latest_offset(self, topic:str):
        lock = self.__locks[topic]
        with lock:
            if (self.__registry.get(topic) is None):
                raise KeyError("Topic doesn't exists {topic}")
            active_segment = self.__registry[topic][-1]
            if(active_segment is None):
                raise ValueError(f"Topic is empty {topic}")
            return active_segment.write_offset