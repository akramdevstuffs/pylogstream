import pytest
from pylogstream_broker.log.segment_registry import SegmentRegistry, SegmentState
from pylogstream_broker.log.error import IllegalTransitionError, TopicDoesntExistsError

@pytest.fixture
def registry(tmp_path):
    return SegmentRegistry(tmp_path)

def test_build_segment_path(registry):
    path = registry._build_segment_path('topic1', 0)
    path2 = registry._build_segment_path('topic1', 42)
    path3 = registry._build_segment_path('topic2', 0)
    assert path.endswith('topic1/00000000000000000000.log')
    assert path2.endswith('topic1/00000000000000000042.log')
    assert path3.endswith('topic2/00000000000000000000.log')

def test_create_segment(registry):
    segment = registry.create_segment('topic1', 0)
    assert segment.topic == 'topic1'
    assert segment.base_offset == 0
    assert segment.path.endswith('topic1/00000000000000000000.log')

def test_create_overlapping_segment(registry):
    registry.create_segment('topic1', 100)
    with pytest.raises(ValueError):
        registry.create_segment('topic1', 50)
    with pytest.raises(ValueError):
        registry.create_segment('topic1', 100)
    
def test_advance_offset(registry):
    segment = registry.create_segment('topic1', 0)
    updated_segment = registry.advance_offset(segment, 100)
    assert updated_segment.get_end_offset() == 100
    # Advancing again should update the same segment
    updated_segment2 = registry.advance_offset(updated_segment, 50)
    assert updated_segment2.get_end_offset() == 150

def test_find_segment(registry):
    registry.create_segment('topic1', 0)
    registry.advance_offset(registry.get_segment('topic1'), 100)
    registry.create_segment('topic1', 100)
    registry.advance_offset(registry.get_segment('topic1'), 100)
    
    segment = registry.get_segment('topic1', 50)
    assert segment.base_offset == 0
    segment = registry.get_segment('topic1', 150)
    assert segment.base_offset == 100
    # Finding non-existing offset should raise ValueError
    with pytest.raises(ValueError):
        registry.get_segment('topic1', 250)
    
def test_find_segment_in_empty_topic(registry):
    registry.create_segment('topic1', 0)
    with pytest.raises(ValueError):
        registry.get_segment('topic1', 50)

def test_find_segment_in_non_existing_topic(registry):
    with pytest.raises(TopicDoesntExistsError):
        registry.get_segment('topic1', 50)

def test_transition_state(registry):
    registry.create_segment('topic1', 0)
    registry.advance_offset(registry.get_segment('topic1'), 100)
    segment = registry.get_segment('topic1')
    assert segment.state == SegmentState.ACTIVE
    updated_segment = registry.progress_state(segment, SegmentState.SEALED)
    assert updated_segment.state == SegmentState.SEALED
    updated_segment2 = registry.progress_state(updated_segment, SegmentState.EXPIRED)
    assert updated_segment2.state == SegmentState.EXPIRED
    # Illegal transition should raise an error
    with pytest.raises(IllegalTransitionError):
        registry.progress_state(updated_segment2, SegmentState.ACTIVE)

def test_remove_segment(registry):
    registry.create_segment('topic1', 0)
    registry.advance_offset(registry.get_segment('topic1'), 100)
    # Removing an active segment will result in IlligalTransitionError
    with pytest.raises(IllegalTransitionError):
        registry.remove_segment(registry.get_segment('topic1', 0))
    # Mark this segment as SEALED
    registry.progress_state(registry.get_segment('topic1', 0), SegmentState.SEALED)
    
    # Removing an SEALED segment will raise an IlligalTransitionError 
    # because the transition from SEALED to DELETED is not allowed
    with pytest.raises(IllegalTransitionError):
        registry.remove_segment(registry.get_segment('topic1', 0))
    
    registry.progress_state(registry.get_segment('topic1', 0), SegmentState.EXPIRED)
    registry.remove_segment(registry.get_segment('topic1', 0))

    with pytest.raises(ValueError):
        registry.get_segment('topic1')

def test_remove_non_existing_segment(registry):
    with pytest.raises(TopicDoesntExistsError):
        registry.remove_segment(registry.get_segment('topic1', 0))

def test_list_segments(registry):
    registry.create_segment('topic1', 0)
    registry.advance_offset(registry.get_segment('topic1'), 100)
    registry.create_segment('topic1', 100)
    registry.advance_offset(registry.get_segment('topic1'), 100)
    
    segments = registry.list_segments('topic1')
    assert len(segments) == 2
    assert segments[0].base_offset == 0
    assert segments[1].base_offset == 100

def test_list_segments_by_state(registry):
    registry.create_segment('topic1', 0)
    registry.advance_offset(registry.get_segment('topic1'), 100)
    registry.create_segment('topic1', 100)
    registry.advance_offset(registry.get_segment('topic1'), 100)
    
    active_segments = registry.list_segments_by_state('topic1', SegmentState.ACTIVE)
    assert len(active_segments) == 2
    
    registry.progress_state(registry.get_segment('topic1', 0), SegmentState.SEALED)
    
    active_segments = registry.list_segments_by_state('topic1', SegmentState.ACTIVE)
    assert len(active_segments) == 1
    assert active_segments[0].base_offset == 100
    
    sealed_segments = registry.list_segments_by_state('topic1', SegmentState.SEALED)
    assert len(sealed_segments) == 1
    assert sealed_segments[0].base_offset == 0

def test_list_segments_in_non_existing_topic(registry):
    with pytest.raises(TopicDoesntExistsError):
        registry.list_segments('topic1')

def test_segment_list_snapshot(registry):
    registry.create_segment('topic1', 0)
    registry.advance_offset(registry.get_segment('topic1'), 100)
    
    segments = registry.list_segments('topic1')
    assert len(segments) == 1
    
    # Modifying the returned list should not affect the registry
    segments.pop()
    
    segments2 = registry.list_segments('topic1')
    assert len(segments2) == 1, "Modifying the returned list should not affect the registry"


# -----------------
# CONCURRENCY TESTS
# -----------------

import concurrent.futures
import threading
import time

def test_lock_race_creates_duplicate_topics(registry, monkeypatch):
    topic = "topic1"
    base_offset = 0

    registry.create_segment(topic, base_offset)

    errors = []
    created_segments = []

    from pylogstream_broker.log.segment import SegmentMeta

    original = SegmentMeta.get_end_offset

    def delayed_get_end_offset(self):
        time.sleep(0.001)  # Simulate delay to increase
        return original(self)
    
    # Patch the get_end_offset to introduce a delay, increasing
    monkeypatch.setattr(SegmentMeta, "get_end_offset", delayed_get_end_offset)

    # Barrier to force threads to start at same time
    barrier = threading.Barrier(50)

    def worker():
        try:
            barrier.wait()  # synchronize start
            seg = registry.create_segment(topic, base_offset=101)
            created_segments.append(seg)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(200)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(created_segments) == 1, (
        f"Race condition detected: {len(created_segments)} segments created instead of 1"
    )


def test_concurrent_topic_creation(registry):
    def create_topic(topic):
        registry.create_segment(topic, 0)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        futures = []
        for i in range(100):
            futures.append(executor.submit(create_topic, f'topic{i}'))
        concurrent.futures.wait(futures)
    
    # Check if all topics are created
    for i in range(100):
        segment = registry.get_segment(f'topic{i}')
        assert segment.base_offset == 0

def test_concurrent_segment_creation(registry):
    def create_segments(topic, base_offset):
        for i in range(10):
            registry.create_segment(topic, base_offset + i*100)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for i in range(5):
            futures.append(executor.submit(create_segments, 'topic1', i*1000))
        concurrent.futures.wait(futures)
    
    # Check if all segments are created
    for i in range(5):
        for j in range(10):
            segment = registry.get_segment_by_base_offset('topic1', i*1000 + j*100)
            assert segment.base_offset == i*1000 + j*100

def test_concurrent_offset_advancement(registry):
    segment = registry.create_segment('topic1', 0)
    
    def advance_offset():
        for _ in range(100):
            registry.advance_offset(segment, 1)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for _ in range(5):
            futures.append(executor.submit(advance_offset))
        concurrent.futures.wait(futures)
    
    updated_segment = registry.get_segment('topic1')
    assert updated_segment.get_end_offset() == 500

def test_concurrent_state_transitions(registry):
    segment = registry.create_segment('topic1', 0)
    segment = registry.advance_offset(segment, 100)
    
    def transition_state():
        registry.progress_state(registry.get_segment('topic1'), SegmentState.SEALED)
        registry.progress_state(registry.get_segment('topic1'), SegmentState.EXPIRED)
        registry.remove_segment(registry.get_segment('topic1'))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for _ in range(1000):
            futures.append(executor.submit(transition_state))
        concurrent.futures.wait(futures)
    
    with pytest.raises(ValueError):
        registry.get_segment('topic1', 50)

def test_concurrent_listing(registry):
    for i in range(100):
        registry.create_segment('topic1', i*100)
    
    def list_segments():
        segments = registry.list_segments('topic1')
        assert len(segments) == 100
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for _ in range(100):
            futures.append(executor.submit(list_segments))
        concurrent.futures.wait(futures)