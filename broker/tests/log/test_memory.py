import pytest
from pylogstream_broker.log.segment_registry import SegmentMeta, SegmentState
from pylogstream_broker.log.segment_memory import SegmentMemory, SegmentHandle, Entry, AtomicInt
import threading
import os

@pytest.fixture
def segment_meta(tmp_path):
    path = tmp_path / "segment" / "00000000000000000000.log"
    return SegmentMeta(topic="test-topic", base_offset=0, path=str(path))

TEST_MEMORY_LRU_SIZE = 100
TEST_INIT_SEGMENT_SIZE = 1024
TEST_SEGMENT_SIZE_INC = 1024

@pytest.fixture
def entry(segment_meta):
    return Entry(meta=segment_meta, init_segment_size=TEST_INIT_SEGMENT_SIZE, segment_size_inc=TEST_SEGMENT_SIZE_INC)

@pytest.fixture
def segment_policy():
    class DummySegmentPolicy:
        def should_rollover(self, meta: SegmentMeta, required_size: int) -> bool:
            return False
        def is_expired(self, meta: SegmentMeta, now: float) -> bool:
            return False
    return DummySegmentPolicy()
    
@pytest.fixture
def memory(segment_policy):
    return SegmentMemory(TEST_MEMORY_LRU_SIZE, TEST_INIT_SEGMENT_SIZE, TEST_SEGMENT_SIZE_INC, segment_policy)

# ------------------
# Test SegmentMemory
# ------------------

def test_segment_memory_initialization(memory):
    assert memory._SegmentMemory__capacity == TEST_MEMORY_LRU_SIZE
    assert memory.init_segment_size == TEST_INIT_SEGMENT_SIZE
    assert memory.segment_size_inc == TEST_SEGMENT_SIZE_INC

def test_segment_memory_acquire_and_release(memory, segment_meta):
    handle = memory.acquire(segment_meta)
    assert handle is not None
    assert isinstance(handle, SegmentHandle)
    # Check if the entry is loaded in memory
    assert handle._SegmentHandle__entry is not None
    # Check the refcount is incremented
    assert handle._SegmentHandle__entry._refcount.get() == 1
    # Release the handle and check if refcount is decremented
    handle.release()
    assert handle._SegmentHandle__entry._refcount.get() == 0

def test_segment_memory_concurrent_acquire_and_release(memory, segment_meta):
    def worker():
        handle = memory.acquire(segment_meta)
        assert handle is not None
        assert isinstance(handle, SegmentHandle)
        assert handle._SegmentHandle__entry is not None
        assert handle._SegmentHandle__entry._refcount.get() > 0
        handle.release()

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    handle = memory.acquire(segment_meta)
    assert handle._SegmentHandle__entry._refcount.get() == 1
    handle.release()
    assert handle._SegmentHandle__entry._refcount.get() == 0

def test_segment_memory_eviction(memory, segment_meta):
    # Acquire more entries than the capacity to trigger eviction
    for i in range(TEST_MEMORY_LRU_SIZE + 10):
        meta = SegmentMeta(topic="test-topic", base_offset=i*TEST_INIT_SEGMENT_SIZE, path=f"/tmp/segment_{i}.log")
        handle = memory.acquire(meta)
        handle.release()

        assert len(memory._SegmentMemory__cache) <= TEST_MEMORY_LRU_SIZE, \
            f"Cache size exceeded: {len(memory._SegmentMemory__cache)} > {TEST_MEMORY_LRU_SIZE}"
    assert len(memory._SegmentMemory__cache) == TEST_MEMORY_LRU_SIZE

def test_segment_memory_force_evict(memory, segment_meta):
    handle = memory.acquire(segment_meta)
    assert handle._SegmentHandle__entry._refcount.get() == 1
    # Make sure the refcount is zero
    handle.release()
    memory.force_evict(segment_meta)
    # After force eviction, the entry should be evicted and refcount should be 0
    assert handle._SegmentHandle__entry._refcount.get() == 0
    assert handle._SegmentHandle__entry._Entry__mmap is None
    assert handle._SegmentHandle__entry._Entry__file_obj is None

def test_segment_memory_try_evict(memory, segment_meta):
    handle = memory.acquire(segment_meta)
    assert handle._SegmentHandle__entry._refcount.get() == 1
    # Try to evict while refcount is not zero, it should not evict
    assert memory.try_evict(segment_meta) == False
    assert handle._SegmentHandle__entry._refcount.get() == 1
    assert handle._SegmentHandle__entry._Entry__mmap is not None
    assert handle._SegmentHandle__entry._Entry__file_obj is not None

    # Release the handle and try to evict again, it should evict successfully
    handle.release()
    assert memory.try_evict(segment_meta) == True
    assert handle._SegmentHandle__entry._refcount.get() == 0
    assert handle._SegmentHandle__entry._Entry__mmap is None
    assert handle._SegmentHandle__entry._Entry__file_obj is None

def test_segment_memory_cleanup_thread(memory, segment_meta):
    # Acquire more entries than the capacity to trigger eviction
    for i in range(TEST_MEMORY_LRU_SIZE + 10):
        meta = SegmentMeta(topic="test-topic", base_offset=i*TEST_INIT_SEGMENT_SIZE, path=f"/tmp/segment_{i}.log")
        handle = memory.acquire(meta)
        handle.release()
    # Wait for the cleanup thread to process the evicted entries
    import time
    time.sleep(1)
    # Check if the evicted entries are cleaned up
    assert memory._SegmentMemory__cleanup_queue.qsize() == 0

# -----------------
# Test Entry class
# -----------------

def test_entry_initialization(entry):
    assert entry._Entry__filepath.endswith("00000000000000000000.log")
    assert entry.init_segment_size == TEST_INIT_SEGMENT_SIZE
    assert entry.segment_size_inc == TEST_SEGMENT_SIZE_INC

def test_entry_loading(entry: Entry):
    entry.load()
    # Check if file is created
    assert entry._Entry__file_obj is not None
    assert entry._Entry__capacity is not None
    assert entry._Entry__capacity == TEST_INIT_SEGMENT_SIZE
    # Check if file is created on disk by 
    assert os.path.exists(entry._Entry__filepath)
    assert entry._Entry__mmap is not None

def test_entry_write_and_read(entry: Entry):
    entry.load()
    data = b"hello world"
    entry.write(0, data)
    read_data = entry.read_bytes(0, len(data))
    assert read_data == data

def test_entry_resize(entry: Entry):
    entry.load()
    previous_capacity = entry._Entry__capacity
    data = b"a" * (TEST_INIT_SEGMENT_SIZE + 1)  # Data larger than initial size to trigger resize
    entry.write(0, data)
    read_data = entry.read_bytes(0, len(data))
    assert read_data == data
    assert entry._Entry__capacity == previous_capacity + TEST_SEGMENT_SIZE_INC

def test_concurrent_entry_writes(entry: Entry):
    entry.load()
    def writer():
        for _ in range(100):
            entry.write(0, b"x" * (TEST_INIT_SEGMENT_SIZE + 1))
    threads = [threading.Thread(target=writer) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # If we reach here without exceptions, the thread safety is maintained

def test_concurrent_entry_reads_and_writes(entry: Entry):
    entry.load()
    def writer():
        for _ in range(100):
            entry.write(0, b"x" * (TEST_INIT_SEGMENT_SIZE + 1))
    def reader():
        for _ in range(100):
            entry.read_bytes(0, TEST_INIT_SEGMENT_SIZE)
    threads = [threading.Thread(target=writer) for _ in range(5)] + [threading.Thread(target=reader) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # If we reach here without exceptions, the thread safety is maintained

def test_entry_release(entry: Entry):
    entry.load()
    entry.release()
    assert entry._Entry__mmap is None
    assert entry._Entry__file_obj is None

def test_entry_multiple_loads_and_releases(entry: Entry):
    for _ in range(5):
        entry.load()
        assert entry._Entry__mmap is not None
        assert entry._Entry__file_obj is not None
        entry.release()
        assert entry._Entry__mmap is None
        assert entry._Entry__file_obj is None

def test_entry_io_after_release(entry: Entry):
    entry.load()
    entry.release()
    with pytest.raises(AssertionError):
        entry.write(0, b"data")
    with pytest.raises(AssertionError):
        entry.read_bytes(0, 4)

def test_entry_resize_under_concurrent_reads_and_writes(entry: Entry):
    entry.load()
    def writer():
        for _ in range(100):
            entry.write(0, b"x" * (TEST_INIT_SEGMENT_SIZE + 1))
    def reader():
        for _ in range(100):
            entry.read_bytes(0, TEST_INIT_SEGMENT_SIZE)
    threads = [threading.Thread(target=writer) for _ in range(5)] + [threading.Thread(target=reader) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # If we reach here without exceptions, the thread safety is maintained during resize with concurrent reads and writes

# ----------------
#  Test AtomicInt
# ----------------

def test_atomic_int():
    a = AtomicInt(0)
    assert a.get() == 0
    a.increment()
    assert a.get() == 1
    a.decrement()
    assert a.get() == 0
    a.increment(5)
    assert a.get() == 5
    a.decrement(3)
    assert a.get() == 2

def test_atomic_int_thread_safety():
    a = AtomicInt(0)
    def incrementer():
        for _ in range(1000):
            a.increment()
    def decrementer():
        for _ in range(1000):
            a.decrement()
    threads = []
    for _ in range(10):
        for _ in range(100):
            t_inc = threading.Thread(target=incrementer)
            t_dec = threading.Thread(target=decrementer)
            threads.append(t_inc)
            threads.append(t_dec)
            t_inc.start()
            t_dec.start()
    for t in threads:
        t.join()
    assert a.get() == 0