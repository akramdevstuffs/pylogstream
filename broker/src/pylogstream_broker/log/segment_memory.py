from pylogstream_broker.log.segment import SegmentMeta
from pylogstream_broker.log.segment_policy import SegmentPolicy
from pylogstream_broker.utility.utility import set_sequential_hint

import mmap
import _io
import threading
import os
from collections import OrderedDict
from typing import OrderedDict, List
from queue import SimpleQueue

class AtomicInt:
    """
    A class that implements Atomic Integer using locks
    """
    def __init__(self, start:int=0) -> None:
        self.__val = start
        self.__lock = threading.Lock()
        self.__zero_cv = threading.Condition(self.__lock)
    
    def increment(self, delta=1):
        with self.__lock:
            self.__val += delta
            if(self.__val==0):
                self.__zero_cv.notify_all()
        
    def decrement(self, delta=1):
        with self.__lock:
            self.__val -= delta
            if self.__val == 0:
                self.__zero_cv.notify_all()
    
    def set(self, val:int):
        with self.__lock:
            self.__val = val
            if self.__val==0:
                self.__zero_cv.notify_all()
    
    def get(self):
        with self.__lock:
            return self.__val
    
    def wait_until_zero(self):
        with self.__lock:
            while self.__val != 0:
                self.__zero_cv.wait()

class Entry:
    """
    Owned by SegmentMemory class, provides read and write guarantees.
    """

    def __init__(self, meta:SegmentMeta, init_segment_size, segment_size_inc) -> None:
        self.__mmap: mmap.mmap | None = None
        self.__file_obj: _io.BufferedRandom | None = None
        self.__capacity: int|None = None
        self.__filepath = meta.get_filepath()
        self.__mutable:bool = meta.is_mutable()
        self._refcount:AtomicInt = AtomicInt(0)
        self._lock: threading.Lock = threading.Lock()

        self.init_segment_size = init_segment_size
        self.segment_size_inc = segment_size_inc
    
    def load(self) -> None:
        '''Load resource before use'''
        # Never call it internally while holding the lock
        with self._lock:
            # Only load the file onces
            if self.__mmap is None:
                # ensure directory exists
                os.makedirs(os.path.dirname(self.__filepath), exist_ok=True)
                exists = os.path.exists(self.__filepath)
                # Load the file
                self.__file_obj = open(self.__filepath, 'r+b' if exists else 'wb+')
                self.__capacity = os.fstat(self.__file_obj.fileno()).st_size
                if(self.__capacity == 0):
                    # First time creating a file
                    # ensure file capacity
                    self.__file_obj.truncate(self.init_segment_size)
                    self.__capacity = self.init_segment_size


                self.__mmap = mmap.mmap(self.__file_obj.fileno(), 0)

                # Hint the os for sequential reads
                set_sequential_hint(self.__mmap, self.__file_obj.fileno())

    def read_bytes(self, offset: int, length: int) -> bytes:
        assert(self.__mmap is not None)
        return self.__mmap[offset : offset+length]

    
    def write(self, offset: int, msg: bytes) -> None:
        assert(self.__mmap is not None)
        assert(self.__mutable == True)
        required_capacity = offset+len(msg)
        with self._lock:
            self._ensure_capacity_locked(required_capacity)
            self.__mmap[offset:required_capacity] = msg
    
    def release(self) -> None:
        # Not thread-safe
        if self.__mmap is not None:
            if self.__mutable:
                self.__mmap.flush()
            self.__mmap.close()
            self.__mmap = None
        if self.__file_obj is not None:
            if self.__mutable:
                self.__file_obj.flush()
            self.__file_obj.close()
            self.__file_obj = None

    def _ensure_capacity_locked(self, capacity: int):
        """Internal function: Not thread safe """

        assert(self.__capacity is not None)
        assert(self.__file_obj is not None)
        assert(self.__mmap is not None)
        if(self.__capacity < capacity):
            # Increasing the segment size 
            new_capacity = max(capacity, self.segment_size_inc+self.__capacity)
            self.__file_obj.truncate(new_capacity)
            self.__mmap.resize(new_capacity)

            self.__capacity = new_capacity

class SegmentHandle:
    """A abstraction class that hides entry(class) method.
    A contextmanager owns the context of the handle.
    Responsible for releasing resources.
    """
    def __init__(self, memory: "SegmentMemory",entry: Entry):
        self.__memory = memory
        self.__entry = entry
    
    def __enter__(self):
        return self
    
    def read_bytes(self, offset:int, length: int) -> bytes:
        return self.__entry.read_bytes(offset, length)
    
    def write(self, offset:int, msg:bytes):
        return self.__entry.write(offset, msg)
    
    def release(self):
        '''Call to release the resources'''
        self.__memory._release_entry(self.__entry)
    
    def __exit__(self, exc_type, exc, tb):
        self.__memory._release_entry(self.__entry)

class SegmentMemory:
    """
    A resource class, it handle file system knows how to load the required files and 
    mmap it. Gives guranteed that file will be loaded when it's in use.
    Provides single threaded writes and multi-threaded read.

    Thread-safety contract:

    1. Writes to a segment MUST be externally serialized.
    2. Readers MUST NOT read beyond the last committed offset.
    3. SegmentMemory does NOT enforce commit visibility.
    4. Eviction waits until no active handles exist.
    5. Violating this contract results in undefined behavior.

    """

    def __init__(self, capacity:int, init_segment_size, segment_size_inc, policy: SegmentPolicy):

        self.init_segment_size = init_segment_size
        self.segment_size_inc = segment_size_inc

        self.__policy = policy

        # We are following reverse order for LRU, that last element is Most Used
        self.__cache: OrderedDict[str, Entry] = OrderedDict()
        self.__capacity:int = capacity
        # Use to protect access to __cache
        self.__cache_lock = threading.Lock()
        # A thread-safe queue for evicted entries to be release
        self.__cleanup_queue: SimpleQueue[Entry] = SimpleQueue()

        # Start the cleanup thread
        threading.Thread(
            target=self.__cleanup_thread,
            daemon=True
        ).start()
    
    def acquire(self,meta: SegmentMeta):
        key = meta.get_id()
        with self.__cache_lock:
            # Ensure entry exists
            # If entry doesn't exists create it and make sure it will exists releasing the lock
            if key not in self.__cache:
                entry = Entry(meta, self.init_segment_size, self.segment_size_inc)
                entry._refcount.increment()
                self.__cache[key] = entry
            else:
                entry = self.__cache[key]
                entry._refcount.increment()
            self.__cache.move_to_end(key)
            # Call the cleanup function 
            self.__cleanup_locked()
        # Load the resources
        entry.load()

        return SegmentHandle(self, entry)
    
    def _release_entry(self, entry: Entry):
        entry._refcount.decrement()
    
    def __cleanup_locked(self):
        """Internal function for cleanup. Not thead-safe.
        Never call this function without holding __cache_lock
        """
        while(len(self.__cache) > self.__capacity):
            _, oldest_entry = self.__cache.popitem(last=False)
            # Insert them into cleanup queue
            # it's thead-safe
            self.__cleanup_queue.put(oldest_entry)
    
    def force_evict(self, meta:SegmentMeta) -> None:
        """A blocking method that wait untils refcount becomes 0 and release the resources"""
        key = meta.get_id()
        with self.__cache_lock:
            entry = self.__cache[key]
            if entry is None:
                return 
            self.__cache.pop(key, None)

        # Wait till _refcount becomes 0
        # No need to held lock it is guranteed that after it is removed from __cache no one will acquire it
        self._force_evict_entry(entry)
    
    def _force_evict_entry(self, entry:Entry) -> None:
        """An internal function that blocks until _refcount of entry becomes zero.
            Clean the entry
        """
        entry._refcount.wait_until_zero()

        with entry._lock:
            entry.release()
    
    def try_evict(self, meta:SegmentMeta) -> bool:
        key = meta.get_id()
        with self.__cache_lock:
            entry = self.__cache[key]
            if entry is None:
                return True
            if(entry._refcount.get() > 0):
                return False
            # The acquisition of entry happens only under __cache_lock,
            # We will get constant value of _refcount while hoding __cache_lock
            # Removing it from cache so no one get this entry and refcount remains 0
            self.__cache.pop(key)
        
        with entry._lock:
            entry.release()
            return True
    
    def __cleanup_thread(self):
        while True:
            # Run infinitelly, releases the entries from __cleanup_queue
            entry = self.__cleanup_queue.get(block=True)
            entry._refcount.wait_until_zero()
            with entry._lock:
                entry.release()
