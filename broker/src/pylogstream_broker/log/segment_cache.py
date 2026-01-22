from collections import OrderedDict
import threading

class LRUCache:
    def __init__(self, capacity: int, callback):
        self.cache = OrderedDict()
        self.capacity = capacity
        self.lock = threading.Lock()
        self.callback = callback

    def get(self, key: str):
        with self.lock:
            if key not in self.cache:
                return None
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: str, value):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                oldest_key, oldest_value = self.cache.popitem(last=False)
                self.callback(oldest_key, oldest_value)
    def remove(self, key: str):
        with self.lock:
            if key in self.cache:
                value = self.cache.pop(key)
                self.callback(key, value)