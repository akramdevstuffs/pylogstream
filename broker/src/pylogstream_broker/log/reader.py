import threading
from dataclasses import dataclass
from typing import Dict
import asyncio
from queue import SimpleQueue, Empty
from log_manager import read_message

@dataclass
class ReadRequest:
    client_id:str
    topic: str
    offset: int
    max_messages: int = 10

@dataclass
class ReadResult:
    topic: str
    msg: str|None
    offset: int
    done: bool

class ReaderThread(threading.Thread):
    def __init__(self, loop: asyncio.AbstractEventLoop, request_q: SimpleQueue, client_queues: Dict[str, asyncio.Queue], check_hash:bool=True):
        super().__init__(daemon=True)
        self.request_queue = request_q
        self.client_queues = client_queues
        self.loop = loop
        self.check_hash = check_hash
        self.running = True

    def stop(self):
        self.running = False
        # Sending None message so queue waiting for message wakes up
        self.request_queue.put(None)

    def run(self):
        """
        Main reader loop:
         - collect requests from the queue
         - perform the read operations
         - deliver results to asyncio queues via loop.call_soon_threadsafe(...)
        """
        print("Reader thread started")
        while self.running:
            try:
                req:ReadRequest = self.request_queue.get()
            except Empty:
                continue
            offset = req.offset
            batch = []
            for _ in range(req.max_messages):
                old_offset = offset
                msg, offset = read_message(req.topic, offset, check_hash=self.check_hash)
                if msg is None and offset==old_offset:
                    break
                batch.append(ReadResult(req.topic, msg,offset,False))
            if batch:
                self.loop.call_soon_threadsafe(
                    self._push_batch, req.client_id, batch,offset
                )

    def _push_batch(self, client_id, batch, offset):
        q = self.client_queues[client_id]
        for r in batch:
            if not q.full():
                q.put_nowait(r)
        q.put_nowait(ReadResult(batch[-1].topic, None, offset, True))
