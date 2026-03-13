import threading
from dataclasses import dataclass
from typing import Dict
import asyncio
from pylogstream_broker.log.log_manager import LogManager
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from pylogstream_broker.config import WriterConfig


@dataclass(frozen=True)
class WriteRequest:
    producer_id:str
    topic: str
    data: bytes
    created_at: float
    callback: asyncio.Future

class Writer:
    def __init__(
            self, 
            log_manager: LogManager,
            config: WriterConfig
    ):
        self.__log_manager = log_manager
        self.config = config
        self.topic_queue: Dict[str, asyncio.Queue[WriteRequest]] = dict()

        self.executor = ThreadPoolExecutor(max_workers=config.max_workers)
    
    def submit(self, req: WriteRequest):
        # Submit the write request
        # Starts the collector coroutine if not active
        if req.topic not in self.topic_queue:
            self.topic_queue[req.topic] = asyncio.Queue()
            asyncio.create_task(self.topic_collector(req.topic))

        self.topic_queue[req.topic].put_nowait(req)

    async def topic_collector(self, topic: str):
        q: asyncio.Queue[WriteRequest] = self.topic_queue[topic]

        batch: list[WriteRequest] = []
        batch_bytes = 0
        start_time: float | None = None

        while True:

            # wait for first element if batch empty
            if not batch:
                req = await q.get()
                batch.append(req)
                batch_bytes += len(req.data)
                start_time = req.created_at
                continue

            assert(start_time)
            # compute remaining wait time
            elapsed = time.time() - start_time
            remaining = self.config.max_batch_wait - elapsed

            try:
                req = await asyncio.wait_for(q.get(), timeout=max(0, remaining))

                batch.append(req)
                batch_bytes += len(req.data)

                if (
                    len(batch) >= self.config.max_batch_size
                    or batch_bytes >= self.config.max_batch_bytes
                ):
                    self.executor.submit(self._flush_batch, topic, batch)
                    batch = []
                    batch_bytes = 0
                    start_time = None

            except asyncio.TimeoutError:
                # timeout reached -> flush
                if batch:
                    self.executor.submit(self._flush_batch, topic, batch)
                    batch = []
                    batch_bytes = 0
                    start_time = None
    
    def _flush_batch(self, topic, batch: list[WriteRequest]):
        data: list[bytes] = list(map(lambda req: req.data, batch))
        offsets = self.__log_manager.writer.write_batch(topic, data)
        for offset,req in zip(offsets, batch):
            req.callback.set_result(offset)
            req.callback.done()