from pylogstream_protocol.commands import ReplicaCommand, ReplicaFetchCommand
from pylogstream_protocol.response import (
    ReplicaResponse, 
    ReplicaFetchBytesResponse, 
    ReplicaFetchFileResponse,
    ReplicaFetchMmapResponse
)
from pylogstream_broker.replication.error import UnauthorisedReplica
from pylogstream_broker.log.log_manager import LogManager

from collections import defaultdict
import time
import asyncio
import heapq

class ReplicaLeader:

    def __init__(
            self,
            topic: str,
            id: str,
            replica_list: list[str],
            log_manager: LogManager,
            min_isr_count: int = 2,
            max_isr_lag_ms: int = 10000
            ):
        self.topic = topic
        self.id = id
        self.replica_list = replica_list

        self.min_isr_count = min_isr_count
        self.max_isr_lag_ms = max_isr_lag_ms

        # Initially only leader is part of ISR
        self.in_sync_replica = [id]

        self.replica_offset: dict[str, int] = defaultdict(int)

        self.log_manager = log_manager

        # Init the hw_water queue
        self.hw_waiter_queue: list[tuple[int, asyncio.Future]] = []

        # On init first fetch the current Log End offset of leader
        self.replica_offset[id] = self.log_manager.get_latest_offset(self.topic)
        # Compute the initial HW based on leader LEO
        # Internal HW, may store stale values
        self._high_watermark = self._compute_high_watermark()

        # Stores the last timestamp of a replica when it reached till LEO of leader
        self.replica_last_caugthup: dict[str, float] = defaultdict(float)
    
    def handle(self, cmd: ReplicaCommand) -> ReplicaResponse:
        # TODO: Proper route handling for normal replicaCommands
        if not isinstance(cmd, ReplicaFetchCommand):
            raise Exception("Not a fetch command")
        if not cmd.replica_id in self.replica_list:
            raise UnauthorisedReplica(cmd.replica_id, self.replica_list)

        # Fetch leo to send with resposne
        log_end_offset = self.get_log_end_offset()

        # Fetch hw to send with response
        high_watermark = self.get_high_watermark()

        # Check if replica is completely replicated the leader
        if cmd.offset == log_end_offset:
            self.replica_last_caugthup[cmd.replica_id] = time.time()
        
        # Commit the last fetched offset as processed by the replica
        self.replica_offset[cmd.replica_id] = cmd.offset

        # Send by copying bytes if size is small
        if(cmd.size <= 1024):
            res = self.log_manager.read_bytes(cmd.topic,cmd.offset,cmd.size)
            return ReplicaFetchBytesResponse(
                cmd.topic, 
                log_end_offset,
                high_watermark,
                len(res.data),
                res.data
                )
        else:
            res = self.log_manager.read(cmd.topic, cmd.offset, cmd.size)
            if(res.file_slice is not None):
                return ReplicaFetchFileResponse(
                    cmd.topic, 
                    log_end_offset,
                    high_watermark,
                    res.file_slice.batch_size,
                    res.file_slice.filepath, 
                    res.file_slice.offset, 
                    res.file_slice.batch_size
                )
            # TODO: Implement send by memoryview and design clean owner ship of who will free it
            else:
                # Temporary fallback: copy from mmap into bytes
                data = bytes(res.result)

                return ReplicaFetchBytesResponse(
                    cmd.topic,
                    log_end_offset,
                    high_watermark,
                    len(data),
                    data
                )
        
    async def wait_for_hw(self, offset:int):
        hw = self.get_high_watermark()

        if hw>=offset:
            return
        
        fut = asyncio.get_running_loop().create_future()
        heapq.heappush(self.hw_waiter_queue, (offset,fut))

        await fut
        return fut.result()
    
    async def close(self) -> None:
        # Just a placeholder function for cleanup
        # cleanup the waiter_queue
        for offset, fut in self.hw_waiter_queue:
            fut.set_exception(Exception("Leader is not available"))
    
    def get_log_end_offset(self):
        return self.log_manager.get_latest_offset(self.topic)
    
    def _compute_high_watermark(self) -> int:
        # An internal function to update highwatermark
        if not self.in_sync_replica:
            return 0
        
        # Updating the leader own log end offset
        self.replica_offset[self.id] = self.get_log_end_offset()

        return min(self.replica_offset[r] for r in self.in_sync_replica)
    
    def get_high_watermark(self) -> int:
        # A wrapper around _compute_high_watermark
        # Can be used with caching strategies later to avoid calling log_manager cost
        # Update the internal high watermark
        self._high_watermark = max(self._high_watermark, self._compute_high_watermark())
        return self._high_watermark
    
    def _notify_hw_waiters(self, high_watermark: int):
        while  (len(self.hw_waiter_queue)>0 
            and self.hw_waiter_queue[0][0] <= high_watermark):
            offset, fut = heapq.heappop(self.hw_waiter_queue)
            fut.set_result(True)

    def _update_isr(self):
        # An internal function: recompute ISR list
        current_time = time.time()

        # New list which initially only contains leader
        new_isr_list = [self.id]

        # how many more isr must be added
        needed = max(0,self.min_isr_count - 1)

        replica_list = self.replica_list.copy()
        replica_list.remove(self.id)
        # Sort replica_list by last caugthed up 
        replica_list.sort(
            key=lambda replica_id: current_time - self.replica_last_caugthup[replica_id],
            reverse=True
        )
        # First include the minimum number of isr
        while len(replica_list) and needed > 0:
            needed -= 1
            new_isr_list.append(replica_list[-1])
            replica_list.pop()
        # Add all the eligible replica's in isr
        for replica_id in reversed(replica_list):
            if (current_time - self.replica_last_caugthup[replica_id] 
                    <= self.max_isr_lag_ms
                ):
                new_isr_list.append(replica_id)
            else:
                break
        # Swap the isr_list
        self.in_sync_replica = new_isr_list