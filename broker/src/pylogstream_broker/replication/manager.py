from pylogstream_broker.config import ReplicaConfig
from pylogstream_broker.log.log_manager import LogManager
from pylogstream_broker.replication.leader import ReplicaLeader
from pylogstream_broker.replication.fetcher import ReplicaFetcher
from pylogstream_protocol.commands import ReplicaCommand
from pylogstream_protocol.response import (
    ReplicaResponse,
    TopicMetaDataResponse,
    TopicMetaDataListResponse,
)
from enum import Enum
from dataclasses import dataclass
import asyncio
from typing import Callable, Awaitable
from collections import Counter


class Role(Enum):
    LEADER = "leader"
    FOLLOWER = "follower"

@dataclass
class TopicState:
    topic: str
    role: Role
    leader_id: str
    leader_addr: str
    leader_port: int 
    replicas: list[str]
    version: int = 0
    leader: ReplicaLeader | None = None
    fetcher: ReplicaFetcher | None = None
    loop_task: asyncio.Task | None = None
    running: bool = False

    def compare_meta(self, another_state:"TopicState") -> bool:
        # Compare another state with self to check all the metadata are same
        if another_state is None:
            return False
        return (
            self.topic == another_state.topic and 
            self.role == another_state.role and 
            self.leader_id == another_state.leader_id and
            self.leader_addr == another_state.leader_addr and 
            self.leader_port == another_state.leader_port and
            set(self.replicas) == set(another_state.replicas)
        )


class ReplicaManager:

    def __init__(
        self,
        log_manager: LogManager,
        config: ReplicaConfig,
        on_isr_change: Callable[[str, list[str]], Awaitable[None]] | None = None
    ):
        self.config = config
        self.broker_id = self.config.id

        self.topics: dict[str, TopicState] = {}
        self.log_manager = log_manager
        self.on_isr_change = on_isr_change

        self.update_isr_delay = self.config.max_isr_lag_ms / 2

    async def apply_metadata(self, meta: TopicMetaDataResponse):
        # Applies the new_metadata to the manager

        #check if it's part of the replicas
        if self.broker_id not in meta.replica_list:
            # It's is not in the replica_list
            # Do the cleanup
            await self._remove_topic(meta.topic, log_cleanup=True)
            return
        
        role = ( Role.LEADER if meta.leader_id == self.broker_id 
                else Role.FOLLOWER)

        state = TopicState(
            topic=meta.topic,
            leader_id=meta.leader_id,
            leader_addr=meta.leader_addr,
            leader_port=meta.leader_port,
            role=role,
            replicas=meta.replica_list,
            version=meta.version
        )

        if state.topic in self.topics:
            # Topic already exists upgrade or downgrade
            old_state = self.topics[state.topic]

            # Check for metadata version is greater than old state else ignore
            if meta.version <= old_state.version:
                # There's no newer metadata
                return

            if state.compare_meta(old_state):
                # There's no metadata change
                return
            
            # Close all state
            await self._close_state(old_state)
        
        # Update the state
        self.topics[state.topic] = state

        await self._start_topic(state)
    
    async def apply_metadata_list(self, meta_list_resp: TopicMetaDataListResponse):
        new_topics = set(meta.topic for meta in meta_list_resp.meta_list)

        old_topics = list(self.topics.keys())

        for topic in old_topics:
            # Check if old_topic is still present else cleanup
            if topic not in new_topics:
                await self._remove_topic(topic)
        
        # Apply the meta list
        for meta in meta_list_resp.meta_list:
            await self.apply_metadata(meta)
    
    def get_leader(self, topic: str) -> ReplicaLeader:
        if topic not in self.topics:
            print(f"Topic {topic} not found in local registry")
            print("Available topics:", list(self.topics.keys()))
            # TODO: Raise specific TopicNotFoundError
            raise Exception("Topic doesn't exists")
        
        state = self.topics[topic]

        if state.role!=Role.LEADER or state.leader is None:
            # TODO: Raise Reading from non leader node
            raise Exception("Broker is not leader of this topic")
        
        return state.leader

    async def close(self, topic:str):
        if topic in self.topics:
            await self._remove_topic(topic, log_cleanup=False)
    
    async def close_all(self):
        # Close all currently running Leaders and Fetchers
        topic_list = list(self.topics.keys())
        for topic in topic_list:
            # Try to close topic
            await self._remove_topic(topic, log_cleanup=False)

    # --------------------------
    # INTERNAL HELPERS
    # --------------------------

    async def _start_topic(self, state: TopicState):

        # First make that topic exists in log manager
        self.log_manager.create_topic(state.topic)

        if state.role == Role.LEADER:
            state.leader = ReplicaLeader(
                topic=state.topic,
                id=self.broker_id,
                replica_list=state.replicas,
                log_manager=self.log_manager,
                min_isr_count=self.config.min_isr_count,
                max_isr_lag_ms=self.config.max_isr_lag_ms
            )

            state.running = True
            state.loop_task = asyncio.create_task(self._leader_loop(state))

        else:
            state.fetcher = await ReplicaFetcher.create(
                topic=state.topic,
                leader_addr=state.leader_addr,
                leader_port=state.leader_port,
                broker_id=self.broker_id,
                fetch_size=self.config.fetch_size,
                pool_wait=self.config.pool_wait,
                log_manager=self.log_manager
            )

            state.running = True
            state.loop_task = asyncio.create_task(self._follower_loop(state))
    
    async def _close_state(self, state: TopicState):
        state.running = False
        
        ## First make sure to stop background_loop before closing fetcher or leader
        # Stop loop
        if state.loop_task:
            state.loop_task.cancel()
            try:
                await state.loop_task
            except asyncio.CancelledError:
                pass

        # Stop fetcher
        if state.fetcher:
            await state.fetcher.close()
        
        if state.leader:
            await state.leader.close()

    async def _remove_topic(self, topic: str, log_cleanup: bool = True) -> bool:
        ''' 
        An internal function to remove topic and optional cleanup\n
        Tries to remove the topic from registry\n
        Returns True if successfully deleted and returns false if topic doesn't exists
        '''
        state = self.topics.get(topic)

        if state is None:
            # Topic doesn't exists
            return False

        await self._close_state(state)

        if log_cleanup:
            #TODO: Implement log cleanup here
            pass

        del self.topics[topic]
        return True

    async def _leader_loop(self, state: TopicState):
        while state.running and state.role == Role.LEADER:
            assert state.leader is not None
            old_isr = state.leader.in_sync_replica.copy()
            state.leader._update_isr()
            if self.on_isr_change:
                if Counter(old_isr) != Counter(state.leader.in_sync_replica):
                    self.on_isr_change(state.topic, state.leader.in_sync_replica.copy())
            await asyncio.sleep(self.update_isr_delay)

    async def _follower_loop(self, state: TopicState):
        # placeholder loop (optional)
        while state.running and state.role == Role.FOLLOWER:
            await asyncio.sleep(1)

    # --------------------------
    # REQUEST HANDLING
    # --------------------------

    def handle(self, cmd: ReplicaCommand) -> ReplicaResponse:
        state = self.topics.get(cmd.topic)

        if state is None:
            # TODO: raise TopicNotFoundError and do proper error handling
            raise Exception("Topic not found")

        if state.role != Role.LEADER:
            # TODO: raise suitable error and do proper error handling
            raise Exception("Follower cannot serve replica fetch")

        assert state.leader is not None
        return state.leader.handle(cmd)