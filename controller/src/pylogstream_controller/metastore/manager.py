import hashlib
import bisect
from .storage import MetastoreStorage
from .models import BrokerNode, TopicMetadata

class ConsistentHashRing:
    def __init__(self, replicas=50):
        self.replicas = replicas
        self.ring = {}
        self._sorted_keys = []

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16) & 0xFFFFFFFF

    def add_node(self, node_id: str) -> None:
        # First remove virtual nodes to avoid duplicates if re-registering
        self.remove_node(node_id)
        for i in range(self.replicas):
            val = f"{node_id}-vnode-{i}"
            key = self._hash(val)
            self.ring[key] = node_id
            bisect.insort(self._sorted_keys, key)

    def remove_node(self, node_id: str) -> None:
        for i in range(self.replicas):
            val = f"{node_id}-vnode-{i}"
            key = self._hash(val)
            if key in self.ring:
                del self.ring[key]
                idx = bisect.bisect_left(self._sorted_keys, key)
                if idx < len(self._sorted_keys) and self._sorted_keys[idx] == key:
                    self._sorted_keys.pop(idx)

    def get_node(self, topic: str) -> str | None:
        if not self.ring:
            return None
        key = self._hash(topic)
        idx = bisect.bisect_right(self._sorted_keys, key)
        if idx == len(self._sorted_keys):
            idx = 0
        return self.ring[self._sorted_keys[idx]]

    def get_replicas(self, topic: str, count: int) -> list[str]:
        if not self.ring:
            return []
        key = self._hash(topic)
        idx = bisect.bisect_right(self._sorted_keys, key)
        
        replicas = []
        n = len(self._sorted_keys)
        for i in range(n):
            curr_idx = (idx + i) % n
            node = self.ring[self._sorted_keys[curr_idx]]
            if node not in replicas:
                replicas.append(node)
                if len(replicas) == count:
                    break
        return replicas


class MetastoreManager:
    def __init__(self, replicas_per_topic=3, storage=None):
        self.storage = storage if storage is not None else MetastoreStorage()
        self.ring = ConsistentHashRing()
        self.replicas_per_topic = replicas_per_topic

    def _elect_leader(self, topic_meta: "TopicMetadata") -> str:
        """Elect a leader for a topic using ISR-based selection.

        Priority:
          1. First broker in isr_list  (healthy, fully caught-up replicas)
          2. First broker in replica_list (assigned replicas that may be lagging)
          3. Hash-ring fallback          (any live broker)
        """
        if topic_meta.isr_list:
            return topic_meta.isr_list[0]
        if topic_meta.replica_list:
            return topic_meta.replica_list[0]
        return self.ring.get_node(topic_meta.topic) or ""

    async def register_broker(self, node_id: str, host: str, port: int) -> BrokerNode:
        node = BrokerNode(node_id=node_id, host=host, port=port)
        await self.storage.add_node(node)
        self.ring.add_node(node_id)
        return node

    async def deregister_broker(self, node_id: str) -> list[TopicMetadata]:
        await self.storage.remove_node(node_id)
        self.ring.remove_node(node_id)
        
        updated_topics = []
        all_topics = await self.storage.get_all_topics()
        all_nodes = await self.storage.get_all_nodes()
        active_node_ids = [n.node_id for n in all_nodes]
        
        for topic_meta in all_topics:
            needs_update = False
            replica_list = topic_meta.replica_list
            
            if node_id in replica_list:
                new_replicas = [r for r in replica_list if r != node_id]
                if len(new_replicas) < self.replicas_per_topic and active_node_ids:
                    h_replicas = self.ring.get_replicas(topic_meta.topic, self.replicas_per_topic)
                    for r in h_replicas:
                        if r not in new_replicas:
                            new_replicas.append(r)
                            if len(new_replicas) == self.replicas_per_topic:
                                break
                
                topic_meta.replica_list = new_replicas
                needs_update = True

            if topic_meta.leader_id == node_id:
                # Remove the failed node from ISR before electing a new leader
                topic_meta.isr_list = [r for r in topic_meta.isr_list if r != node_id]
                topic_meta.leader_id = self._elect_leader(topic_meta)
                needs_update = True
            
            if needs_update:
                topic_meta.version += 1
                await self.storage.update_topic(topic_meta)
                updated_topics.append(topic_meta)
                
        return updated_topics

    async def register_topic(self, topic_name: str) -> TopicMetadata:
        existing = await self.storage.get_topic(topic_name)
        if existing:
            return existing
        
        replicas = self.ring.get_replicas(topic_name, self.replicas_per_topic)
        leader = replicas[0] if replicas else ""
        
        meta = TopicMetadata(
            topic=topic_name,
            leader_id=leader,
            replica_list=replicas,
            isr_list=replicas.copy(),
            version=1
        )
        await self.storage.add_topic(meta)
        return meta
    
    async def update_topic_isr(self, topic_name: str, isr_list: list[str]) -> TopicMetadata | None:
        topic_meta = await self.storage.get_topic(topic_name)
        if topic_meta:
            topic_meta.isr_list = isr_list
            # Re-elect a leader if the current leader is no longer in the ISR
            if topic_meta.leader_id not in isr_list:
                topic_meta.leader_id = self._elect_leader(topic_meta)
            topic_meta.version += 1
            await self.storage.update_topic(topic_meta)
            return topic_meta
        return None

    async def get_topic_metadata(self, topic_name: str) -> TopicMetadata | None:
        return await self.storage.get_topic(topic_name)

    async def get_all_topics(self) -> list[TopicMetadata]:
        return await self.storage.get_all_topics()
        
    async def get_node(self, node_id: str) -> BrokerNode | None:
        return await self.storage.get_node(node_id)
