import asyncio
import json
from .models import BrokerNode, TopicMetadata


# ---------------------------------------------------------------------------
# In-memory storage (used by unit tests and as fallback)
# ---------------------------------------------------------------------------

class MetastoreStorage:
    def __init__(self):
        self._nodes: dict[str, BrokerNode] = {}
        self._topics: dict[str, TopicMetadata] = {}
        self._lock = asyncio.Lock()

    async def add_node(self, node: BrokerNode) -> None:
        async with self._lock:
            self._nodes[node.node_id] = node

    async def remove_node(self, node_id: str) -> BrokerNode | None:
        async with self._lock:
            return self._nodes.pop(node_id, None)

    async def get_node(self, node_id: str) -> BrokerNode | None:
        async with self._lock:
            return self._nodes.get(node_id)

    async def get_all_nodes(self) -> list[BrokerNode]:
        async with self._lock:
            return list(self._nodes.values())

    async def add_topic(self, topic: TopicMetadata) -> None:
        async with self._lock:
            if topic.topic in self._topics:
                raise ValueError(f"Topic {topic.topic} already exists")
            self._topics[topic.topic] = topic

    async def update_topic(self, topic: TopicMetadata) -> None:
        async with self._lock:
            if topic.topic not in self._topics:
                raise ValueError(f"Topic {topic.topic} does not exist")
            self._topics[topic.topic] = topic

    async def remove_topic(self, topic_name: str) -> TopicMetadata | None:
        async with self._lock:
            return self._topics.pop(topic_name, None)

    async def get_topic(self, topic_name: str) -> TopicMetadata | None:
        async with self._lock:
            return self._topics.get(topic_name)

    async def get_all_topics(self) -> list[TopicMetadata]:
        async with self._lock:
            return list(self._topics.values())


# ---------------------------------------------------------------------------
# ZooKeeper-backed storage
# ---------------------------------------------------------------------------

_BROKERS_PATH = "/pylogstream/brokers"
_TOPICS_PATH  = "/pylogstream/topics"


def _node_to_dict(node: BrokerNode) -> dict:
    return {"node_id": node.node_id, "host": node.host, "port": node.port}


def _node_from_dict(d: dict) -> BrokerNode:
    return BrokerNode(node_id=d["node_id"], host=d["host"], port=d["port"])


def _topic_to_dict(t: TopicMetadata) -> dict:
    return {
        "topic": t.topic,
        "leader_id": t.leader_id,
        "replica_list": t.replica_list,
        "isr_list": t.isr_list,
        "version": t.version,
    }


def _topic_from_dict(d: dict) -> TopicMetadata:
    return TopicMetadata(
        topic=d["topic"],
        leader_id=d["leader_id"],
        replica_list=d["replica_list"],
        isr_list=d["isr_list"],
        version=d["version"],
    )


class ZKMetastoreStorage:
    """ZooKeeper-backed metastore storage.

    ZNode layout:
        /pylogstream/brokers/<broker_id>  — persistent, JSON-encoded BrokerNode
        /pylogstream/topics/<topic_name>  — persistent, JSON-encoded TopicMetadata
    """

    def __init__(self, zk_client):
        self._zk = zk_client

    async def setup(self) -> None:
        """Ensure base paths exist. Call once after ZK is connected."""
        await self._zk.ensure_path(_BROKERS_PATH)
        await self._zk.ensure_path(_TOPICS_PATH)

    # ------------------------------------------------------------------
    # Broker nodes
    # ------------------------------------------------------------------

    async def add_node(self, node: BrokerNode) -> None:
        path = f"{_BROKERS_PATH}/{node.node_id}"
        from kazoo.exceptions import NodeExistsError
        try:
            await self._zk.create(path, _node_to_dict(node))
        except NodeExistsError:
            await self._zk.set(path, _node_to_dict(node))

    async def remove_node(self, node_id: str) -> BrokerNode | None:
        path = f"{_BROKERS_PATH}/{node_id}"
        existing = await self.get_node(node_id)
        await self._zk.delete(path)
        return existing

    async def get_node(self, node_id: str) -> BrokerNode | None:
        data = await self._zk.get(f"{_BROKERS_PATH}/{node_id}")
        return _node_from_dict(data) if data else None

    async def get_all_nodes(self) -> list[BrokerNode]:
        children = await self._zk.get_children(_BROKERS_PATH)
        nodes = []
        for child in children:
            node = await self.get_node(child)
            if node:
                nodes.append(node)
        return nodes

    # ------------------------------------------------------------------
    # Topics
    # ------------------------------------------------------------------

    async def add_topic(self, topic: TopicMetadata) -> None:
        path = f"{_TOPICS_PATH}/{topic.topic}"
        from kazoo.exceptions import NodeExistsError
        try:
            await self._zk.create(path, _topic_to_dict(topic))
        except NodeExistsError:
            raise ValueError(f"Topic {topic.topic} already exists")

    async def update_topic(self, topic: TopicMetadata) -> None:
        path = f"{_TOPICS_PATH}/{topic.topic}"
        exists = await self._zk.exists(path)
        if not exists:
            raise ValueError(f"Topic {topic.topic} does not exist")
        await self._zk.set(path, _topic_to_dict(topic))

    async def remove_topic(self, topic_name: str) -> TopicMetadata | None:
        existing = await self.get_topic(topic_name)
        await self._zk.delete(f"{_TOPICS_PATH}/{topic_name}")
        return existing

    async def get_topic(self, topic_name: str) -> TopicMetadata | None:
        data = await self._zk.get(f"{_TOPICS_PATH}/{topic_name}")
        return _topic_from_dict(data) if data else None

    async def get_all_topics(self) -> list[TopicMetadata]:
        children = await self._zk.get_children(_TOPICS_PATH)
        topics = []
        for child in children:
            topic = await self.get_topic(child)
            if topic:
                topics.append(topic)
        return topics
