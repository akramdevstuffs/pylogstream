import asyncio
from .models import BrokerNode, TopicMetadata

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
