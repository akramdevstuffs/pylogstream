from dataclasses import dataclass

@dataclass
class BrokerNode:
    node_id: str
    host: str
    port: int

@dataclass
class TopicMetadata:
    topic: str
    leader_id: str
    replica_list: list[str]
    isr_list: list[str]
    version: int = 1
