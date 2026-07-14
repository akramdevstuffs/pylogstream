from dataclasses import dataclass
import yaml

@dataclass(frozen=True)
class LogConfig:
    log_dir:str
    retension_ms: int
    rollover_ms: int
    max_segment_size: int
    init_segment_size: int
    segment_size_inc: int
    segment_cache_size: int = 100
    cleaner_service_interval: float = 2.0
    flush_service_interval: float = 2.0

@dataclass(frozen=True)
class WriterConfig:
    max_batch_size: int = 64
    max_batch_bytes: int = 1024*1024
    max_batch_wait: float = 0.002
    max_workers: int = 4

@dataclass(frozen=True)
class ControllerAddress:
    controller_id: str
    host: str
    port: int

@dataclass(frozen=True)
class BrokerConfig:
    host: str
    port: int
    controller_list: list[ControllerAddress]
    checksum_enable: bool
    writer_config: WriterConfig
    offset_topic: str  = "__consumer_offset"

@dataclass(frozen=True)
class ReplicaConfig:
    id: str
    fetch_size: int = 1024*1024
    pool_wait:int = 1
    min_isr_count: int = 2
    max_isr_lag_ms: int = 10000

@dataclass(frozen=True)
class Config:
    broker: BrokerConfig
    log: LogConfig
    replica: ReplicaConfig

def load_config(path: str) -> Config:
    with open(path) as f:
        data = yaml.safe_load(f)

    broker_data = data["broker"]

    return Config(
        broker=BrokerConfig(
            host=broker_data["host"],
            port=broker_data["port"],
            checksum_enable=broker_data["checksum_enable"],
            offset_topic=broker_data.get("offset_topic", "__consumer_offset"),
            writer_config=WriterConfig(**broker_data["writer_config"]),
            controller_list=[ControllerAddress(**ctrl) for ctrl in broker_data.get("controllers", [])]
        ),
        log=LogConfig(**data["log"]),
        replica=ReplicaConfig(**data["replica"])
    )