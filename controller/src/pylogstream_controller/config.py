from dataclasses import dataclass, field
import yaml


@dataclass(frozen=True)
class ControllerConfig:
    host: str
    port: int
    zk_hosts: str = "127.0.0.1:2181"
    controller_id: str = ""   # defaults to "host:port" if empty


@dataclass(frozen=True)
class Config:
    controller: ControllerConfig


def load_config(path: str) -> Config:
    with open(path) as f:
        data = yaml.safe_load(f)
    ctrl = data.get("controller", {})
    return Config(
        controller=ControllerConfig(
            host=ctrl["host"],
            port=ctrl["port"],
            zk_hosts=ctrl.get("zk_hosts", "127.0.0.1:2181"),
            controller_id=ctrl.get("controller_id", ""),
        )
    )