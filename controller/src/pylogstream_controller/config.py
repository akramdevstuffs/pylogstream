from dataclasses import dataclass
import yaml

@dataclass(frozen=True)
class ControllerConfig:
    host: str
    port: int

@dataclass(frozen=True)
class Config:
    controller: ControllerConfig

def load_config(path: str) -> Config:
    with open(path) as f:
        data = yaml.safe_load(f)
    return Config(
        controller=ControllerConfig(
            host=data['controller']['host'],
            port=data['controller']['port']
        )
    )