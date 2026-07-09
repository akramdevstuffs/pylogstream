from dataclasses import dataclass
from pylogstream_protocol.common import Packet

class Command(Packet):
    pass

class RegisterCommand(Command):
    pass


@dataclass(slots=True)
class ClientIdCommand(Command):
    client_id: str


@dataclass(slots=True)
class PublishCommand(Command):
    topic: str
    payload: bytes
    acks: int  # allowed values -1, 0, 1
    contains_checksum: bool

@dataclass(slots=True)
class FetchOffsetCommand(Command):
    topic: str

@dataclass(slots=True)
class PullCommand(Command):
    topic: str
    offset: int
    size: int

@dataclass(slots=True)
class SubscribeCommand(Command):
    topic: str


@dataclass(slots=True)
class CommitOffsetCommand(Command):
    topic: str
    offset: int
    acks: int # -1, 0, 1


class PingCommand(Command):
    pass

@dataclass(slots=True)
class ReplicaCommand(Command):
    # Just a class for type hinting
    topic: str

@dataclass(slots=True)
class ReplicaFetchCommand(ReplicaCommand):
    offset: int
    size: int
    replica_id: str

class ControllerCommand(Command):
    pass

@dataclass(slots=True)
class RegisterTopicCommand(ControllerCommand):
    topic: str


@dataclass(slots=True)
class MetadataRequestCommand(ControllerCommand):
    topic: str


@dataclass(slots=True)
class BrokerRegisterCommand(ControllerCommand):
    broker_id: str
    host: str
    port: int


@dataclass(slots=True)
class ControllerPingCommand(ControllerCommand):
    broker_id: str


@dataclass(slots=True)
class BrokerMetadataListCommand(ControllerCommand):
    broker_id: str

@dataclass(slots=True)
class ISRChangeCommand(ControllerCommand):
    topic: str
    isr_list: list[str]