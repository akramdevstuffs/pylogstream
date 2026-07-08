from dataclasses import dataclass

class Command:
    pass

@dataclass
class RegisterCommand(Command):
    pass


@dataclass
class ClientIdCommand(Command):
    client_id: str


@dataclass
class PublishCommand(Command):
    topic: str
    payload: bytes
    acks: int  # allowed values -1, 0, 1
    contains_checksum: bool

@dataclass
class FetchOffsetCommand(Command):
    topic: str

@dataclass
class PullCommand(Command):
    topic: str
    offset: int
    size: int

@dataclass
class SubscribeCommand(Command):
    topic: str


@dataclass
class CommitOffsetCommand(Command):
    topic: str
    offset: int
    acks: int # -1, 0, 1


@dataclass
class PingCommand(Command):
    pass

@dataclass
class ReplicaCommand(Command):
    # Just a class for type hinting
    topic: str

@dataclass 
class ReplicaFetchCommand(ReplicaCommand):
    offset: int
    size: int
    replica_id: str

@dataclass
class ControllerCommand(Command):
    pass

@dataclass
class RegisterTopicCommand(ControllerCommand):
    topic: str


@dataclass
class MetadataRequestCommand(ControllerCommand):
    topic: str


@dataclass
class BrokerRegisterCommand(ControllerCommand):
    broker_id: str
    host: str
    port: int


@dataclass
class ControllerPingCommand(ControllerCommand):
    broker_id: str


@dataclass
class BrokerMetadataListCommand(ControllerCommand):
    broker_id: str

@dataclass
class ISRChangeCommand(ControllerCommand):
    topic: str
    isr_list: list[str]