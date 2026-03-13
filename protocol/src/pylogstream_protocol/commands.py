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


@dataclass
class PingCommand(Command):
    pass