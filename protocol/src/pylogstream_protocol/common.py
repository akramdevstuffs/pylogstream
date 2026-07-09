from dataclasses import dataclass, field

@dataclass
class PacketHeader:
    correlation_id: str

@dataclass(slots=True)
class Packet:
    header: PacketHeader|None = field(default=None, init=False)