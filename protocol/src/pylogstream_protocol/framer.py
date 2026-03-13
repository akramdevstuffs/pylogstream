PREFIX_SIZE = 4

def encode_frame(payload: bytes) -> bytes:
    return len(payload).to_bytes(4,"big") + payload

def decode_length(prefix: bytes) -> int:
    return int.from_bytes(prefix,"big")