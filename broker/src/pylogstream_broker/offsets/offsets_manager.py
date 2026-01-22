from log_manager import read_message, append_message, RETENSION
import time

INTERNAL_CONSUMER_LOG = "__consumer_offset"

client_offsets = {}

def load_client_offsets():
    entries = {}
    offset = 0
    while True:
        # No checksum of message in internal logs
        msg, new_offset = read_message(INTERNAL_CONSUMER_LOG, offset, check_hash=False)
        if offset == new_offset:
            break
        # Msg none means we reached a deleted/expired message
        # offset will be updated to next valid message
        if msg is None:
            continue
        parts = msg.split(' ')
        if len(parts)!=4:
            offset = new_offset
            continue
        ts_ms = int(parts[0])
        id = parts[1]
        topic = parts[2]
        topic_offset = int(parts[3])
        if id not in entries:
            entries[id] = {}
        entries[id][topic] = topic_offset
        offset = new_offset
    client_offsets = entries

    for id in list(client_offsets.keys()):
        print(f"Loaded offsets for client {id}: {client_offsets[id]}")

def get_client_offsets(id) -> dict:
    if id not in client_offsets:
        client_offsets[id] = {}
    return client_offsets[id]

def update_client_offset(id: str, topic: str, offset: int):
    if id not in client_offsets:
        client_offsets[id] = {}
    client_offsets[id][topic] = offset
    append_message(INTERNAL_CONSUMER_LOG, f"{int(time.time()*1000)} {id} {topic} {offset}".encode())