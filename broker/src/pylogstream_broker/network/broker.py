import uuid
import socket
import asyncio
import time
from log_manager import append_message, start_threads,load_topics_log, check_message_available, get_latest_offset
from offsets_manager import update_client_offset, get_client_offsets, load_client_offsets
from concurrent.futures import ThreadPoolExecutor
from reader import ReaderThread, ReadRequest, ReadResult
import os
import queue
from typing import Dict

if os.name == "posix":
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("Using uvloop for asyncio")
else:
    print("Using default asyncio loop")

# Increasing the soft fds limit
import resource
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))

HOST = "0.0.0.0"
PORT = 1234


BATCH_SIZE = 50       # drain after 50 messages
MAX_BUFFERED = 32_000 # or when >64KB of data queued
LINGER_MS = 10    # only wait 50ms before draining
BEAT_MAX_DELAY = 120  # seconds

READER_THREAD_COUNT = 32

MESSAGE_CHECKSUM_ENABLE = True # Enables the message integrity checks

pool = ThreadPoolExecutor(max_workers=100)

# client last beat
client_heartbeats: Dict[str,float] = {}

topic_events = {}

clients_task = {}

reader_threads = None

reader_req_qs = [queue.SimpleQueue() for _ in range(READER_THREAD_COUNT)]
reader_res_qs: Dict[str, asyncio.Queue] = {}

# Map for reader_q assigned to topic as int representing index
__reader_q = {}
__last_q_assigned = 0
# We will assign each topic to a separte reader thread
def get_req_q(topic:str):
    global __last_q_assigned
    global __reader_q
    if(topic in __reader_q):
        return reader_req_qs[__reader_q[topic]]
    else:
        __last_q_assigned = (__last_q_assigned+1)%READER_THREAD_COUNT
        __reader_q[topic] = __last_q_assigned
        print(f"{topic} -> {__last_q_assigned}")
        return reader_req_qs[__last_q_assigned]

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    client_id = None
    sock = writer.get_extra_info("socket")
    if sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    while True:
        try:
            len_bytes = await reader.readexactly(4)
            if not len_bytes or len_bytes == b'\x00\x00\x00\x00':
                break
            msg_length = int.from_bytes(len_bytes, 'big')
            msg_bytes = await reader.readexactly(msg_length)
            command = msg_bytes[:3].decode()
        except Exception as e:
            return
        if command == 'REG':
            client_id = str(uuid.uuid4())
            id_bytes = client_id.encode()
            try:
                writer.write(len(id_bytes).to_bytes(4,'big') + id_bytes)
                await writer.drain()
            except Exception:
                return
        elif command == 'CID':
            msg = msg_bytes.decode()
            client_id = msg.split(' ', 1)[1]
            if client_id in clients_task:
                # Writer from previous connection still active, close this one
                task = clients_task[client_id]
                task.cancel()
                clients_task.pop(client_id,None)
            client_heartbeats[client_id] = time.time()
            reader_res_qs[client_id] = asyncio.Queue(maxsize=1000)
        elif client_id is None:
            # Client must register first
            return
        elif command == 'SUB':
            msg = msg_bytes.decode()
            parts = msg.split(' ')
            topic = parts[1]
            if topic not in get_client_offsets(client_id):
                update_client_offset(client_id, topic, 0)
            if topic not in topic_events:
                topic_events[topic] = asyncio.Event()
            # Start client writer task
            if not clients_task.get(client_id) :
                task = asyncio.create_task(client_writer(writer, client_id))
                task.add_done_callback(lambda t,cid=client_id:
                    clients_task.pop(cid,None))
                clients_task[client_id] = task
        # For setting offsets from clients side
        elif command == 'SET':
            msg = msg_bytes.decode()
            parts = msg.split(' ')
            topic = parts[1]
            offset = int(parts[2])
            # If offset -1 set it to latest_offset
            if offset==-1:
                offset = get_latest_offset(topic)
            update_client_offset(client_id, topic, offset)
        elif command == 'PUB':
            msg = msg_bytes.decode()
            parts = msg.split(' ', 2)
            topic = parts[1]
            conv = parts[2]
            # Append timestamp
            # conv += " "+str(time.time())
            if MESSAGE_CHECKSUM_ENABLE:
                hash = await reader.readexactly(4) # Reads the 4 byte for checksum
            else:
                hash = None
            # loop = asyncio.get_running_loop()
            # code = await loop.run_in_executor(pool, partial(append_message, topic, conv.encode(), hash))
            code = append_message(topic, conv.encode(), hash)
            if code==0:
                if topic in topic_events:
                    topic_events[topic].set()
                    topic_events[topic].clear()
            else:
                print('status code',code)
            # Add the ack logic here
            pass

        # Heart beat from client
        elif command=='PNG':
            client_heartbeats[client_id] = time.time()
            print(f"Client {client_id} heartbeat at {time.time()}")

async def client_writer(writer: asyncio.StreamWriter, client_id: str):
    count = 0
    buffered = 0
    timestamp = time.time()
    updated_offsets = {}
    transport = writer.transport
    loop = asyncio.get_running_loop()
    # Checking the client last beat
    while client_heartbeats.get(client_id,0)+BEAT_MAX_DELAY > time.time():
        # exit if connection closed
        if transport.is_closing():
            print(f"Connection closed for client {client_id}")
            return
        if count == 0:
            timestamp = time.time()
        # Get current offsets
        client_offsets = get_client_offsets(client_id)
        topics_request = set() # Keeps tab on requested data from reader thread
        for topic in client_offsets.keys():
            offset = client_offsets[topic]
            if topic in updated_offsets:
                offset = updated_offsets[topic]
            if(not check_message_available(topic, offset)):
                continue
            reader_req = get_req_q(topic)
            reader_req.put(ReadRequest(
                client_id=client_id,
                topic=topic,
                offset=offset,
            ))
            topics_request.add(topic)
        new_msg_arrived: bool = False
        while topics_request:
            read_result: ReadResult = await reader_res_qs[client_id].get()
            if(read_result.done):
                topics_request.remove(read_result.topic)
                continue
            if read_result.msg is None:
                print('None message received',read_result.topic, read_result.offset)
            new_msg_arrived = True
            if read_result.msg is not None:
                try:
                    """
                    # Code for calculating delay on server side
                        ts = float(read_result.msg.split(' ')[-1])
                        delay = (time.time() -ts)*1000
                        print(f"delay: {delay}ms new_offset: {read_result.offset}")
                    """
                    resp_bytes = f'{read_result.topic} {read_result.msg}'.encode()

                    writer.write(len(resp_bytes).to_bytes(4,'big') + resp_bytes)
                    buffered += len(resp_bytes) + 4
                    count += 1
                    updated_offsets[read_result.topic] = read_result.offset
                except Exception:
                    print(f"Exception sending to client {client_id}")
                    return
            else:
                updated_offsets[read_result.topic] = read_result.offset

        if buffered > 0 and ((not new_msg_arrived) or (count >= BATCH_SIZE or buffered >= MAX_BUFFERED or (time.time()-timestamp)*1000 >= LINGER_MS)):
            try:
                await writer.drain()
            except Exception:
                print(f"Exception draining to client {client_id}")
                return
            for topic, new_offset in updated_offsets.items():
                update_client_offset(client_id, topic, new_offset)
            updated_offsets = {}
            count = 0
            buffered = 0
            timestamp = time.time()
        # Nothing was sent, wait for new messages
        elif count==0:
            for topic, new_offset in updated_offsets.items():
                update_client_offset(client_id, topic, new_offset)
            updated_offsets = {}
            # Subscribe to all topic events
            tasks = [
                asyncio.create_task(topic_events[tp].wait())
                for tp in client_offsets.keys()
                if tp in topic_events
            ]
            # Cancel all the pending tasks
            _,pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED,timeout=1)
            for t in pending:
                t.cancel()


async def start_server():
    global reader_thread
    load_topics_log()
    start_threads()
    load_client_offsets()
    #Start reader thread
    for i in range(READER_THREAD_COUNT):
        reader_thread = ReaderThread(
            asyncio.get_event_loop(),
            request_q=reader_req_qs[i],
            client_queues=reader_res_qs,
            check_hash=MESSAGE_CHECKSUM_ENABLE
        )
        reader_thread.start()

    server = await asyncio.start_server(handle_client,HOST, PORT)
    for sock in server.sockets:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(start_server())
