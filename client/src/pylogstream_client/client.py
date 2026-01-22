import socket
import zlib
import os
import threading
import queue
import time

_gevent = None # Lazy import for gevent

class Client:
    checksum_enabled:bool = True
    outgoing_buffer_capacity = 1000
    ping_interval = 30 # in seconds
    def __init__(self, HOST:str, PORT: int, use_gevent:bool=False):
        global _gevent
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.HOST = HOST
        self.PORT = PORT
        self.conn.connect((HOST, PORT))
        self.send_queue = queue.Queue(self.outgoing_buffer_capacity) # Using queue so message won't get mixed between threads
        self.alive = True
        # Start threads
        #Trying to use gevent
        if use_gevent:
            try:
                import gevent
                from gevent import monkey
                monkey.patch_all()
                _gevent = gevent
                self.writer_thread = _gevent.spawn(self._writer_loop)
                self.ping_thread = _gevent.spawn(self._ping_loop)
            except ImportError:
                # Import failed, use threads
                use_gevent = False
                print("gevent is not installed")
        if not use_gevent:
            self.writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
            self.writer_thread.start()
            self.ping_thread = threading.Thread(target=self._ping_loop, daemon=True)
            self.ping_thread.start()

    def _writer_loop(self):
        """Continously takes message from queue and push it to the socket"""
        while self.alive:
            msg = self.send_queue.get()
            if msg is None:
                break
            try:
                self.sendall(msg)
            except Exception as e:
                print("Send failed:",e)
                self.alive = False
        self.conn.close()

    def _ping_loop(self):
        """Enqueue heartbeat message in the sent queue"""
        while self.alive:
            time.sleep(self.ping_interval)
            framed = self._frame_message('PNG','')
            self.send_queue.put(framed)


    def register(self) -> str:
        """Register new client to the server and returns id"""
        framed =self._frame_message('REG','')
        self.send_queue.put(framed) # Register
        _ = self.conn.recv(4) # id_length
        id_len = int.from_bytes(_, 'big')
        _ = self.conn.recv(id_len)
        id_str = _.decode()
        framed = self._frame_message('CID', id_str)
        self.send_queue.put(framed)
        return id_str
    
    def close(self):
        self.alive = False
        self.send_queue.put(None)
        if _gevent:
            self.ping_thread.kill()
            self.writer_thread.kill()
        else:
            self.ping_thread.close()
            self.writer_thread.close()
        self.conn.close()

    def login(self, id:str):
        """Takes id and login previous clients"""
        framed = self._frame_message('CID',id)
        self.send_queue.put(framed)

    def reset_offset_oldest(self, topic):
        """Sets client topic offset to 0"""
        framed = self._frame_message('SET '+topic, '0')
        self.send_queue.put(framed)

    def reset_offset_latest(self, topic):
        """Sets client topic offset to latest offset"""
        framed = self._frame_message('SET '+topic, '-1')
        self.send_queue.put(framed)


    def subscribe(self, topic:str):
        """Subscribes to the topic"""
        framed = self._frame_message('SUB',topic)
        self.send_queue.put(framed)

    def produce(self, topic:str, message:str):
        """Produces a message in the topic channel"""
        framed = self._frame_message('PUB '+topic, message, add_checksum=self.checksum_enabled)
        self.send_queue.put(framed)

    def recvall(self, size: int)->bytes|None:
        data = b''
        while len(data) < size:
            packet = self.conn.recv(size - len(data))
            if not packet or packet==b'':
                return None
            data += packet
        return data

    def sendall(self, data:bytes):
        if os.name == "posix" or os.name=="win32":
            self.conn.sendall(data)
        else:
            total_sent = 0
            while total_sent < len(data):
                sent = conn.send(data[total_sent:])
                if sent == 0:
                    self.alive = False
                    raise RuntimeError("socket connection broken")
                total_sent += sent

    def _frame_message(self, cmd,payload, add_checksum=False) -> bytes:
        """Frame message in the format: [length][payload][checksum] and also checksum if required"""
        hash = b''
        if add_checksum:
            hash = zlib.crc32(payload.encode()).to_bytes(4,'big')
        msg_bytes = f'{cmd} {payload}'.encode()
        return len(msg_bytes).to_bytes(4,'big')+msg_bytes+hash


    def consume(self) -> str|None:
        """Blocks until a message arrives and returns it."""
        len_bytes = self.recvall(4)
        if not len_bytes or len_bytes==b'':
            #Connection closed return
            return None
        msg_bytes = self.recvall(int.from_bytes(len_bytes, 'big'))
        if not msg_bytes:
            return None
        return msg_bytes.decode()
