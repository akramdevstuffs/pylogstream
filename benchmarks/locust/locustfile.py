import gevent
from locust import User, task, between, events, constant
import time, json 
from gevent import monkey
from gevent.event import Event
from gevent.lock import Semaphore
import random
import string
import uuid

# Apply monkey patch before import client so it will patch all blocking socket send and receive calls
monkey.patch_all()

from client.client import Client

# Modify fds and socket limits to handle multiple sockets
import resource
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))

TOPIC = f"bench_{str(uuid.uuid4())}_" # Assign a unique topic prefix for each worker so TOPIC_0, TOPIC_1,... won't collide with another workers running in distributed
HOST = "localhost"
PORT = 1234


NUMBER_OF_PRODUCER = 2000
CONSUMER_PRODUCER_RATIO = 1
TARGET_MESSAGE_PER_SEC = 6000
INTERVAL = (NUMBER_OF_PRODUCER*(CONSUMER_PRODUCER_RATIO+1))/TARGET_MESSAGE_PER_SEC


consumers_ready = Event()
consumer_counter = 0
consumer_total = NUMBER_OF_PRODUCER*CONSUMER_PRODUCER_RATIO
# Shared start flag so producers wait until consumers ready
lock = Semaphore()

# Counter for producers
producer_counter = 0


class ConsumerUser(User):
    """
    Consumers subscribe and block waiting for messages.
    When messages arrive, record latency using the Locust event system.
    """
    weight = CONSUMER_PRODUCER_RATIO
    wait_time = constant(1000)

    def on_start(self):
        global consumer_counter, consumers_ready, consumer_total
        self.client_obj = Client(HOST, PORT, use_gevent=True)
        self.registered = False
        while not self.registered:
            try:
                self.client_obj.register()
                self.registered = True
            except Exception:
                print("Failed to register")
                time.sleep(1)

        with lock:
            self.producer_id = consumer_counter//CONSUMER_PRODUCER_RATIO
            self.client_obj.subscribe(f"{TOPIC}{self.producer_id}")
            self.client_obj.reset_offset_latest(f"{TOPIC}{self.producer_id}")
            print(f"[Consumer-{consumer_counter-1}] Subscribed to {TOPIC}{self.producer_id}")
            consumer_counter += 1
        gevent.spawn(self.consume_loop)

    def consume_loop(self):
        self.client_obj.reset_offset_latest(f"{TOPIC}{self.producer_id}")
        while True:
            try:
                resp = self.client_obj.consume()
                if not resp:
                    continue
                msg = resp.split(' ',1)[1]
                if not msg:
                    continue
                data = json.loads(msg)
                sent_ts = data.get("ts", None)
                if sent_ts:
                    latency = (time.time() - sent_ts) * 1000
                    events.request.fire(
                        request_type="consume",
                        name="message_latency",
                        response_time=latency,
                        response_length=len(msg),
                        exception=None,
                    )
            except Exception as e:
                events.request.fire(
                    request_type="consume",
                    name="message_latency",
                    response_time=0,
                    response_length=0,
                    exception=e,
                )

    @task
    def idle(self):
        time.sleep(1000)  # main Locust loop just stays alive


class ProducerUser(User):
    """
    Producers send messages at a steady rate once consumers are ready.
    """
    weight=1
    wait_time = constant(INTERVAL)

    def on_start(self):
        global producer_counter
        self.client_obj = Client(HOST, PORT, use_gevent=True)
        self.client_obj.register()
        with lock:
            self.producer_id = producer_counter
            producer_counter += 1 # Increase the producer count
        print(f"[Producer-{self.producer_id}] starting production...")

    @task
    def produce_message(self):
        payload = json.dumps({"ts": time.time(), "msg": ''.join(random.choices(string.ascii_letters, k=100))})
        start = time.time()
        try:
            self.client_obj.produce(f"{TOPIC}{self.producer_id}", payload)
            latency = (time.time() - start) * 1000
            events.request.fire(
                request_type="produce",
                name="produce_message",
                response_time=latency,
                response_length=len(payload),
                exception=None,
            )
        except Exception as e:
            events.request.fire(
                request_type="produce",
                name="produce_message",
                response_time=0,
                response_length=0,
                exception=e,
            )
