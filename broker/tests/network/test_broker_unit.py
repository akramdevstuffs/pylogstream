import pytest
import asyncio
import uuid
import time
from unittest.mock import Mock, AsyncMock

from pylogstream_broker.config import Config, BrokerConfig, LogConfig, ReplicaConfig, WriterConfig
from pylogstream_broker.network.broker import Broker
from pylogstream_protocol import commands as cmd
from pylogstream_protocol import response as resp
from pylogstream_protocol.encoder import encode_command, encode_response
from pylogstream_protocol.parser import parse_response

class MockWriter:
    def __init__(self):
        self.submitted = []
    def submit(self, req):
        self.submitted.append(req)
        # Mock successful immediate response
        if req.callback:
            req.callback.set_result(100)  # mock offset 100
    async def close(self):
        pass

class MockReplica:
    def __init__(self):
        self.hw = 50
    def get_high_watermark(self):
        return self.hw
    async def wait_for_hw(self, offset):
        pass

class MockReplicaManager:
    def __init__(self):
        self.broker_id = "test-replica-1"
        self.topics = {}
        self.leaders = {}
        self.on_isr_change = None

    def get_leader(self, topic):
        if topic not in self.leaders:
            raise Exception("Topic doesn't exists")
        return self.leaders[topic]

    def handle(self, replica_cmd):
        return resp.ReplicaFetchBytesResponse(
            topic=replica_cmd.topic,
            log_end_offset=100,
            high_watermark=90,
            payload_length=5,
            payload=b"bytes"
        )

    async def close_all(self):
        pass

class MockControllerConnection:
    def __init__(self):
        self.closed = False

    async def register_topic(self, topic):
        return resp.TopicMetaDataResponse(
            topic=topic,
            leader_id="test-replica-1",
            leader_addr="127.0.0.1",
            leader_port=9092,
            replica_list=["test-replica-1"],
            version=1
        )

    async def request_metadata(self, topic):
        return resp.TopicMetaDataResponse(
            topic=topic,
            leader_id="test-replica-1",
            leader_addr="127.0.0.1",
            leader_port=9092,
            replica_list=["test-replica-1"],
            version=1
        )

    async def close(self):
        self.closed = True

@pytest.fixture
def broker_config():
    return Config(
        broker=BrokerConfig(
            host="127.0.0.1",
            port=9999,
            controller_list=[],
            checksum_enable=True,
            writer_config=WriterConfig()
        ),
        log=LogConfig(
            log_dir="/tmp/dummy",
            retension_ms=1000,
            rollover_ms=1000,
            max_segment_size=1000,
            init_segment_size=1000,
            segment_size_inc=1000
        ),
        replica=ReplicaConfig(id="test-replica-1")
    )

@pytest.fixture
def mock_dependencies():
    return {
        "log_manager": Mock(),
        "writer": MockWriter(),
        "replica_manager": MockReplicaManager(),
    }

@pytest.fixture
def unit_broker(broker_config, mock_dependencies):
    b = Broker(
        config=broker_config,
        log_manager=mock_dependencies["log_manager"],
        writer=mock_dependencies["writer"],
        replica_manager=mock_dependencies["replica_manager"]
    )
    b.running = True
    return b

@pytest.mark.asyncio
async def test_handle_client_registration(unit_broker):
    # Mock StreamReader and StreamWriter
    reader = AsyncMock()
    writer = AsyncMock()
    writer.get_extra_info = Mock(return_value=None)

    # Create REG command
    reg_cmd = cmd.RegisterCommand()
    frame = encode_command(reg_cmd)

    # Mock reader to return the command frame and then exit loop
    reader.readexactly.side_effect = [
        frame.header[:4],  # prefix size
        frame.header[4:] + (frame.payload or b"")
    ]

    # Run the handler but cancel/exit after the first command
    task = asyncio.create_task(unit_broker.handle_client(reader, writer))
    await asyncio.sleep(0.05)
    unit_broker.running = False
    try:
        await asyncio.wait_for(task, timeout=1.0)
    except Exception:
        pass

    # Verify writer received ClientIDResponse
    assert writer.write.called
    written_bytes = b"".join(args[0] for args, _ in writer.write.call_args_list)
    assert b"CID" in written_bytes

@pytest.mark.asyncio
async def test_handle_client_ping(unit_broker):
    reader = AsyncMock()
    writer = AsyncMock()
    writer.get_extra_info = Mock(return_value=None)

    # Pre-register client
    ping_cmd = cmd.PingCommand()
    ping_cmd.header = None
    frame = encode_command(ping_cmd)

    # We need to register first, then ping
    reg_cmd = cmd.RegisterCommand()
    reg_frame = encode_command(reg_cmd)

    reader.readexactly.side_effect = [
        reg_frame.header[:4],
        reg_frame.header[4:] + (reg_frame.payload or b""),
        frame.header[:4],
        frame.header[4:] + (frame.payload or b"")
    ]

    task = asyncio.create_task(unit_broker.handle_client(reader, writer))
    await asyncio.sleep(0.05)
    unit_broker.running = False
    try:
        await asyncio.wait_for(task, timeout=1.0)
    except Exception:
        pass

    written_bytes = b"".join(args[0] for args, _ in writer.write.call_args_list)
    assert b"PNG" in written_bytes

@pytest.mark.asyncio
async def test_handle_client_publish_and_pull(unit_broker, mock_dependencies):
    replica_mgr = mock_dependencies["replica_manager"]
    replica_mgr.leaders["test-topic"] = MockReplica()

    reader = AsyncMock()
    writer = AsyncMock()
    writer.get_extra_info = Mock(return_value=None)

    # Register
    reg_cmd = cmd.RegisterCommand()
    reg_frame = encode_command(reg_cmd)

    # Publish
    pub_cmd = cmd.PublishCommand(topic="test-topic", payload=b"hello", acks=1, contains_checksum=True)
    pub_frame = encode_command(pub_cmd)

    # Pull
    pull_cmd = cmd.PullCommand(topic="test-topic", offset=0, size=50)
    pull_frame = encode_command(pull_cmd)

    reader.readexactly.side_effect = [
        reg_frame.header[:4],
        reg_frame.header[4:] + (reg_frame.payload or b""),
        pub_frame.header[:4],
        pub_frame.header[4:] + (pub_frame.payload or b""),
        pull_frame.header[:4],
        pull_frame.header[4:] + (pull_frame.payload or b"")
    ]

    # Mock log manager read_bytes response
    mock_dependencies["log_manager"].read_bytes.return_value = Mock(data=b"hello")

    task = asyncio.create_task(unit_broker.handle_client(reader, writer))
    await asyncio.sleep(0.05)
    unit_broker.running = False
    try:
        await asyncio.wait_for(task, timeout=1.0)
    except Exception:
        pass

    written_bytes = b"".join(args[0] for args, _ in writer.write.call_args_list)
    assert b"ACK" in written_bytes
    assert b"MSG" in written_bytes

@pytest.mark.asyncio
async def test_handle_client_register_topic_with_controller(unit_broker):
    unit_broker.controller = MockControllerConnection()

    reader = AsyncMock()
    writer = AsyncMock()
    writer.get_extra_info = Mock(return_value=None)

    # Register topic command
    rgt_cmd = cmd.RegisterTopicCommand(topic="new-topic")
    rgt_frame = encode_command(rgt_cmd)

    reader.readexactly.side_effect = [
        rgt_frame.header[:4],
        rgt_frame.header[4:] + (rgt_frame.payload or b"")
    ]

    task = asyncio.create_task(unit_broker.handle_client(reader, writer))
    await asyncio.sleep(0.05)
    unit_broker.running = False
    try:
        await asyncio.wait_for(task, timeout=1.0)
    except Exception:
        pass

    written_bytes = b"".join(args[0] for args, _ in writer.write.call_args_list)
    assert b"TMD" in written_bytes
