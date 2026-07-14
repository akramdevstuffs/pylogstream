import pytest
import pytest_asyncio
import asyncio
from pylogstream_broker.network.broker import Broker 
from pylogstream_protocol.response import ErrorResponse, TopicMetaDataResponse
from pylogstream_protocol.commands import (
    Command,
    CommitOffsetCommand,
    RegisterCommand,
    PingCommand,
    PublishCommand,
    PullCommand,
    FetchOffsetCommand,
    ReplicaFetchCommand
)
from pylogstream_protocol.response import (
    Response, ClientIDResponse, PingResponse, PubAckResponse,
    MessageResponseHeader,
    MessageResponse,
    FetchOffsetResponse,
    OffsetAckResponse,
    ReplicaFetchBytesResponse,
    ReplicaFetchHeaderResponse
)

from pylogstream_protocol.framer import PREFIX_SIZE, decode_length
from pylogstream_protocol.encoder import encode_command
from pylogstream_protocol.parser import parse_response, parse_records, decode_record
from pylogstream_broker.config import Config, BrokerConfig, WriterConfig, LogConfig, ReplicaConfig, load_config


@pytest.mark.asyncio
async def test_broker_startup(broker):
    assert broker.running
    assert broker._Broker__replica_manager.get_leader("test-topic").id == 'replica1'

@pytest.mark.asyncio
async def test_broker_shutdown(broker):
    assert broker.running
    

@pytest_asyncio.fixture
async def client(broker):
    exp = 0.1
    while True:
        try:
            reader, writer = await asyncio.open_connection(broker.config.broker.host, broker.config.broker.port)
            break
        except Exception:
            # Exponential backoff for connection attempts
            exp *= 2
            await asyncio.sleep(exp)
            if exp > 5:
                raise Exception("Failed to connect to broker after multiple attempts")
    yield (reader, writer)
    writer.close()
    await writer.wait_closed()
@pytest.mark.asyncio
async def test_broker_handle_client(client):
    reader, writer = client
    assert reader is not None
    assert writer is not None
    cmd = RegisterCommand()
    frame = encode_command(cmd)
    if frame.header is not None:
        writer.write(frame.header)
    if frame.payload is not None:
        writer.write(frame.payload)
    await writer.drain()

async def send_command(writer: asyncio.StreamWriter, cmd: Command):
    frame = encode_command(cmd)
    if frame.header is not None:
        writer.write(frame.header)
    if frame.payload is not None:
        writer.write(frame.payload)
    await writer.drain()

async def read_response(reader: asyncio.StreamReader) -> Response:
    # Read the prefix to get the header length
    prefix = await reader.readexactly(PREFIX_SIZE)
    header_length = decode_length(prefix)

    # Read the header
    header = await reader.readexactly(header_length)

    resp = parse_response(header)
    if resp is None:
        raise Exception("Failed to parse response header")
    if isinstance(resp, MessageResponseHeader):
        payload = await reader.readexactly(resp.length)
        resp = MessageResponse(
            topic=resp.topic,
            payload=payload,
        )
    
    if isinstance(resp, ReplicaFetchHeaderResponse):
        payload = await reader.readexactly(resp.payload_length)
        resp = ReplicaFetchBytesResponse(
            topic=resp.topic,
            log_end_offset=resp.log_end_offset,
            high_watermark=resp.high_watermark,
            payload_length=resp.payload_length,
            payload=payload
        )

    return resp

async def push_message(writer: asyncio.StreamWriter, reader: asyncio.StreamReader,topic: str, payload: str):
    cmd = PublishCommand(topic=topic, payload=payload.encode(), acks=1, contains_checksum=True)
    await send_command(writer, cmd)
    resp = await read_response(reader)
    assert isinstance(resp, PubAckResponse)
    assert resp.acks == 1


@pytest_asyncio.fixture
async def registered_client(client):
    reader, writer = client
    cmd = RegisterCommand()
    await send_command(writer, cmd)
    resp = await read_response(reader)
    assert isinstance(resp, ClientIDResponse)
    assert resp.client_id is not None
    return (reader, writer)

@pytest.mark.asyncio
async def test_broker_client_reg(registered_client):
    assert registered_client is not None

@pytest.mark.asyncio
async def test_broker_client_ping(registered_client):
    reader, writer = registered_client
    cmd = PingCommand()
    await send_command(writer, cmd)
    resp = await read_response(reader)
    assert resp is not None
    assert isinstance(resp, PingResponse)

@pytest.mark.asyncio
async def test_broker_client_publish_pull(registered_client):
    reader, writer = registered_client
    topic = "test-topic"
    payload = "Hello, World!"
    cmd = PublishCommand(topic=topic, payload=payload.encode(), acks=1, contains_checksum=True)
    await send_command(writer, cmd)
    resp = await read_response(reader)
    assert resp is not None
    assert isinstance(resp, PubAckResponse)
    cmd = PullCommand(topic=topic, offset=0, size=1024)
    await send_command(writer, cmd)
    resp = await read_response(reader)
    assert resp is not None
    assert isinstance(resp, MessageResponse)
    assert resp.topic == topic
    for record in parse_records(resp.payload):
        hash, msg = decode_record(record)
        assert msg == payload
    
@pytest.mark.asyncio
async def test_broker_client_fetch_offset(registered_client):
    reader, writer = registered_client
    topic = "test-topic"
    cmd = FetchOffsetCommand(topic=topic)
    await send_command(writer, cmd)
    resp = await read_response(reader)
    assert resp is not None
    assert isinstance(resp, FetchOffsetResponse)
    assert resp.topic == topic
    assert resp.offset >= 0

@pytest.mark.asyncio
async def test_broker_client_invalid_command(registered_client):
    reader, writer = registered_client
    writer.write(b"INVALID_COMMAND")
    await writer.drain()
    # The broker should close the connection after sending an error response
    try:
        resp = await read_response(reader)
        assert resp is not None
        assert isinstance(resp, ErrorResponse)
        assert resp.code == 400 or resp.code == 408
    except Exception as e:
        # Connection should be closed by the broker
        assert isinstance(e, asyncio.IncompleteReadError)

@pytest.mark.asyncio
async def test_broker_client_timeout(registered_client):
    reader, writer = registered_client
    # Do not send any command and wait for the broker to timeout the connection
    try:
        resp = await read_response(reader)
        assert resp is not None
        assert isinstance(resp, ErrorResponse)
        assert resp.code == 408
    except Exception as e:
        # Connection should be closed by the broker
        assert isinstance(e, asyncio.IncompleteReadError)

@pytest.mark.asyncio
async def test_broker_invalid_command(registered_client):
    reader, writer = registered_client
    cmd = b"XY"
    writer.write(len(cmd).to_bytes(4, "big") + cmd)
    await writer.drain()
    # The broker should respond with an error and close the connection
    try:
        resp = await read_response(reader)
        assert resp is not None
        assert isinstance(resp, ErrorResponse)
        assert resp.code == 400 or resp.code == 408
        assert resp.message == "Unknown command"
    except Exception as e:
        # Connection should be closed by the broker
        assert isinstance(e, asyncio.IncompleteReadError)

@pytest.mark.asyncio
async def test_broker_client_checksum_failed(registered_client):
    reader, writer = registered_client
    topic = "test-topic"
    payload = "Hello, World!"
    # Send a publish command with invalid checksum
    cmd = PublishCommand(topic=topic, payload=payload.encode(), acks=1, contains_checksum=True)
    frame = encode_command(cmd)
    if frame.header is not None:
        # Corrupt the checksum in the header
        corrupted_header = frame.header.replace(b"1 ", b"1 12345 ")
        writer.write(corrupted_header)
    if frame.payload is not None:
        writer.write(frame.payload)
    await writer.drain()
    # The broker should respond with a checksum error and close the connection
    try:
        resp = await read_response(reader)
        assert resp is not None
        assert isinstance(resp, ErrorResponse)
        assert resp.code == 400 or resp.code == 408
        assert resp.message == "Checksum failed"
    except Exception as e:
        # Connection should be closed by the broker
        assert isinstance(e, asyncio.IncompleteReadError)

@pytest.mark.asyncio
async def test_broker_send_by_file(registered_client):
    reader, writer = registered_client
    topic = "test-topic"
    payload = "Hello, File!"
    pub_cmd = PublishCommand(topic=topic, payload=payload.encode(), acks=1, contains_checksum=True)
    await send_command(writer, pub_cmd)
    ack_resp = await read_response(reader)
    assert ack_resp is not None
    assert isinstance(ack_resp, PubAckResponse)
    # For requested max_size > 1024 it will send the response by file
    pull_cmd = PullCommand(topic=topic, offset=0, size=2048)
    await send_command(writer, pull_cmd)
    resp = await read_response(reader)
    assert resp is not None
    assert isinstance(resp, MessageResponse)
    assert resp.topic == topic
    for record in parse_records(resp.payload):
        hash, msg = decode_record(record)
        assert msg == payload

@pytest.mark.asyncio
async def test_broker_offset_commit(registered_client):
    reader, writer = registered_client
    topic = "test-topic"
    payload = "Hello, File!"
    pub_cmd = PublishCommand(topic=topic, payload=payload.encode(), acks=1, contains_checksum=True)
    await send_command(writer, pub_cmd)
    ack_resp = await read_response(reader)
    assert ack_resp is not None
    assert isinstance(ack_resp, PubAckResponse)
    # Commit offset 5 for the topic
    cmt_cmd = CommitOffsetCommand(topic=topic, offset=5, acks=1)
    await send_command(writer, cmt_cmd)
    cmt_resp = await read_response(reader)
    assert cmt_resp is not None
    assert isinstance(cmt_resp, OffsetAckResponse)
    assert cmt_resp.offset == 5

@pytest.mark.asyncio
async def test_broker_offset_commit_no_ack(registered_client):
    reader, writer = registered_client
    topic = "test-topic"
    payload = "Hello, File!"
    pub_cmd = PublishCommand(topic=topic, payload=payload.encode(), acks=1, contains_checksum=True)
    await send_command(writer, pub_cmd)
    ack_resp = await read_response(reader)
    assert ack_resp is not None
    assert isinstance(ack_resp, PubAckResponse)
    # Commit offset 10 for the topic with acks=0 (no ack expected)
    cmt_cmd = CommitOffsetCommand(topic=topic, offset=10, acks=0)
    await send_command(writer, cmt_cmd)
    # Since acks=0, we should not receive any response from the broker
    try:
        resp = await asyncio.wait_for(read_response(reader), timeout=2.0)
        assert False, "Expected no response from broker when acks=0"
    except TimeoutError:
        pass  # Expected

@pytest.mark.asyncio
async def test_broker_invalid_topic_pub_pul(registered_client):
    reader,writer = registered_client
    topic = "nonexisting-topic"
    payload = "Something"
    pub_cmd = PublishCommand(topic=topic, payload=payload.encode(), acks=1, contains_checksum=True)
    await send_command(writer, pub_cmd)
    try:        
        resp = await read_response(reader)
        assert isinstance(resp, ErrorResponse)
        assert resp.code == 400
        assert resp.message == "Topic not found or not leader"
    except Exception as e:
        assert isinstance(e, asyncio.IncompleteReadError)
    # Try to pull from the non-existing topic
    pull_cmd = PullCommand(topic=topic, offset=0, size=1024)
    await send_command(writer, pull_cmd)
    try:
        resp = await read_response(reader)
        assert isinstance(resp, ErrorResponse)
        assert resp.code == 400
        assert resp.message == "Topic not found or not leader"
    except Exception as e:
        assert isinstance(e, asyncio.IncompleteReadError)

@pytest.mark.asyncio
async def test_broker_pull_invalid_offset(registered_client):
    reader,writer = registered_client
    await push_message(writer, reader, topic="test-topic", payload="Test Message")
    cmd = PullCommand(topic="test-topic", offset=1000, size=1024)
    await send_command(writer, cmd)

    resp = await read_response(reader)
    assert isinstance(resp, ErrorResponse)
    assert resp.code == 400

@pytest.mark.asyncio
async def test_broker_pull_end_offset(registered_client):
    reader,writer = registered_client
    msg = "Test Message"
    await push_message(writer, reader, topic="test-topic", payload=msg)
    cmd = PullCommand(topic="test-topic", offset=0, size=1024)
    await send_command(writer, cmd)

    resp = await read_response(reader)
    assert isinstance(resp, MessageResponse)
    assert resp.topic == "test-topic"
    new_offset = len(resp.payload)

    cmd = PullCommand(topic="test-topic", offset=new_offset, size=1024)
    await send_command(writer, cmd)
    resp = await read_response(reader)
    assert isinstance(resp, MessageResponse)
    assert resp.topic == "test-topic"
    assert len(resp.payload) == 0

@pytest.mark.asyncio
async def test_broker_log_rollover(registered_client):
    reader,writer = registered_client
    topic = "test-topic"
    # Send messages until we trigger a log rollover
    msg = "A" * 1024 * 1024  # 1 MB message
    for i in range(12):  # Send 12 messages to exceed the 10 MB segment size
        await push_message(writer, reader, topic=topic, payload=f"{msg}_{i}")
    
    # Now pull the messages back and verify they are correct
    cmd = PullCommand(topic=topic, offset=0, size=20*1024*1024)  
    await send_command(writer, cmd)
    resp = await read_response(reader)
    assert isinstance(resp, MessageResponse)
    assert resp.topic == topic
    messages = list(parse_records(resp.payload))
    # Because of rollover, we should only get the first 9 messages
    assert len(messages) == 9
    for i, record in enumerate(messages):
        hash, m = decode_record(record)
        assert m == f"{msg}_{i}"
    # pull rest messages
    cmd = PullCommand(topic=topic, offset=len(resp.payload), size=20*1024*1024)
    await send_command(writer, cmd)
    resp = await read_response(reader)
    assert isinstance(resp, MessageResponse)
    assert resp.topic == topic
    messages = list(parse_records(resp.payload))
    assert len(messages) == 3
    for i, record in enumerate(messages):
        hash, m = decode_record(record)
        assert m == f"{msg}_{i+9}"
    

# ---------------------------
# Replica Tests
# ---------------------------

@pytest.mark.asyncio
async def test_broker_replica_request(broker, monkeypatch):
    topic = "test-topic"
    # Mock the replica manager to return a fixed response for testing
    def mock_handle(command):
        assert command.topic == topic
        return ReplicaFetchBytesResponse(
            topic=topic,
            log_end_offset=100,
            high_watermark=90,
            payload_length=len(b"Replica Response"),
            payload=b"Replica Response",
        )
    monkeypatch.setattr(broker._Broker__replica_manager, "handle", mock_handle)  # type: ignore
    cmd = ReplicaFetchCommand(topic=topic, offset=0, size=1024, replica_id="replica2")
    reader, writer = await asyncio.open_connection(broker.config.broker.host, broker.config.broker.port)
    await send_command(writer, cmd)
    resp = await read_response(reader)
    assert isinstance(resp, ReplicaFetchBytesResponse)
    assert resp.topic == topic
    assert resp.payload == b"Replica Response"