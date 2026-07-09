from pylogstream_protocol.response import NotLeaderControllerResponse
from collections import Counter
import pytest
import asyncio
from pylogstream_controller.metastore.manager import ConsistentHashRing, MetastoreManager
from pylogstream_controller.server import ControllerServer
from pylogstream_protocol.commands import (
    BrokerRegisterCommand,
    ISRChangeCommand,
    RegisterTopicCommand,
    MetadataRequestCommand,
    ControllerPingCommand
)
from pylogstream_protocol.response import (
    TopicMetaDataResponse,
    TopicMetaDataListHeaderResponse,
    TopicMetaDataListResponse,
    NotLeaderControllerResponse
)
from pylogstream_protocol.framer import PREFIX_SIZE, decode_length
from pylogstream_protocol.encoder import encode_command
from pylogstream_protocol.parser import parse_response, parse_metadata_list_payload

def test_consistent_hash_ring():
    ring = ConsistentHashRing(replicas=5)
    ring.add_node("node1")
    ring.add_node("node2")
    ring.add_node("node3")

    leader1 = ring.get_node("topic-a")
    leader2 = ring.get_node("topic-b")
    assert leader1 in {"node1", "node2", "node3"}
    assert leader2 in {"node1", "node2", "node3"}

    replicas = ring.get_replicas("topic-c", 3)
    assert len(replicas) == 3
    assert set(replicas) == {"node1", "node2", "node3"}

    ring.remove_node("node2")
    replicas_after = ring.get_replicas("topic-c", 3)
    assert len(replicas_after) == 2
    assert "node2" not in replicas_after

@pytest.mark.asyncio
async def test_metastore_manager_failover():
    manager = MetastoreManager(replicas_per_topic=3)
    await manager.register_broker("node1", "127.0.0.1", 9001)
    await manager.register_broker("node2", "127.0.0.1", 9002)
    await manager.register_broker("node3", "127.0.0.1", 9003)

    topic_meta = await manager.register_topic("my-topic")
    assert topic_meta.topic == "my-topic"
    assert topic_meta.version == 1
    assert len(topic_meta.replica_list) == 3
    
    leader = topic_meta.leader_id
    assert leader in {"node1", "node2", "node3"}

    # Deregister the leader node (simulate broker failure)
    updated = await manager.deregister_broker(leader)
    assert len(updated) == 1
    assert updated[0].topic == "my-topic"
    assert updated[0].leader_id != leader
    assert updated[0].version == 2
    assert leader not in updated[0].replica_list

@pytest.mark.asyncio
async def test_controller_server_integration():
    server = ControllerServer(host="127.0.0.1", port=19093, replicas_per_topic=2, heartbeat_timeout=2.0)
    await server.start()

    async def read_resp(reader):
        prefix = await reader.readexactly(PREFIX_SIZE)
        length = decode_length(prefix)
        data = await reader.readexactly(length)
        resp = parse_response(data)
        if isinstance(resp, TopicMetaDataListHeaderResponse):
            payload = await reader.readexactly(resp.payload_length)
            meta_list = parse_metadata_list_payload(payload)
            resp = TopicMetaDataListResponse(meta_list=meta_list)
        return resp

    async def read_until(reader, expected_type):
        for _ in range(10):
            resp = await read_resp(reader)
            if isinstance(resp, expected_type):
                return resp
        raise TimeoutError(f"Expected response of type {expected_type} not received")

    try:
        # Connect client 1 (Broker 1)
        reader1, writer1 = await asyncio.open_connection("127.0.0.1", 19093)
        reg_cmd1 = BrokerRegisterCommand(broker_id="b1", host="127.0.0.1", port=9001)
        frame1 = encode_command(reg_cmd1)
        writer1.write(frame1.header)
        await writer1.drain()

        # Read the initial metadata list response
        init_resp = await read_until(reader1, TopicMetaDataListResponse)
        assert isinstance(init_resp, TopicMetaDataListResponse)
        assert len(init_resp.meta_list) == 0

        # Register Broker 2
        reader2, writer2 = await asyncio.open_connection("127.0.0.1", 19093)
        reg_cmd2 = BrokerRegisterCommand(broker_id="b2", host="127.0.0.1", port=9002)
        frame2 = encode_command(reg_cmd2)
        writer2.write(frame2.header)
        await writer2.drain()

        # Read response for Broker 2
        await read_until(reader2, TopicMetaDataListResponse)

        # Register a topic using client 1
        reg_topic = RegisterTopicCommand(topic="test-topic")
        frame_topic = encode_command(reg_topic)
        writer1.write(frame_topic.header)
        await writer1.drain()

        # Read Register topic response from Broker 1
        topic_resp = await read_until(reader1, TopicMetaDataResponse)
        assert isinstance(topic_resp, TopicMetaDataResponse)
        assert topic_resp.topic == "test-topic"
        assert topic_resp.leader_id in {"b1", "b2"}

        # Now disconnect Broker 2 (simulate crash)
        writer2.close()
        await writer2.wait_closed()

        # Broker 1 should receive metadata update indicating the new state of the cluster
        async def wait_for_failover():
            for _ in range(10):
                broadcast_resp = await read_until(reader1, TopicMetaDataListResponse)
                assert isinstance(broadcast_resp, TopicMetaDataListResponse)
                if len(broadcast_resp.meta_list) == 1 and "b2" not in broadcast_resp.meta_list[0].replica_list:
                    return broadcast_resp
            raise AssertionError("b2 was not removed from replica list")
        
        broadcast_resp = await wait_for_failover()
        assert len(broadcast_resp.meta_list) == 1
        assert broadcast_resp.meta_list[0].topic == "test-topic"
        assert "b2" not in broadcast_resp.meta_list[0].replica_list

        writer1.close()
        await writer1.wait_closed()

    finally:
        await server.close()

async def write_and_read_response(writer, reader, command):
    frame = encode_command(command)
    writer.write(frame.header)
    await writer.drain()
    prefix = await reader.readexactly(PREFIX_SIZE)
    length = decode_length(prefix)
    data = await reader.readexactly(length)
    return parse_response(data)

@pytest.mark.asyncio
async def test_update_isr_command(controller_server: ControllerServer, controller_connection_factory):
    streams =[]
    for i in range(3):
        reader, writer = await controller_connection_factory()
        await write_and_read_response(writer, reader, BrokerRegisterCommand(broker_id=f"b{i+1}", host="127.0.0.1", port=9001 + i))
        #keeping the connections open to simulate brokers being alive
        streams.append((reader, writer))
    
    reader, writer = await controller_connection_factory()

    resp = await write_and_read_response(writer, reader, RegisterTopicCommand(topic="isr-test-topic"))
    assert isinstance(resp, TopicMetaDataResponse)
    meta = await controller_server.manager.get_topic_metadata("isr-test-topic")
    assert meta is not None
    isr_list = meta.isr_list.copy()
    frame = encode_command(ISRChangeCommand(topic="isr-test-topic", isr_list=isr_list))
    writer.write(frame.header)
    await writer.drain()
    await asyncio.sleep(0.1)  # Allow some time for the server to process the ISR change
    new_resp = await write_and_read_response(writer, reader, MetadataRequestCommand(topic="isr-test-topic"))

    new_meta = await controller_server.manager.get_topic_metadata("isr-test-topic")
    assert new_meta is not None

    assert isinstance(new_resp, TopicMetaDataResponse)
    assert Counter(new_meta.isr_list) == Counter(isr_list)
    assert new_resp.version == resp.version + 1  # Version should have incremented


@pytest.mark.asyncio
async def test_controller_not_leader_rejection():
    server = ControllerServer(host="127.0.0.1", port=19095, replicas_per_topic=2, heartbeat_timeout=2.0)
    await server.start()

    try:
        # Simulate losing leadership
        server.is_leader = False
        
        # Monkey patch get_leader to simulate another leader
        async def mock_get_leader():
            return {"controller_id": "other-leader", "host": "127.0.0.2", "port": 19096}
        server.get_leader = mock_get_leader
        
        # Connect to the server
        reader, writer = await asyncio.open_connection("127.0.0.1", 19095)
        
        # Send a ping command
        ping_cmd = ControllerPingCommand(broker_id="b1")
        frame = encode_command(ping_cmd)
        writer.write(frame.header)
        await writer.drain()
        
        # We should receive NotLeaderControllerResponse
        prefix = await reader.readexactly(PREFIX_SIZE)
        length = decode_length(prefix)
        data = await reader.readexactly(length)
        resp = parse_response(data)
        
        assert isinstance(resp, NotLeaderControllerResponse)
        assert resp.leader_id == "other-leader"
        assert resp.leader_host == "127.0.0.2"
        assert resp.leader_port == 19096
        
        # Connection should be closed by server
        try:
            data = await reader.read(100)
            assert len(data) == 0  # EOF
        except ConnectionResetError:
            pass # Also means connection is closed
        
    finally:
        await server.close()

@pytest.mark.asyncio
async def test_controller_broadcast_not_leader():
    server = ControllerServer(host="127.0.0.1", port=19096, replicas_per_topic=2, heartbeat_timeout=2.0)
    await server.start()

    try:
        # Connect to the server while it's leader
        reader, writer = await asyncio.open_connection("127.0.0.1", 19096)
        reg_cmd = BrokerRegisterCommand(broker_id="b1", host="127.0.0.1", port=9001)
        frame = encode_command(reg_cmd)
        writer.write(frame.header)
        await writer.drain()
        
        # read initial metadata
        prefix = await reader.readexactly(PREFIX_SIZE)
        length = decode_length(prefix)
        data = await reader.readexactly(length)
        resp = parse_response(data)
        # Should be TML header
        assert isinstance(resp, TopicMetaDataListHeaderResponse)
        payload = await reader.readexactly(resp.payload_length)

        # Broadcasted metadata is sent as well
        prefix = await reader.readexactly(PREFIX_SIZE)
        length = decode_length(prefix)
        data = await reader.readexactly(length)
        resp = parse_response(data)
        assert isinstance(resp, TopicMetaDataListHeaderResponse)
        payload = await reader.readexactly(resp.payload_length)
        
        # Now simulate losing leadership
        server.is_leader = False
        async def mock_get_leader():
            return {"controller_id": "other-leader", "host": "127.0.0.2", "port": 19097}
        server.get_leader = mock_get_leader
        
        # Broadcast
        await server._broadcast_not_leader()
        
        # Broker should receive NotLeaderControllerResponse
        prefix = await reader.readexactly(PREFIX_SIZE)
        length = decode_length(prefix)
        data = await reader.readexactly(length)
        resp = parse_response(data)
        
        assert isinstance(resp, NotLeaderControllerResponse)
        assert resp.leader_id == "other-leader"
        assert resp.leader_host == "127.0.0.2"
        assert resp.leader_port == 19097
        
        # Connection should be closed by server
        try:
            data = await reader.read(100)
            assert len(data) == 0  # EOF
        except ConnectionResetError:
            pass # Also means connection is closed
        
        # Broker should be removed from active_brokers
        assert len(server.active_brokers) == 0

    finally:
        await server.close()