import pytest
import asyncio
from pylogstream_controller.metastore.manager import ConsistentHashRing, MetastoreManager
from pylogstream_controller.server import ControllerServer
from pylogstream_protocol.commands import (
    BrokerRegisterCommand,
    RegisterTopicCommand,
    MetadataRequestCommand,
    ControllerPingCommand
)
from pylogstream_protocol.response import (
    TopicMetaDataResponse,
    TopicMetaDataListHeaderResponse,
    TopicMetaDataListResponse,
    ControllerPingResponse,
    ErrorResponse
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
        assert topic_resp.topic == "test-topic"
        assert topic_resp.leader_id in {"b1", "b2"}

        # Now disconnect Broker 2 (simulate crash)
        writer2.close()
        await writer2.wait_closed()

        # Broker 1 should receive metadata update indicating the new state of the cluster
        async def wait_for_failover():
            for _ in range(10):
                broadcast_resp = await read_until(reader1, TopicMetaDataListResponse)
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

