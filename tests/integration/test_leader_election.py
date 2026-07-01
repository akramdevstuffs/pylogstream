import asyncio
import pytest
from .utility import read_response, write_command
from pylogstream_protocol import commands, response
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pylogstream_broker.network.broker import Broker


@pytest.mark.asyncio
async def test_leader_election(broker_factory, client_controller_connection):

    brokers: list[Broker] = await broker_factory(3)

    await write_command(client_controller_connection[1], commands.RegisterTopicCommand(topic="test_topic"))
    resp = await read_response(client_controller_connection[0])

    assert isinstance(resp, response.TopicMetaDataResponse), "Response should be a TopicMetaDataResponse"

    timeout = 5 # seconds
    start_time = asyncio.get_event_loop().time()

    while len(resp.replica_list) == 0:
        if asyncio.get_event_loop().time() - start_time > timeout:
            pytest.fail("Timeout waiting for topic assignment")
        await asyncio.sleep(0.1)
        await write_command(client_controller_connection[1], commands.MetadataRequestCommand(topic="test_topic"))
        resp = await read_response(client_controller_connection[0])
        assert isinstance(resp, response.TopicMetaDataResponse), "Response should be a TopicMetaDataResponse"

    leader_id = resp.leader_id
    leader_broker = next((b for b in brokers if b.config.replica.id == leader_id), None)
    assert leader_broker is not None, "Leader broker should be found in the list of brokers"
    # Kill the leader broker
    await leader_broker.close()

    # Wait for the controller to elect a new leader
    old_version = resp.version
    timeout_for_election = 20 # seconds
    while resp.version == old_version:
        if asyncio.get_event_loop().time() - start_time > timeout_for_election:
            pytest.fail("Timeout waiting for new leader election")
        await asyncio.sleep(0.1)
        await write_command(client_controller_connection[1], commands.MetadataRequestCommand(topic="test_topic"))
        resp = await read_response(client_controller_connection[0])
        assert isinstance(resp, response.TopicMetaDataResponse), "Response should be a TopicMetaDataResponse"
    assert resp.version > old_version, "Version should have incremented after leader election"
    assert resp.leader_id != leader_id, "New leader should be different from the old leader"
    assert resp.leader_id in [b.config.replica.id for b in brokers if b != leader_broker], "New leader should be one of the remaining brokers"
    
    from collections import Counter
    assert Counter(resp.replica_list) == Counter([b.config.replica.id for b in brokers if b != leader_broker]), "Replica list should contain all remaining brokers"