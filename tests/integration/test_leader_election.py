import asyncio
import pytest
from .utility import read_response, write_command
from pylogstream_protocol import commands, response
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from pylogstream_broker.network.broker import Broker
    from pylogstream_controller.server import ControllerServer


@pytest.mark.asyncio
async def test_broker_leader_selection(broker_factory, client_broker_connection_factory):

    # Create extra two brokers
    brokers: list[Broker] = await broker_factory(3)

    broker_connection = await client_broker_connection_factory(brokers[0].config.broker.host, brokers[0].config.broker.port)

    topic_name = f"test_topic_{uuid4().hex[:8]}"

    await write_command(broker_connection[1], commands.RegisterTopicCommand(topic=topic_name))
    resp = await read_response(broker_connection[0])

    assert isinstance(resp, response.TopicMetaDataResponse), "Response should be a TopicMetaDataResponse"
    assert resp.topic == topic_name, "Response topic should match the requested topic"
    assert len(resp.replica_list) == 3, "There should be 3 replicas for the topic"

    leader_id = resp.leader_id
    leader_broker = next((b for b in brokers if b.config.replica.id == leader_id), None)
    assert leader_broker is not None, "Leader broker should be found in the list of brokers"
    # Kill the leader broker
    await leader_broker.close()

    if leader_id == brokers[0].config.replica.id:
        # Connect to the second broker if the first one was the leader
        broker_connection = await client_broker_connection_factory(brokers[1].config.broker.host, brokers[1].config.broker.port)
    # Wait for the controller to elect a new leader
    old_version = resp.version
    timeout_for_election = 20 # seconds
    start_time = asyncio.get_event_loop().time()
    while resp.version == old_version:
        elasped_time = asyncio.get_event_loop().time() - start_time
        if elasped_time > timeout_for_election:
            pytest.fail("Timeout waiting for new leader election")
        await asyncio.sleep(0.1)
        await write_command(broker_connection[1], commands.MetadataRequestCommand(topic=topic_name))
        resp = await read_response(broker_connection[0], timeout=max(0, timeout_for_election - elasped_time))
        assert isinstance(resp, response.TopicMetaDataResponse), "Response should be a TopicMetaDataResponse"
    assert resp.version > old_version, "Version should have incremented after leader election"
    assert resp.leader_id != leader_id, "New leader should be different from the old leader"
    assert resp.leader_id in [b.config.replica.id for b in brokers if b != leader_broker], "New leader should be one of the remaining brokers"
    
    from collections import Counter
    assert Counter(resp.replica_list) == Counter([b.config.replica.id for b in brokers if b != leader_broker]), "Replica list should contain all remaining brokers"

@pytest.mark.asyncio
async def test_controller_leader_election(controller_factory):
    controllers: list[ControllerServer] = await controller_factory(3)

    assert len([c for c in controllers if c.is_leader]) == 1, "There should be 1 leader"
    # Find the leader
    leader_controller = next((c for c in controllers if c.is_leader), None)
    assert leader_controller is not None, "There should be a leader controller"
    await leader_controller.close()


    # Remove old controllers
    controllers = [c for c in controllers if c != leader_controller]

    new_leader_controller = None
    timeout = 20 # seconds
    start_time = asyncio.get_event_loop().time()
    while new_leader_controller is None:
        if asyncio.get_event_loop().time() - start_time > timeout:
            pytest.fail("Timeout waiting for new leader election")
        await asyncio.sleep(0.1)
        new_leader_controller = next((c for c in controllers if c.is_leader), None)
    assert new_leader_controller is not None, "There should be a new leader controller"
    assert len([c for c in controllers if c.is_leader]) == 1, "There should be only one leader controller"