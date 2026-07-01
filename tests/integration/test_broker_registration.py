import pytest
import asyncio
from pylogstream_broker.network.broker import Broker
from pylogstream_controller.server import ControllerServer
from typing import AsyncGenerator

@pytest.mark.asyncio
async def test_broker_registration(broker: Broker, controller: ControllerServer):
    """
    Test that a broker can register with the controller and receive metadata.
    """
    # Check that the broker is connected to the controller
    assert broker.controller_connected, "Broker should be connected to the controller"

    # Check that the controller has registered the broker
    registered_broker = await controller.manager.get_node(broker.config.replica.id)
    assert registered_broker is not None, "Controller should have registered the broker"
    assert registered_broker.host == broker.config.broker.host, "Registered broker host should match"
    assert registered_broker.port == broker.config.broker.port, "Registered broker port should match"

@pytest.mark.asyncio
async def test_broker_dead_heartbeat(broker: Broker, controller: ControllerServer):
    """
    Test that the controller detects a dead broker when it stops sending heartbeats.
    """

    assert broker.controller_connected, "Broker should be connected to the controller"
    registered_broker = await controller.manager.get_node(broker.config.replica.id)
    assert registered_broker is not None, "Controller should have registered the broker"
    assert registered_broker.host == broker.config.broker.host, "Registered broker host should match"
    assert registered_broker.port == broker.config.broker.port, "Registered broker port should match"

    await broker.close()

    # Wait for the controller to detect the dead broker
    await asyncio.sleep(controller.heartbeat_timeout + 5)

    # Check that the controller has deregistered the broker
    registered_broker = await controller.manager.get_node(broker.config.replica.id)
    assert registered_broker is None, "Controller should have deregistered the dead broker"

@pytest.mark.asyncio
async def test_broker_reconnect(broker: Broker, controller: ControllerServer):
    """
    Test that a broker can reconnect to the controller after being disconnected.
    """

    assert broker.controller_connected, "Broker should be connected to the controller"
    registered_broker = await controller.manager.get_node(broker.config.replica.id)
    assert registered_broker is not None, "Controller should have registered the broker"

    await broker.close()

    # Wait for the controller to detect the dead broker
    await asyncio.sleep(controller.heartbeat_timeout + 5)

    # Check that the controller has deregistered the broker
    registered_broker = await controller.manager.get_node(broker.config.replica.id)
    assert registered_broker is None, "Controller should have deregistered the dead broker"

    # Restart the broker
    asyncio.create_task(broker.start_server())

    # Wait for the broker to reconnect and register again
    await asyncio.sleep(5)

    # Check that the controller has registered the broker again
    registered_broker = await controller.manager.get_node(broker.config.replica.id)
    assert registered_broker is not None, "Controller should have re-registered the broker"

@pytest.mark.asyncio
async def test_broker_registration_with_multiple_brokers(broker_factory, controller: ControllerServer):
    """
    Test that multiple brokers can register with the controller and receive metadata.
    """

    brokers: list[Broker] = await broker_factory(count=10)

    for broker in brokers:
        assert broker.controller_connected, f"Broker {broker.config.replica.id} should be connected to the controller"
        registered_broker = await controller.manager.get_node(broker.config.replica.id)
        assert registered_broker is not None, f"Controller should have registered broker {broker.config.replica.id}"
        assert registered_broker.host == broker.config.broker.host, f"Registered broker {broker.config.replica.id} host should match"
        assert registered_broker.port == broker.config.broker.port, f"Registered broker {broker.config.replica.id} port should match"