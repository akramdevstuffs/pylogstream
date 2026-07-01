from pylogstream_protocol.encoder import encode_command
import pytest
import pytest_asyncio
import socket
import asyncio
from pylogstream_broker.config import Config, BrokerConfig, LogConfig, ReplicaConfig, WriterConfig
from pylogstream_broker.network.broker import Broker
from pylogstream_controller.server import ControllerServer
from pylogstream_protocol import commands, framer, parser, response
from typing import AsyncGenerator


def get_free_port() -> int:
    """Get a free port from the OS."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 0))
        return s.getsockname()[1]


def make_broker_config(
        *,
        broker_id: str,
        broker_port: int,
        controller_port: int,
        data_dir: str
) -> Config:
    """Create an isolated broker config for testing."""

    return Config(
        broker=BrokerConfig(
            host="127.0.0.1",
            port=broker_port,
            controller_host="127.0.0.1",
            controller_port=controller_port,
            checksum_enable=True,
            writer_config=WriterConfig()
        ),
        log=LogConfig(log_dir=data_dir,
                    retension_ms= 604800000,        # 7 days
                    rollover_ms= 3600000,           # 1 hour
                    max_segment_size= 134217728,    # 128MB
                    init_segment_size= 16777216,    # 16MB
                    segment_size_inc= 16777216     # 16MB growth
        ),
        replica=ReplicaConfig(id=broker_id)
    )

@pytest_asyncio.fixture
async def controller() -> AsyncGenerator[ControllerServer, None]:
    """
    Start a fresh controller
    """
    free_port = get_free_port()
    controller = ControllerServer(host="127.0.0.1", port=free_port, replicas_per_topic=3, heartbeat_timeout=15.0)
    await controller.start()
    yield controller
    await controller.close()

@pytest_asyncio.fixture
async def broker(tmp_path, controller) -> AsyncGenerator[Broker, None]:
    """
    Start a fresh broker with a unique ID and port.
    """
    broker_id = f"broker-{get_free_port()}"
    broker_port = get_free_port()
    data_dir = tmp_path / f"broker_data_{broker_id}"
    data_dir.mkdir(parents=True, exist_ok=True)

    config = make_broker_config(
        broker_id=broker_id,
        broker_port=broker_port,
        controller_port=controller.port,
        data_dir=str(data_dir)
    )

    broker = Broker(config=config)

    task = asyncio.create_task(broker.start_server())

    # TODO: Remove flaky tests by implementing a proper wait for the broker to be ready
    await asyncio.sleep(1)  # Give the broker time to start
    # Make sure that controller is connected
    while not broker.controller_connected:
        await asyncio.sleep(0.1)

    yield broker

    await broker.close()
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

@pytest_asyncio.fixture
async def broker_factory(tmp_path, controller) -> AsyncGenerator:
    """
    Factory fixture to create multiple brokers with unique IDs and ports.
    """
    brokers = []
    tasks = []

    async def _create_broker(count:int) -> list[Broker]:
        for i in range(count):
            broker_id = f"broker-{get_free_port()}"
            broker_port = get_free_port()
            data_dir = tmp_path / f"broker_data_{broker_id}"
            data_dir.mkdir(parents=True, exist_ok=True)

            config = make_broker_config(
                broker_id=broker_id,
                broker_port=broker_port,
                controller_port=controller.port,
                data_dir=str(data_dir)
            )

            broker = Broker(config=config)
            task = asyncio.create_task(broker.start_server())
            brokers.append(broker)
            tasks.append(task)
        await asyncio.sleep(10)  # Give the brokers time to start
        # Make sure that all brokers are connected to the controller
        for broker in brokers:
            while not broker.controller_connected:
                await asyncio.sleep(0.1)
        return brokers

    yield _create_broker

    for broker in brokers:
        await broker.close()
    
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)

@pytest_asyncio.fixture
async def client_controller_connection(controller) -> AsyncGenerator[tuple[asyncio.StreamReader, asyncio.StreamWriter], None]:
    """
    Fixture to provide a function that creates a new client connection to the controller.
    """

    reader, writer = await asyncio.open_connection(controller.host, controller.port)
    yield reader, writer
    writer.close()
    await writer.wait_closed()

@pytest_asyncio.fixture
async def client_broker_connection(broker) -> AsyncGenerator[tuple[asyncio.StreamReader, asyncio.StreamWriter, str], None]:
    """
    Fixture to provide a function that creates a new client connection to the broker.
    """

    reader, writer = await asyncio.open_connection(broker.config.broker.host, broker.config.broker.port)
    register_cmd = commands.RegisterCommand()
    encoded_cmd = encode_command(register_cmd, checksum_enable=True)
    writer.write(encoded_cmd.header)
    writer.write(encoded_cmd.payload or b"")
    await writer.drain()
    
    data = await reader.readexactly(framer.PREFIX_SIZE)  # Read the response header
    length = framer.decode_length(data)  # Decode the length from the header
    response_data = await reader.readexactly(length)  # Read the response payload
    resp = parser.parse_response(response_data)
    assert isinstance(resp, response.ClientIDResponse), "Expected a ClientIDResponse from the broker"
    assert resp.client_id is not None, "Client ID should not be None"

    yield reader, writer, resp.client_id

    writer.close()
    await writer.wait_closed()