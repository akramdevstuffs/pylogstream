import pytest_asyncio
import asyncio
from pylogstream_controller.server import ControllerServer
from pylogstream_controller.config import ControllerConfig

def make_controller_config(
        *,
        host="127.0.0.1",
        port=19093,
        ):
    return ControllerConfig(host=host, port=port)


@pytest_asyncio.fixture
async def controller_server():
    config = make_controller_config()
    server = ControllerServer(host=config.host, port=config.port)
    await server.start()
    yield server
    await server.close()

@pytest_asyncio.fixture
async def controller_connection(controller_server):
    reader, writer = await asyncio.open_connection(controller_server.host, controller_server.port)
    yield reader, writer
    writer.close()
    await writer.wait_closed()

@pytest_asyncio.fixture
async def controller_connection_factory(controller_server):
    async def _create_connection():
        reader, writer = await asyncio.open_connection(controller_server.host, controller_server.port)
        return reader, writer
    yield _create_connection