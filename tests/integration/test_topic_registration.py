import pytest_asyncio
import pytest
import asyncio
from pylogstream_protocol.commands import RegisterTopicCommand, PullCommand
from pylogstream_protocol.response import MessageResponseHeader
from pylogstream_protocol.framer import PREFIX_SIZE, decode_length
from pylogstream_protocol.encoder import encode_command
from pylogstream_protocol.parser import parse_response
from pylogstream_protocol import response
from .utility import read_response, write_command

@pytest.mark.asyncio
async def test_topic_registration(broker, controller, client_controller_connection, client_broker_connection):
    topic_name = "test_topic"
    # Register the topic
    reader, writer = client_controller_connection
    register_cmd = RegisterTopicCommand(topic=topic_name)
    encoded_cmd = encode_command(register_cmd, checksum_enable=True)
    writer.write(encoded_cmd.header)
    writer.write(encoded_cmd.payload or b"")
    await writer.drain()

    # Wait for the controller to process the registration
    await asyncio.sleep(1)
    controller_topic_meta = await controller.manager.get_topic_metadata(topic_name)
    assert controller_topic_meta is not None, "Controller should have registered the topic"

    #Check topic is readable from broker
    reader, writer, client_id = client_broker_connection
    pull_cmd = PullCommand(topic=topic_name, offset=0, size=10)
    encoded_pull_cmd = encode_command(pull_cmd, checksum_enable=True)
    writer.write(encoded_pull_cmd.header)
    writer.write(encoded_pull_cmd.payload or b"")
    await writer.drain()
    data = await reader.readexactly(PREFIX_SIZE)
    length = int.from_bytes(data, byteorder='big')
    response_data = await reader.readexactly(length)
    resp  = parse_response(response_data)
    
    assert isinstance(resp, MessageResponseHeader), "Response should be a MessageResponseHeader"
    assert resp.topic == topic_name, "Response topic should match the requested topic"
    assert resp.length == 0, "Response size should be 0"

@pytest.mark.asyncio
async def test_topic_registration_duplicate(client_controller_connection):
    topic_name = "test_topic_duplicate"
    # Register the topic
    reader, writer = client_controller_connection
    register_cmd = RegisterTopicCommand(topic=topic_name)
    await write_command(writer, register_cmd)
    resp = await read_response(reader)

    assert isinstance(resp, response.TopicMetaDataResponse), "Response should be a TopicMetaDataResponse"
    assert resp.topic == topic_name, "Response topic should match the requested topic"

    # Register the same topic again
    await write_command(writer, register_cmd)
    new_resp = await read_response(reader)
    assert isinstance(new_resp, response.TopicMetaDataResponse), "Response should be a TopicMetaDataResponse"
    assert new_resp.topic == topic_name, "Response topic should match the requested topic"
    assert new_resp.version == resp.version, "Version should not change on duplicate registration"