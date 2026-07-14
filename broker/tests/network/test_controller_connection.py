import pytest
import asyncio
from unittest.mock import Mock, AsyncMock

from pylogstream_broker.config import BrokerConfig, ControllerAddress
from pylogstream_broker.network.controller import ControllerConnection
from pylogstream_protocol import response as resp, commands as cmd, framer
from pylogstream_protocol.encoder import encode_command, encode_response
from pylogstream_protocol.parser import parse_command

class DummyReplicaManager:
    def __init__(self):
        self.broker_id = "broker-test-1"
        self.applied_list = None
        self.applied_meta = None
        self.on_isr_change = None

    async def apply_metadata_list(self, metadata_list):
        self.applied_list = metadata_list

    async def apply_metadata(self, metadata):
        self.applied_meta = metadata

@pytest.mark.asyncio
async def test_controller_connection_leader_flow():
    # Spin up a mock leader controller TCP server
    received_commands = []
    
    async def handle_connection(reader, writer):
        try:
            # 1. Read BrokerRegisterCommand
            len_data = await reader.readexactly(framer.PREFIX_SIZE)
            length = framer.decode_length(len_data)
            data = await reader.readexactly(length)
            command = parse_command(data)
            received_commands.append(command)
            
            # Send TopicMetaDataListResponse back
            meta_resp = resp.TopicMetaDataListResponse(meta_list=[])
            frame = encode_response(meta_resp)
            writer.write(frame.header)
            if frame.payload:
                writer.write(frame.payload)
            await writer.drain()
            
            # Keep reading commands (like ping/heartbeat)
            while True:
                len_data = await reader.readexactly(framer.PREFIX_SIZE)
                length = framer.decode_length(len_data)
                data = await reader.readexactly(length)
                command = parse_command(data)
                received_commands.append(command)
                
                # If ping, respond CPR
                if isinstance(command, cmd.RegisterTopicCommand):
                    # Respond with TopicMetaDataResponse
                    topic_resp = resp.TopicMetaDataResponse(
                        topic=command.topic,
                        leader_id="replica1",
                        leader_addr="127.0.0.1",
                        leader_port=9092,
                        replica_list=["replica1", "replica2", "replica3"],
                        version=1,
                    )
                    topic_resp.header = command.header  # echo header
                    frame = encode_response(topic_resp)
                    writer.write(frame.header)
                    if frame.payload:
                        writer.write(frame.payload)
                    await writer.drain()
        except Exception:
            print("Mock controller server connection closed.")
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle_connection, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    config = BrokerConfig(
        host="127.0.0.1",
        port=9092,
        controller_list=[
            ControllerAddress(controller_id="ctrl-1", host="127.0.0.1", port=port)
        ],
        checksum_enable=True,
        writer_config=Mock()
    )

    rep_mgr = DummyReplicaManager()
    
    conn = await ControllerConnection.connect(config, rep_mgr) #type: ignore
    assert conn.connected
    assert rep_mgr.applied_list is not None
    
    # Wait for a heartbeat to be sent or register topic
    res = await conn.register_topic("test-new-topic")
    
    await conn.close()
    server.close()
    await server.wait_closed()

    # Verify we sent BrokerRegisterCommand and RegisterTopicCommand
    assert len(received_commands) >= 2
    assert isinstance(received_commands[0], cmd.BrokerRegisterCommand)
    assert any(isinstance(c, cmd.RegisterTopicCommand) and c.topic == "test-new-topic" for c in received_commands)

@pytest.mark.asyncio
async def test_controller_connection_redirect_flow():
    # Setup two mock servers:
    # 1. Port A (Follower): returns NotLeaderControllerResponse pointing to Port B
    # 2. Port B (Leader): responds normally
    
    port_b = 0
    leader_server_received = []

    async def handle_follower(reader, writer):
        try:
            # Read whatever comes (should be register command)
            len_data = await reader.readexactly(framer.PREFIX_SIZE)
            length = framer.decode_length(len_data)
            await reader.readexactly(length)
            
            # Send NotLeaderControllerResponse pointing to port_b
            nlc = resp.NotLeaderControllerResponse(
                leader_id="leader-ctrl",
                leader_host="127.0.0.1",
                leader_port=port_b
            )
            frame = encode_response(nlc)
            writer.write(frame.header)
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def handle_leader(reader, writer):
        try:
            len_data = await reader.readexactly(framer.PREFIX_SIZE)
            length = framer.decode_length(len_data)
            data = await reader.readexactly(length)
            command = parse_command(data)
            leader_server_received.append(command)
            
            meta_resp = resp.TopicMetaDataListResponse(meta_list=[])
            frame = encode_response(meta_resp)
            writer.write(frame.header)
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server_leader = await asyncio.start_server(handle_leader, "127.0.0.1", 0)
    port_b = server_leader.sockets[0].getsockname()[1]

    server_follower = await asyncio.start_server(handle_follower, "127.0.0.1", 0)
    port_a = server_follower.sockets[0].getsockname()[1]

    config = BrokerConfig(
        host="127.0.0.1",
        port=9092,
        controller_list=[
            ControllerAddress(controller_id="follower-ctrl", host="127.0.0.1", port=port_a),
            ControllerAddress(controller_id="leader-ctrl", host="127.0.0.1", port=port_b)
        ],
        checksum_enable=True,
        writer_config=Mock()
    )

    rep_mgr = DummyReplicaManager()
    
    conn = await ControllerConnection.connect(config, rep_mgr) #type: ignore
    assert conn.connected
    
    await conn.close()
    server_follower.close()
    await server_follower.wait_closed()
    server_leader.close()
    await server_leader.wait_closed()

    assert len(leader_server_received) > 0
    assert isinstance(leader_server_received[0], cmd.BrokerRegisterCommand)
