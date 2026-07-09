import pytest

from pylogstream_protocol.parser import parse_command, parse_response, parse_packet_header
from pylogstream_protocol.encoder import encode_command, encode_response, encode_packet_header
from pylogstream_protocol.commands import PublishCommand, CommitOffsetCommand, ISRChangeCommand
from pylogstream_protocol.response import PubAckResponse, OffsetAckResponse


def round_trip_command(cmd, checksum_enable=False):
    frame = encode_command(cmd, checksum_enable=checksum_enable)
    # Extract the framed payload produced by encode_frame
    from pylogstream_protocol.framer import PREFIX_SIZE, decode_length
    data = frame.header[PREFIX_SIZE:PREFIX_SIZE+decode_length(frame.header[:PREFIX_SIZE])]
    return parse_command(data)


def round_trip_response(resp):
    frame = encode_response(resp)
    from pylogstream_protocol.framer import PREFIX_SIZE, decode_length
    data = frame.header[PREFIX_SIZE:PREFIX_SIZE+decode_length(frame.header[:PREFIX_SIZE])]
    return parse_response(data)

from pylogstream_protocol.common import Packet, PacketHeader

def test_packet_header_roundtrip():
    header = PacketHeader(correlation_id="12345")
    msg = Packet()
    msg.header = header
    encoded_header = encode_packet_header(msg)
    # Now parse the header back
    parsed_header, rem = parse_packet_header(encoded_header + b'extra data')
    assert isinstance(parsed_header, PacketHeader), "Parsed header is not of type PacketHeader"
    assert parsed_header.correlation_id == "12345"
    assert rem == b'extra data', "Remaining data after header parsing is incorrect"

def test_optional_packet_header_roundtrip():
    # Test with no header
    packet = Packet()
    encoded_header = encode_packet_header(packet)
    # Now parse the header back
    parsed_header, rem = parse_packet_header(encoded_header + b'extra data')
    assert parsed_header is None, "Parsed header should be None when no header is present"
    assert rem == b'extra data', "Remaining data after header parsing is incorrect"

def test_positional_arguments_publish_command():
    # Create a PublishCommand with positional arguments
    cmd = PublishCommand('my_topic', b'my_payload', 1, False)
    parsed = round_trip_command(cmd, checksum_enable=False)
    assert isinstance(parsed, PublishCommand)
    assert parsed.topic == 'my_topic'
    assert parsed.payload == b'my_payload'
    assert parsed.acks == 1
    assert parsed.contains_checksum is False

def test_publish_parse_valid_acks():
    # test acks 1
    cmd = PublishCommand(topic='t', payload=b'hello', acks=1, contains_checksum=False)
    parsed = round_trip_command(cmd, checksum_enable=False)
    assert isinstance(parsed, PublishCommand)
    assert parsed.topic == 't'
    assert parsed.acks == 1

    # test acks -1
    cmd = PublishCommand(topic='t', payload=b'hello', acks=-1, contains_checksum=False)
    parsed = round_trip_command(cmd, checksum_enable=False)
    assert isinstance(parsed, PublishCommand)
    assert parsed.acks == -1

    # test acks 0
    cmd = PublishCommand(topic='t', payload=b'hello', acks=0, contains_checksum=False)
    parsed = round_trip_command(cmd, checksum_enable=False)
    assert isinstance(parsed, PublishCommand)
    assert parsed.acks == 0


def test_commit_offset_parse_and_response_roundtrip():
    cmd = CommitOffsetCommand(topic='topic1', offset=42, acks=1)
    parsed = round_trip_command(cmd, checksum_enable=False)
    assert isinstance(parsed, CommitOffsetCommand)
    assert parsed.topic == 'topic1'
    assert parsed.offset == 42
    assert parsed.acks == 1

    # Simulate broker response for commit
    resp = OffsetAckResponse(topic='topic1', offset=42, acks=1)
    parsed_resp = round_trip_response(resp)
    assert isinstance(parsed_resp, OffsetAckResponse)
    assert parsed_resp.topic == 'topic1'
    assert parsed_resp.offset == 42
    assert parsed_resp.acks == 1


def test_puback_parse_and_response_roundtrip():
    resp = PubAckResponse(topic='topic2', offset=100, acks=0)
    parsed_resp = round_trip_response(resp)
    assert isinstance(parsed_resp, PubAckResponse)
    assert parsed_resp.acks == 0
    assert parsed_resp.offset == 100


@pytest.mark.parametrize("bad", ["PUB t 2 payload", "CMT t 5 1", "ACK t 3 1", "OAK t 10 1"])
def test_invalid_acks_raise_value_error(bad):
    # feed directly to parser which operates on raw bytes
    with pytest.raises(ValueError):
        if bad.startswith('PUB'):
            parse_command(bad.encode())
        else:
            parse_response(bad.encode())

def test_isr_change_parse_and_response_roundtrip():
    cmd = ISRChangeCommand(topic='topic3', isr_list=['node1', 'node2'])
    parsed = round_trip_command(cmd, checksum_enable=False)
    assert isinstance(parsed, ISRChangeCommand)
    assert parsed.topic == 'topic3'
    assert parsed.isr_list == ['node1', 'node2']