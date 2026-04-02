import pytest

from pylogstream_protocol.parser import parse_command, parse_response
from pylogstream_protocol.encoder import encode_command, encode_response
from pylogstream_protocol.commands import PublishCommand, CommitOffsetCommand
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
