import pytest
import asyncio
from unittest.mock import Mock, AsyncMock

from pylogstream_broker.config import ReplicaConfig
from pylogstream_broker.replication.manager import ReplicaManager, Role
from pylogstream_protocol.response import TopicMetaDataResponse, TopicMetaDataListResponse

@pytest.fixture
def mock_log_manager():
    lm = Mock()
    return lm

@pytest.fixture
def replica_config():
    return ReplicaConfig(
        id="broker-1",
        min_isr_count=1,
        max_isr_lag_ms=1000
    )

@pytest.mark.asyncio
async def test_replica_manager_metadata_leader(mock_log_manager, replica_config):
    rm = ReplicaManager(log_manager=mock_log_manager, config=replica_config)

    meta = TopicMetaDataResponse(
        topic="test-topic",
        leader_id="broker-1",
        leader_addr="127.0.0.1",
        leader_port=9092,
        replica_list=["broker-1", "broker-2"],
        version=1
    )

    await rm.apply_metadata(meta)

    # Verify log manager was called to create the topic
    mock_log_manager.create_topic.assert_called_once_with("test-topic")

    # Verify topic is registered as LEADER
    assert "test-topic" in rm.topics
    state = rm.topics["test-topic"]
    assert state.role == Role.LEADER
    assert state.leader is not None
    assert state.fetcher is None

    await rm.close_all()

@pytest.mark.asyncio
async def test_replica_manager_metadata_follower(mock_log_manager, replica_config, monkeypatch):
    rm = ReplicaManager(log_manager=mock_log_manager, config=replica_config)

    # Mock the ReplicaFetcher creation
    mock_fetcher = AsyncMock()
    monkeypatch.setattr("pylogstream_broker.replication.manager.ReplicaFetcher.create", AsyncMock(return_value=mock_fetcher))

    meta = TopicMetaDataResponse(
        topic="test-topic2",
        leader_id="broker-2",
        leader_addr="127.0.0.1",
        leader_port=9093,
        replica_list=["broker-1", "broker-2"],
        version=1
    )

    await rm.apply_metadata(meta)

    # Verify topic is registered as FOLLOWER
    assert "test-topic2" in rm.topics
    state = rm.topics["test-topic2"]
    assert state.role == Role.FOLLOWER
    assert state.leader is None
    assert state.fetcher is not None

    await rm.close_all()

@pytest.mark.asyncio
async def test_replica_manager_not_in_replica_list(mock_log_manager, replica_config):
    rm = ReplicaManager(log_manager=mock_log_manager, config=replica_config)

    # Pre-register topic
    meta_leader = TopicMetaDataResponse(
        topic="test-topic3",
        leader_id="broker-1",
        leader_addr="127.0.0.1",
        leader_port=9092,
        replica_list=["broker-1"],
        version=1
    )
    await rm.apply_metadata(meta_leader)
    assert "test-topic3" in rm.topics

    # Send new metadata where this broker is not in the replica list
    meta_not_replica = TopicMetaDataResponse(
        topic="test-topic3",
        leader_id="broker-2",
        leader_addr="127.0.0.1",
        leader_port=9093,
        replica_list=["broker-2"],
        version=2
    )
    await rm.apply_metadata(meta_not_replica)

    # Verify topic was removed/cleaned up
    assert "test-topic3" not in rm.topics

    await rm.close_all()

@pytest.mark.asyncio
async def test_replica_manager_apply_metadata_list(mock_log_manager, replica_config):
    rm = ReplicaManager(log_manager=mock_log_manager, config=replica_config)

    meta1 = TopicMetaDataResponse(
        topic="topic-1",
        leader_id="broker-1",
        leader_addr="127.0.0.1",
        leader_port=9092,
        replica_list=["broker-1"],
        version=1
    )
    meta2 = TopicMetaDataResponse(
        topic="topic-2",
        leader_id="broker-1",
        leader_addr="127.0.0.1",
        leader_port=9092,
        replica_list=["broker-1"],
        version=1
    )

    meta_list_resp = TopicMetaDataListResponse(meta_list=[meta1, meta2])
    await rm.apply_metadata_list(meta_list_resp)

    assert "topic-1" in rm.topics
    assert "topic-2" in rm.topics

    # Send updated list without topic-1
    meta_list_resp_updated = TopicMetaDataListResponse(meta_list=[meta2])
    await rm.apply_metadata_list(meta_list_resp_updated)

    assert "topic-1" not in rm.topics
    assert "topic-2" in rm.topics

    await rm.close_all()
