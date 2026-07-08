import pytest
from pylogstream_controller.metastore.manager import MetastoreManager
from pylogstream_controller.metastore.models import TopicMetadata


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _setup_manager_with_brokers(broker_ids: list[str]) -> MetastoreManager:
    mgr = MetastoreManager(replicas_per_topic=len(broker_ids))
    for bid in broker_ids:
        await mgr.register_broker(bid, host="127.0.0.1", port=9000)
    return mgr


def _make_topic(*, topic="t", leader_id, replica_list, isr_list, version=1) -> TopicMetadata:
    return TopicMetadata(topic=topic, leader_id=leader_id,
                         replica_list=replica_list, isr_list=isr_list, version=version)


# ---------------------------------------------------------------------------
# _elect_leader
# ---------------------------------------------------------------------------

class TestElectLeader:
    def _mgr(self) -> MetastoreManager:
        return MetastoreManager(replicas_per_topic=3)

    def test_prefers_first_isr_member(self):
        mgr = self._mgr()
        topic = _make_topic(leader_id="b1", replica_list=["b1", "b2", "b3"], isr_list=["b2", "b3"])
        assert mgr._elect_leader(topic) == "b2"

    def test_falls_back_to_replica_list_when_isr_empty(self):
        mgr = self._mgr()
        topic = _make_topic(leader_id="b1", replica_list=["b1", "b2"], isr_list=[])
        assert mgr._elect_leader(topic) == "b1"

    @pytest.mark.asyncio
    async def test_falls_back_to_hash_ring_when_both_empty(self):
        mgr = MetastoreManager(replicas_per_topic=1)
        await mgr.register_broker("ring-broker", host="127.0.0.1", port=9001)
        topic = _make_topic(topic="orphan", leader_id="", replica_list=[], isr_list=[])
        assert mgr._elect_leader(topic) == "ring-broker"

    def test_returns_empty_string_when_nothing_available(self):
        mgr = self._mgr()
        topic = _make_topic(leader_id="", replica_list=[], isr_list=[])
        assert mgr._elect_leader(topic) == ""


# ---------------------------------------------------------------------------
# deregister_broker
# ---------------------------------------------------------------------------

class TestDeregisterBrokerISRElection:

    @pytest.mark.asyncio
    async def test_elects_isr_member_after_leader_deregistration(self):
        mgr = await _setup_manager_with_brokers(["b1", "b2", "b3"])
        await mgr.storage.add_topic(
            _make_topic(topic="my-topic", leader_id="b1",
                        replica_list=["b1", "b2", "b3"], isr_list=["b1", "b2", "b3"])
        )

        result = (await mgr.deregister_broker("b1"))[0]

        assert result.leader_id in ["b2", "b3"]
        assert "b1" not in result.isr_list

    @pytest.mark.asyncio
    async def test_isr_member_preferred_over_lagging_replica(self):
        mgr = await _setup_manager_with_brokers(["b1", "b2", "b3"])
        await mgr.storage.add_topic(
            _make_topic(topic="selective-isr", leader_id="b1",
                        replica_list=["b1", "b2", "b3"], isr_list=["b1", "b2"])
        )

        result = (await mgr.deregister_broker("b1"))[0]

        assert result.leader_id == "b2"  # b3 is lagging, must not win

    @pytest.mark.asyncio
    async def test_falls_back_to_replica_list_when_isr_empty(self):
        mgr = await _setup_manager_with_brokers(["b1", "b2"])
        await mgr.storage.add_topic(
            _make_topic(topic="sole-isr", leader_id="b1",
                        replica_list=["b1", "b2"], isr_list=["b1"])
        )

        result = (await mgr.deregister_broker("b1"))[0]

        assert result.leader_id == "b2"
        assert result.isr_list == []


# ---------------------------------------------------------------------------
# update_topic_isr
# ---------------------------------------------------------------------------

class TestUpdateTopicISRElection:

    @pytest.mark.asyncio
    async def test_re_elects_when_leader_not_in_new_isr(self):
        mgr = MetastoreManager(replicas_per_topic=3)
        await mgr.storage.add_topic(
            _make_topic(topic="reelect-topic", leader_id="b1",
                        replica_list=["b1", "b2", "b3"], isr_list=["b1", "b2", "b3"])
        )

        result = await mgr.update_topic_isr("reelect-topic", ["b2", "b3"])

        assert result.leader_id in ["b2", "b3"]

    @pytest.mark.asyncio
    async def test_leader_unchanged_when_still_in_isr(self):
        mgr = MetastoreManager(replicas_per_topic=3)
        await mgr.storage.add_topic(
            _make_topic(topic="stable-topic", leader_id="b1",
                        replica_list=["b1", "b2", "b3"], isr_list=["b1", "b2", "b3"])
        )

        result = await mgr.update_topic_isr("stable-topic", ["b1", "b2"])

        assert result.leader_id == "b1"

    @pytest.mark.asyncio
    async def test_falls_back_to_replica_list_when_new_isr_empty(self):
        mgr = MetastoreManager(replicas_per_topic=2)
        await mgr.storage.add_topic(
            _make_topic(topic="empty-isr", leader_id="b1",
                        replica_list=["b1", "b2"], isr_list=["b1"])
        )

        result = await mgr.update_topic_isr("empty-isr", [])

        assert result.leader_id in ["b1", "b2"]

    @pytest.mark.asyncio
    async def test_version_incremented_on_isr_update(self):
        mgr = MetastoreManager(replicas_per_topic=2)
        await mgr.storage.add_topic(
            _make_topic(topic="version-topic", leader_id="b1",
                        replica_list=["b1", "b2"], isr_list=["b1", "b2"], version=5)
        )

        result = await mgr.update_topic_isr("version-topic", ["b1"])

        assert result.version == 6
