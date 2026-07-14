import pytest
import pytest_asyncio
import asyncio
from pylogstream_broker.config import Config, BrokerConfig, WriterConfig, LogConfig, ReplicaConfig, ControllerAddress
from pylogstream_broker.network.broker import Broker
from pylogstream_protocol import commands, response

@pytest.fixture
def config(tmp_path):
    log_dir = tmp_path / "logs"
    return Config(
        broker=BrokerConfig(
            host="127.0.0.1",
            port=9092,
            controller_list=[
                ControllerAddress(controller_id="controller1", host="127.0.0.1", port=9093)
            ],
            checksum_enable=True,
            writer_config=WriterConfig()
        ),
        log=LogConfig(
            log_dir=log_dir,
            retension_ms=7*24*3600*1000,
            rollover_ms=24*3600*1000,
            max_segment_size=1024*1024*10,
            init_segment_size=1024*1024,
            segment_size_inc=1024*1024
        ),
        replica=ReplicaConfig(id="replica1", min_isr_count=1)
    )

class MockControllerConnection:
    def __init__(self, config, replica_manager):
        self.config = config
        self.replica_manager = replica_manager
        # Apply metadata to replica_manager
        self.meta_list = [
            response.TopicMetaDataResponse(
                topic="test-topic",
                leader_id=self.config.replica.id,
                leader_addr=self.config.broker.host,
                leader_port=self.config.broker.port,
                replica_list=[self.replica_manager.broker_id, 'replica2', 'replica3'],
                version=1
            )
        ]
        self.connected = False
    
    @classmethod
    async def connect(cls, config, replica_manager):
        instance = cls(config, replica_manager)
        await instance._start()
        instance.connected = True
        return instance

    async def _start(self):
        await self.replica_manager.apply_metadata_list(response.TopicMetaDataListResponse(self.meta_list))
        # Simulate starting the controller connection
        await asyncio.sleep(0.1)
    
    async def register_topic(self, topic):
        resp = response.TopicMetaDataResponse(
            topic=topic,
            leader_id=self.config.broker.controller_list[0].controller_id,
            leader_addr=self.config.broker.controller_list[0].host,
            leader_port=self.config.broker.controller_list[0].port,
            replica_list=[self.replica_manager.broker_id, 'replica2', 'replica3'],
            version=1
        )
        self.replica_manager.apply_metadata(resp)
        return resp
    
    async def request_metadata(self, topic):
        return response.TopicMetaDataResponse(
            topic=topic,
            leader_id=self.config.broker.controller_list[0].controller_id,
            leader_addr=self.config.broker.controller_list[0].host,
            leader_port=self.config.broker.controller_list[0].port,
            replica_list=[self.replica_manager.broker_id],
            version=1
        )
    async def close(self):
        # Simulate closing the connection
        await asyncio.sleep(0.1)

@pytest.fixture
def mock_controller_connection(monkeypatch,config):
    async def fake_create(self):
        conn = MockControllerConnection(config, self._Broker__replica_manager)
        await conn._start()
        conn.connected = True
        return conn
    monkeypatch.setattr(Broker, "_create_controller_connection", fake_create)

@pytest_asyncio.fixture
async def broker(config, mock_controller_connection):
    broker = Broker(config)
    task = asyncio.create_task(broker.start_server())
    await broker.wait_ready()
    yield broker
    await broker.close()
    task.cancel()