import pytest
import asyncio
import time
from pylogstream_broker.config import WriterConfig, LogConfig
from pylogstream_broker.network.writer import Writer, WriteRequest
from pylogstream_broker.log.log_manager import LogManager

@pytest.fixture
def log_manager(tmp_path):
    log_config = LogConfig(
        log_dir=str(tmp_path),
        retension_ms=1000,  # 1 second retention for testing
        rollover_ms=500,  # 0.5 second rollover for testing
        max_segment_size=1024 * 1024,  # 1 MB segment size for testing
        init_segment_size=1024 * 1024,  # 1 MB initial segment size for testing
        segment_size_inc=1024 * 1024,  # 1 MB segment size increment for testing
    )
    return LogManager(log_config)

@pytest.fixture
def writer(log_manager):
    config = WriterConfig()
    writer = Writer(log_manager, config)
    return writer

@pytest.mark.asyncio
async def test_writer_submit(writer: Writer):
    fut = asyncio.get_running_loop().create_future()
    data = b'test data'
    req = WriteRequest(
        producer_id="producer1",
        topic="test-topic",
        data=data,
        created_at=time.time(),
        callback=fut
    )
    writer.submit(req)
    result = await asyncio.wait_for(fut, timeout=2)
    assert result == 0
    fut = asyncio.get_running_loop().create_future()
    req2 = WriteRequest(
        producer_id="producer1",
        topic="test-topic",
        data=data,
        created_at=time.time(),
        callback=fut
    )
    writer.submit(req2)
    result = await asyncio.wait_for(fut, timeout=2)
    assert result == 0 + len(data) + 4
    await writer.close()

@pytest.mark.asyncio
async def test_writer_batching(writer: Writer):
    data1 = b'message 1'
    data2 = b'message 2'
    fut1 = asyncio.get_running_loop().create_future()
    fut2 = asyncio.get_running_loop().create_future()
    req1 = WriteRequest(
        producer_id="producer1",
        topic="test-topic",
        data=data1,
        created_at=time.time(),
        callback=fut1
    )
    req2 = WriteRequest(
        producer_id="producer1",
        topic="test-topic",
        data=data2,
        created_at=time.time(),
        callback=fut2
    )
    writer.submit(req1)
    writer.submit(req2)
    # Both will stay at queue 
    assert not fut1.done()
    assert not fut2.done()
    # Wait for batch to be processed
    await asyncio.sleep(2*writer.config.max_batch_wait)
    assert fut1.done()
    assert fut2.done()
    assert fut1.result() == 0
    assert fut2.result() == len(data1) + 4
    await writer.close()

@pytest.mark.asyncio
async def test_writer_close(writer: Writer):
    data = b'test data'
    fut = asyncio.get_running_loop().create_future()
    req = WriteRequest(
        producer_id="producer1",
        topic="test-topic",
        data=data,
        created_at=time.time(),
        callback=fut
    )
    writer.submit(req)
    await writer.close()
    assert fut.done()
    with pytest.raises(Exception):
        fut.result()