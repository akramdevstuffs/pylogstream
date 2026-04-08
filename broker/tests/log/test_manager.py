import pytest
from pylogstream_broker.log.log_manager import LogManager, TopicDoesntExistsError
from pylogstream_broker.config import LogConfig
import threading
import time

@pytest.fixture
def log_config(tmp_path):
    return LogConfig(
        log_dir=str(tmp_path),
        segment_cache_size=1024*1024,
        init_segment_size=1024*1024,
        segment_size_inc=1024*1024,
        max_segment_size=10*1024*1024,
        retension_ms=60*60*1000, # 1 hour
        rollover_ms=10*60*1000 # 10 minutes
    )

@pytest.fixture
def log_manager(log_config):
    return LogManager(log_config)

def test_log_manager_create_topic(log_manager):
    topic = "test-topic-create"
    log_manager.create_topic(topic)
    # Check if topic is created by trying to read from it
    # Must not raise TopicDoesntExistsError
    try:
        log_manager.read_bytes(topic, 0, 10)
    except TopicDoesntExistsError:
        pytest.fail("Topic was not created successfully")
    except Exception:
        pass
    
    # Creating the same topic
    log_manager.create_topic(topic)

def test_log_manager_read_nonexistent_topic(log_manager):
    with pytest.raises(TopicDoesntExistsError):
        log_manager.read_bytes("nonexistent-topic", 0, 10)

def test_log_manager_read(log_manager):
    topic = "test-topic-read"
    log_manager.create_topic(topic)
    data = b"Hello, World!"
    len_bytes = len(data).to_bytes(4, byteorder='big')
    offset = log_manager.append(topic, data)
    assert offset == 0
    read_data = log_manager.read_bytes(topic, offset, 4+len(data))
    print(read_data.data)
    assert read_data.data == len_bytes + data
    assert read_data.next_offset == offset + 4 +len(data)

def test_log_manager_concurent_writes(log_manager):
    topic = 'test-topic'
    log_manager.create_topic(topic)
    data = b"Concurrent Data"
    def write_data():
        for _ in range(100):
            log_manager.append(topic, data)
    threads = [threading.Thread(target=write_data) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert log_manager.get_latest_offset(topic) == 5 * 100 * (4 + len(data))
    new_data = log_manager.read_bytes(topic, 0, 5*100*(4+len(data)))
    assert new_data.data == (len(data).to_bytes(4, 'big') + data) * 5 * 100
    assert new_data.next_offset == 5 * 100 * (4 + len(data))

def test_log_manager_rollover(log_manager):
    topic = "test-topic-rollover"
    log_manager.create_topic(topic)
    data = b"A" * (1024 * 1024) # 1 MB of data
    for i in range(11): # Write 11 MB of data to trigger rollover
        log_manager.append(topic, data)
    assert log_manager.get_latest_offset(topic) == 11 * (4 + len(data))
    # Now even asking for 12 MB should only return 9 MB of data because of rollover
    read_data = log_manager.read_bytes(topic, 0, 12 * (4 + len(data)))
    assert read_data.data == (len(data).to_bytes(4, 'big') + data) * 9
    assert read_data.next_offset == 9 * (4 + len(data))

def test_log_manager_rollover_concurrent(log_manager):
    topic = "test-topic-rollover-concurrent"
    log_manager.create_topic(topic)
    data = b"B" * (1024 * 1024) # 1 MB of data
    def write_data():
        for _ in range(11): # Write 11 MB of data to trigger rollover
            log_manager.append(topic, data)
    threads = [threading.Thread(target=write_data) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert log_manager.get_latest_offset(topic) == 5 * 11 * (4 + len(data))
    read_data = b''
    offset = 0
    while offset < log_manager.get_latest_offset(topic):
        result = log_manager.read_bytes(topic, offset, 12 * (4 + len(data)))
        read_data += result.data
        offset = result.next_offset
    assert read_data == (len(data).to_bytes(4, 'big') + data) * 5 * 11

def test_log_manager_rollover_ms(log_config):
    new_log_config = LogConfig(
        log_dir=log_config.log_dir,
        segment_cache_size=log_config.segment_cache_size,
        init_segment_size=log_config.init_segment_size,
        segment_size_inc=log_config.segment_size_inc,
        max_segment_size=log_config.max_segment_size,
        retension_ms=log_config.retension_ms,
        rollover_ms=1000 # 1 second
    )
    log_manager = LogManager(new_log_config)
    topic = "test-topic-retension"
    log_manager.create_topic(topic)
    data = b"C" * (1024 * 1024) # 1 MB of data
    log_manager.append(topic, data)
    assert log_manager.get_latest_offset(topic) == 4 + len(data)
    # Wait for 2 seconds to trigger retension
    time.sleep(2)
    read_data = log_manager.read_bytes(topic, 0, 4*(4 + len(data)))
    # After rollover, we will only get 1 MB data
    assert read_data.data == (len(data).to_bytes(4, 'big') + data)
    assert read_data.next_offset == 4 + len(data)


def test_log_manager_retension(log_config):
    # TODO: After retension service is implemented, test the retension here
    pass