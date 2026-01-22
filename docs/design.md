# PyLogStreams - Design Document

## 1. Overview

PyLogStreams is a lightweight, Kafka-inspired message log system built in Python.  
It combines Redis-style pub/sub simplicity with Kafka-style persistent, segmented message storage.  
It is fully asynchronous, using **`asyncio`** and **`uvloop`** for high-performance networking, allowing the broker to handle thousands of concurrent producer and consumer connections efficiently.  
The goal is to provide a simple, educational message broker suitable for chat applications, logging systems, or as a foundation for distributed systems experiments.

---

## 2. Goals and Non-Goals

### ✅ Goals

| Goal                                      | Description                                                                             |
| ----------------------------------------- | --------------------------------------------------------------------------------------- |
| **High throughput for sequential writes** | Use OS page cache and batched flushing to maximize write performance.                   |
| **Persistent logs**                       | Messages survive restarts using append-only file segments.                              |
| **Fast reads**                            | Memory-mapped reads via an LRU cache of segment handles.                                |
| **Offset tracking**                       | Consumers can resume reading after restarts via persisted offsets.                      |
| **Asynchronous maintenance**              | Background threads handle cleanup and flushing without blocking producers or consumers. |

### 🚫 Non-Goals (for now)

- Full Kafka replication and partition balancing.
- Distributed coordination (e.g., ZooKeeper or Raft).
- Guaranteed exactly-once delivery semantics.
- Cross-machine scaling.

---

## 3. High-Level Architecture

PyLogStreams is built around four key concepts:

1. **Topics** — Independent message streams, each backed by a directory on disk.
2. **Segments** — Fixed-size log files per topic to store messages sequentially.
3. **Offsets** — Each consumer maintains its position in a topic’s log.
4. **Broker** — Central coordinator for producer/consumer communication.

````

    ```
         ┌──────────────────────────┐
         │         Broker           │
         │──────────────────────────│
         │ Handles clients          │
         │ Routes messages          │
         │ Updates offsets          │
         └────────────┬─────────────┘
                      │
    ┌─────────────────┼─────────────────┐
    │                                   │
┌───────────────┐               ┌────────────────────┐
│  LogManager   │               │ OffsetsManager     │
│───────────────│               │────────────────────│
│ append/read   │               │ update/get         │
│ lazy_flusher  │               │ persist →          │
│ log_cleaner   │               │ __consumer_offsets │
│ file_remover  │               └────────────────────┘
│ SegmentCache  │
└───────────────┘

````

Producers → [Broker] → LogManager → Disk Segments
Consumers ← [Broker] ← LogManager ← Disk Segments

The **Broker** receives client requests, appends messages to disk via the **LogManager**, and serves messages to consumers by tracking offsets via **OffsetsManager**.
Background services like **LogCleaner** and **FileRemover** maintain the log directories and remove obsolete or expired segments. Compaction is not yet implemented.

### Log Storage Layout

All topic data is stored under the `/logs` directory (configurable).  
Each topic has its own subdirectory containing multiple segment files:

```

/logs/
├── test_topic/
│     ├── 00000000000000000000.log
│     ├── 00000000000000001000.log
│     └── 00000000000000002000.log
└── __consumer_offsets/
├── 00000000000000000000.log

```

Each segment file is a fixed-size append-only log.  
LogManager handles rolling over to new segments once a file reaches the configured maximum size.

---

## 4. Core Components

| Component              | Description                                                                                                                                                                                                        |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **broker.py**          | Central orchestrator. Handles producer and consumer connections asynchronously using **`asyncio`** and **`uvloop`**, routes requests, and coordinates LogManager and OffsetsManager interactions. Decides when to: |
|                        | - Append new messages (`LogManager.append_message`)                                                                                                                                                                |
|                        | - Read messages for consumers (`LogManager.read_message`)                                                                                                                                                          |
|                        | - Get offset for each client/topic (`OffsetsManager.get_client_offsets`)                                                                                                                                           |
|                        | - Update and persist offsets (`OffsetsManager.update_client_offset`)                                                                                                                                               |
| **log_manager.py**     | Manages low-level persistence and retrieval. Maintains topic directories and segmented log files. Exposes:                                                                                                         |
|                        | - `append_message(topic, message)` — appends a message to a topic’s current segment.                                                                                                                               |
|                        | - `read_message(topic, offset)` — retrieves messages starting from a given offset.                                                                                                                                 |
|                        | - `load_topic_logs()` — scans all persisted log segments from disk.                                                                                                                                                |
|                        | - `start_threads()` — starts background threads for flushing, cleaning, and file removal.                                                                                                                          |
|                        | Internally manages:                                                                                                                                                                                                |
|                        | ├── **lazy_flusher** — periodically flushes buffered writes to disk.                                                                                                                                               |
|                        | ├── **log_cleaner** — trims expired or obsolete segments.                                                                                                                                                          |
|                        | └── **file_remover** — deletes old segment files asynchronously.                                                                                                                                                   |
| **offsets_manager.py** | Tracks per-client offsets for each topic. Stored in memory for fast access and periodically persisted to the internal log (`__consumer_offsets`) for recovery across restarts.                                     |
|                        | - `load_client_offsets()` - load persisted client offset from disk on start.                                                                                                                                       |
| **segment_cache.py**   | Provides an in-memory LRU cache for open log segments:                                                                                                                                                             |
|                        | - `LRUCache.get(key)` and `LRUCache.put(key, value)` for mmaped segments.                                                                                                                                          |
|                        | - Accepts an eviction callback for cleanup (closing FDs, unmapping mmaps).                                                                                                                                         |
|                        | - Helps `LogManager` efficiently reuse hot segments and limit open FDs.                                                                                                                                            |
| **utility.py**         | Utility functions such as `set_sequential_hint`.                                                                                                                                                                   |
| **tests/**             | Benchmarking tools for throughput, latency, and memory usage. Measures producer/consumer `msgs/s`, append latency, and cache performance under load.                                                               |

---

## 5. Data Flow

### **Producer → Broker → LogManager**

1. A producer sends a message asynchronously to a topic.
2. Broker calls `await LogManager.append_message(topic, message)`.
3. LogManager writes to the active segment buffer.
4. Lazy flusher periodically flushes data to disk.
5. When the segment exceeds size limits, LogManager rolls over to a new segment.

### **Consumer → Broker → OffsetsManager + LogManager**

1. A **consumer** requests messages from a specific topic.
2. Broker queries **OffsetsManager** to get the consumer’s last committed offset.
3. Broker calls `await LogManager.read_message(topic, offset)` to fetch messages asynchronously.
4. LogManager serves messages from the **active segment**, or uses **SegmentCache** for older segments.
5. Broker writes messages to the consumer’s **async socket buffer** using non-blocking writes (`await writer.drain()`).
6. Once written, Broker updates the consumer’s offset via `OffsetsManager.update_offset()`.
7. OffsetsManager appends the update to `__consumer_offsets` for recovery.

> ⚙️ _Acknowledgments are implicit — delivery is considered successful once the message reaches the socket buffer. Explicit ACK-based commits may be added in the future._

---

## 6. Background Threads

| Thread           | Purpose                                       |
| ---------------- | --------------------------------------------- |
| **lazy_flusher** | Flushes buffered writes to disk periodically. |
| **log_cleaner**  | Removes or trims expired/obsolete segments.   |
| **file_remover** | Deletes old segment files asynchronously.     |

These run inside **LogManager**, keeping the broker lightweight and focused on routing.

---

## 7. Segment Cache Design

### Motivation

- Reading from disk on every fetch is slow.
- Keeping all segments mmaped consumes OS resources and file descriptors.

### Design

- Only active segments are mmaped; older segments are closed.
- Implements an **LRUCache** `{segment_id → mmap_handle}`.
- Eviction callback ensures proper cleanup:

```python
def on_evict(segment_id, mmap_handle):
    mmap_handle.close()
    fd.close()
```

- `LogManager` queries the cache before opening an old segment.

### Benefits

- Faster reads for hot topics.
- Prevents “Too many open files” errors.
- Controls memory and FD usage.

---

## 8. Design Decisions

| Decision                                | Reasoning                                              |
| --------------------------------------- | ------------------------------------------------------ |
| **Broker orchestrates state**           | Separates persistence logic; simplifies testing.       |
| **Segmentation of logs**                | Enables retention policies and efficient rollover.     |
| **Offsets persisted in internal topic** | Mirrors Kafka for durability and replayability.        |
| **Lazy flushing**                       | Balances throughput and durability by batching fsyncs. |
| **SegmentCache (LRU)**                  | Improves read latency and manages resources.           |
| **Asynchronous cleaners/removers**      | Keeps write path non-blocking and stable under load.   |

---

## 9. Testing and Benchmarking

- Throughput tests (`msgs/s` or MB/s) for producers and consumers.
- Append latency measurement.
- Flush and compaction timing (not implemented yet).
- Segment cache performance under different segment sizes and cache capacities (not implemented yet).

---

## 10. Trade-offs and Limitations

| Limitation                        | Notes                                                                               |
| --------------------------------- | ----------------------------------------------------------------------------------- |
| **At-least-once delivery**        | Crash between append and offset update may cause re-delivery.                       |
| **Lazy flushing durability risk** | Unflushed data may be lost on crash.                                                |
| **Single broker**                 | No replication or leader election yet.                                              |
| **Batching for read/write**       | Current per-message disk reads are inefficient; batching could improve performance. |
| **Backpressure**                  | Messages can be dropped if consumers are slow.                                      |

### Backpressure and Slow Consumers

- Even with **asyncio + uvloop**, very slow consumers can cause internal buffers to grow.
- Async I/O allows thousands of concurrent connections efficiently, but **bounded queues or flow control** are recommended.
- Future improvements:

  - Async **bounded send queues** per consumer.
  - Backpressure signaling to slow down producers.
  - Optional ACK-based delivery for at-least-once semantics.

---

## 11. Future Work

- **Handle backpressure:**
  Implement bounded per-consumer queues and flow control. Consider explicit ACK-based delivery for reliability.

- **Batching for message reads:**
  Read multiple messages from disk and send them as a single network batch to reduce disk I/O and improve throughput.

- **Broker clustering and replication:**
  Support multiple brokers with leader election and log replication for fault tolerance.

- **Compaction policies:**
  Implement delete vs. compact modes for segment compaction.

- **Monitoring and metrics:**
  Expose metrics (throughput, latency, lag, segment usage) via REST or socket API.

- **Integration and recovery tests:**
  Automated tests to verify behavior after crashes, consumer failures, or message replay.

---

## 12. References

- [Apache Kafka Storage Internals – Confluent.io](https://www.confluent.io/blog/kafka-fastest-messaging-system/)
- [Redpanda Storage Design – Vectorized.io](https://vectorized.io/blog/)
- _Designing Data-Intensive Applications_ – Martin Kleppmann
- [Python mmap module](https://docs.python.org/3/library/mmap.html)

---

## 13. Summary

PyLogStreams demonstrates how core streaming primitives can be built using only Python and OS-level primitives:

- Append-only file segments
- Memory-mapped reads
- LRU segment caching
- Offset tracking through internal logs
- **Asynchronous client handling using asyncio and uvloop**
- Asynchronous background maintenance

It’s not just a toy — it’s a readable, hackable foundation for anyone learning message broker internals or experimenting with distributed log design.
