# Log Storage Refactor Design

## Overview

This document describes the internal design of the log storage subsystem for the broker. The goal is to keep the logging system simple, predictable, and easy to reason about.

The storage layer is designed around an append-only segmented log. It should avoid hidden batching logic and should do exactly what higher layers request. Its job is to provide reliable reads, writes, retention, and cleanup with clear ownership boundaries between components.

The system is intended to support:

* append-only event logging
* pull-based 
* replay capability
* backpressure-aware delivery
* replication-transparent storage behavior

---

## Design Goals

### Primary goals

* Provide a predictable append-only logging system.
* Avoid internal batching complexity inside the log layer.
* Keep each component focused on a single responsibility.
* Support concurrent reads efficiently.
* Ensure only one writer per topic at a time.
* Make retention and cleanup explicit and controllable.
* Allow future extensions such as compression policies.

### Non-goals

* The log layer is not responsible for network transport.
* The log layer is not responsible for broker-level batching logic.
* The log layer should not contain business logic for replication or delivery semantics.

---

## High-Level Architecture

The storage subsystem is centered around these components:

* `LogManager`
* `SegmentRegistry`
* `SegmentPolicy`
* `SegmentMemory`
* `CleanerService`

Their responsibilities are intentionally separated:

* `LogManager` orchestrates all storage operations.
* `SegmentRegistry` owns segment metadata and lifecycle state.
* `SegmentPolicy` decides when a segment should roll over or expire.
* `SegmentMemory` manages loaded/opened segment resources such as mmap and file handles.
* `CleanerService` periodically executes cleanup workflows without owning storage logic.

---

## Core Concepts

### Segment

A segment is a portion of the append-only log for a topic. Segments allow the system to:

* append data incrementally
* read by offset
* roll over when a size threshold is reached
* expire and be deleted independently

Each topic consists of multiple segments.

### Segment metadata

Each segment has metadata describing properties such as:

* topic
* base offset
* current size
* capacity
* state
* file location

The metadata is managed separately from loaded memory resources.

### Segment lifecycle

A segment progresses through these states:

```text
ACTIVE -> SEALED -> EXPIRED -> DELETED
```

#### ACTIVE

The segment is writable and may also be readable.

#### SEALED

The segment is no longer writable, but still readable.

#### EXPIRED

The segment is eligible for cleanup according to retention rules.

#### DELETED

The segment has been removed from registry and underlying resources have been cleaned up.

---

## Component Details

## LogManager

`LogManager` acts as the facade of the log subsystem. It coordinates the registry, memory manager, and cleaner. It should remain predictable and not own complicated state beyond orchestration.

### Responsibilities

* coordinate read and write operations
* locate the correct segment for an offset
* create new segments when rollover is needed
* ask policy whether rollover or expiration should happen
* coordinate cleanup of expired segments
* start and manage the cleaner service
* load existing segments on broker restart

### Key principle

`LogManager` should orchestrate but not absorb the logic of the lower-level components.

### Interface

```python
class LogManager:
    async def read(topic, offset, size) -> BatchResult
    def write(topic, msgs[]) -> AppendResult
    def latest_offset(topic) -> Offset
    def load() -> None
    def start_cleaner() -> None
```

### Internal/private responsibilities

```python
get_expired_segments(now) -> List[ExpiredSegment]
cleanup(segment_meta) -> bool
```

### Behavior

#### `read(topic, offset, size) -> BatchResult`

* find the segment containing the requested offset
* acquire the segment through `SegmentMemory`
* read data from the segment
* release the segment after use
* support multiple concurrent readers

#### `write(topic, msgs[]) -> AppendResult`

* identify the active segment for the topic
* ask `SegmentPolicy` whether rollover is needed for incoming bytes
* if rollover is needed:

  * seal current segment
  * create a new active segment
* acquire the writable segment
* append messages
* update metadata and offsets
* release the segment

Only one writer per topic should be active at a time.

#### `latest_offset(topic) -> Offset`

* return the latest committed offset for the topic

#### `get_expired_segments(now) -> List[ExpiredSegment]`

Pseudo-flow:

```python
for sg in SegmentRegistry.list_segments(state=SEALED):
    if SegmentPolicy.is_expired(sg, now):
        SegmentRegistry.progress_state(sg, EXPIRED)

return SegmentRegistry.list_segments(state=EXPIRED)
```

#### `cleanup(segment_meta) -> bool`

* attempt to remove the segment from memory and registry
* cleanup succeeds only if the segment is safe to evict

#### `start_cleaner()`

* create a `CleanerService`
* inject the functions required for retention evaluation and cleanup
* start the background loop

Pseudo-flow:

```python
cleaner_service = CleanerService(get_expired_segments, cleanup)
cleaner_service.start()
```

---

## SegmentRegistry

`SegmentRegistry` owns segment metadata and lifecycle state. It should not make policy decisions on its own. It stores and organizes metadata and performs requested state transitions.

### Responsibilities

* maintain all segment metadata
* locate segments by topic and offset
* create segment metadata for new segments
* list segments by topic and/or state
* progress segments through lifecycle states
* remove deleted segments from the registry

### Interface

```python
class SegmentRegistry:
    def get_segment(topic, offset) -> SegmentMeta
    def create_segment(topic, base_offset) -> SegmentMeta
    def list_segments(topic) -> list[SegmentMeta]
    def list_segments(topic, state) -> list[SegmentMeta]
    def progress_state(segment, new_state) -> None
    def remove_segment(segment) -> None
```

### Notes

* It owns `SegmentMeta`.
* It does not decide whether a segment should expire; that is policy logic.
* It performs lifecycle transitions only when instructed.

### State transition rules

Allowed lifecycle progression:

```text
ACTIVE -> SEALED -> EXPIRED -> DELETED
```

A segment should not skip directly from `ACTIVE` to `EXPIRED` unless explicitly supported in the future.

---

## SegmentPolicy

`SegmentPolicy` contains the logic that determines when segments should roll over or expire. This keeps policy separate from storage and metadata handling.

### Responsibilities

* determine if a segment should roll over
* determine if a segment is expired
* allow future extension for other policies such as compression

### Interface

```python
class SegmentPolicy:
    def is_expired(segment) -> bool
    def should_rollover(segment, new_bytes_size) -> bool
```

### Behavior

#### `should_rollover(segment, new_bytes_size) -> bool`

Returns true when appending `new_bytes_size` would require the current active segment to be sealed and replaced. This can be based on:

* max segment size
* max age
* optional future constraints

#### `is_expired(segment) -> bool`

Returns true when a sealed segment is no longer within retention constraints. This can be based on:

* retention time
* retention size
* future compaction rules

### Future extensions

In the future, policy may include:

* compression decisions
* compaction eligibility
* hot/cold storage tiering
* retention by topic class

---

## SegmentMemory

`SegmentMemory` manages runtime access to segments stored on disk. It owns and manages the lifecycle of loaded segment resources such as file handles, mmap objects, and cache entries.

This component is responsible only for **memory residency and resource management**. It does not make decisions about segment lifecycle or retention policies.

Segment metadata (`SegmentMeta`) is owned by `SegmentRegistry`, while the runtime memory representation (`Entry`) is owned by `SegmentMemory`.

---

### Responsibilities

* load segment resources into memory on demand
* maintain reference counts for active users
* cache loaded segments using an LRU policy
* provide safe acquisition and release of segment resources
* evict entries when unused and eligible
* expose handles that provide read and write access to the segment

---

## Interface

```python
class SegmentMemory:
    def acquire(segment_meta) -> SegmentHandle
    def try_evict(expired_segment) -> bool
    async def force_evict(expired_segment) -> None
```

`acquire()` returns a **SegmentHandle**, which provides access to the segment and manages reference counting.

The handle automatically releases the entry when it goes out of scope.

---

## Internal Structure

`SegmentMemory` maintains an internal cache of loaded segments.

```python
_cache_lock: threading.Lock
_cache: Dict[Key, Entry]
```

The cache behaves as an **LRU cache** of loaded segment resources.

Entries are created lazily when a segment is first accessed.

---

## Entry Structure

`Entry` represents the runtime memory representation of a segment.
It is **owned exclusively by `SegmentMemory`**.

```python
class Entry:
    id
    refcount
    mmap
    file_obj
    capacity
    size
    lock: threading.Lock
```

---

## Entry Responsibilities

Each `Entry` is responsible for:

* ensuring the underlying file exists
* maintaining the open file descriptor
* managing the mmap mapping if used
* tracking segment size and capacity
* protecting concurrent access through its lock
* tracking the number of active references

Entries should **never be accessed directly outside `SegmentMemory`**.

---

## SegmentHandle

`SegmentHandle` is a lightweight object returned by `SegmentMemory.acquire()`.

It provides read and write access to the segment and manages reference counting automatically.

The handle behaves as a **context-managed object**.

Example usage:

```python
with segment_memory.acquire(meta) as segment:
    segment.read(offset, size)
```

When the handle exits scope, it automatically releases the segment entry.

### Manual Release

A manual release method may also be available:

```python
segment._release()
```

However, the preferred usage pattern is through the context manager to avoid leaks.

---

## Acquire Flow

`acquire(segment_meta) -> SegmentHandle`

This operation must be **atomic**.

Flow:

1. acquire `_cache_lock`
2. check if the segment entry exists in the cache
3. if not present:

   * load the segment from disk
   * create a new `Entry`
   * insert the entry into the cache
4. increment the entry's `refcount`
5. update the LRU position
6. if the cache exceeds the configured limit:

   * attempt eviction of entries with `refcount == 0`
7. return a `SegmentHandle` referencing the entry

---

## Release Behavior

Release occurs when:

* the handle exits a context block
* `_release()` is called explicitly

Release flow:

1. locate the entry
2. decrement the entry `refcount`
3. update LRU metadata
4. if `refcount == 0`, the entry becomes eligible for eviction

---

## Eviction

Segments can only be evicted when they are not actively in use.

### try_evict

```python
try_evict(expired_segment) -> bool
```

Returns:

* `False` if the entry is still in use (`refcount > 0`)
* `True` if eviction succeeded

Eviction steps may include:

* closing the mmap mapping
* flushing pending writes
* closing the file descriptor
* removing the entry from the cache

---

### force_evict

```python
async force_evict(expired_segment)
```

Asynchronous eviction path used when cleanup must be retried later or executed in the background.

---

## Cleanup Behavior

`SegmentMemory` performs opportunistic cache cleanup.

Cleanup logic:

* inspect cache pressure
* remove least recently used entries
* only evict entries where `refcount == 0`
* ensure entries currently in use remain resident

The goal is to maintain a **bounded memory footprint** while minimizing unnecessary disk reloads.

---

## Key Properties

The following guarantees must hold:

* entries are loaded lazily
* refcounts prevent active segments from being evicted
* `SegmentMeta` remains separate from runtime memory resources
* eviction never occurs while a segment is in use
* resource cleanup happens safely and deterministically

---


## CleanerService

`CleanerService` is intentionally dumb. It should execute routines on a schedule, but it should not own business logic or storage logic.

It knows *when* to run cleanup, not *how to make storage decisions*.

### Responsibilities

* periodically invoke retention evaluation
* periodically attempt eviction of expired segments
* run independently of storage decision logic

### Interface

```python
class CleanerService:
    def start_cleaner() -> None
```

### Design principle

The service should be provided with callable functions by `LogManager`. That way:

* `CleanerService` stays generic
* `LogManager` keeps orchestration control
* cleanup logic remains testable and decoupled

### Periodic loop

Pseudo-flow:

```python
while running:
    expired = apply_retention_policy(now)
    for segment in expired:
        try_evict(segment)
```

Or, with the current orchestration model:

```python
while running:
    segments = get_expired_segments(now)
    for segment in segments:
        cleanup(segment)
```

### Notes

The cleaner does not own objects like registry or memory directly unless explicitly injected. It only executes routines.

---

## Interfaces

## LogManager Interface

```python
class LogManager:
    def __init__(config)

    def read_bytes(topic, offset) -> ReadResultBytes
    def read(topic, offset) -> ReadResult (Data in memoryview)
    def append(topic, msg)
    def load()
    def start_cleaner()
```

### Notes

* multiple cores or threads may read the same segment concurrently
* only one writer per topic should exist
* on restart, `load()` reconstructs in-memory registry state from disk

---

## Registry and Memory Separation

There is an important design distinction here.

### SegmentRegistry

Owns:

* `SegmentMeta`
* segment lifecycle state
* topic-to-segment lookup

Does not own:

* mmap handles
* file objects
* refcounts for loaded memory

### SegmentMemory

Owns:

* pinned/unpinned loaded segments
* refcount
* LRU cache
* open file handles
* mmap-backed entries

Does not own:

* retention policy
* lifecycle policy decisions
* authoritative segment metadata

This separation is important because metadata lifecycle and memory residency are different concerns.

---

## Concurrency Model

## Read concurrency

* multiple readers may access the same segment at the same time
* `SegmentMemory.acquire()` increments refcount
* readers release after completion
* mmapped or file-backed reads can proceed concurrently if protected appropriately

## Write concurrency

* only one writer per topic should write at a time
* this simplifies offset assignment and active segment mutation
* per-topic write serialization is preferred

## Cleaner concurrency

* cleaner may mark segments expired while readers still hold them
* expired does not mean immediately deleted
* `SegmentMemory.try_evict()` must respect refcount
* segment deletion should only happen when no active readers/writers remain

## Cache safety

* cache-wide operations require `_cache_lock`
* per-entry read/write or state changes may require `Entry.lock`
* avoid holding global cache lock longer than needed

---

## Startup and Recovery

On broker restart, the system should reconstruct its in-memory state from disk.

### `load()` responsibilities

* scan topic directories
* discover segment files
* reconstruct `SegmentMeta`
* determine each segment state
* rebuild latest offsets
* restore registry state
* defer actual mmap/open until `SegmentMemory.acquire()` when possible

### Recovery principle

Metadata should be reconstructed cheaply, while actual memory loading should remain lazy.

---

## Typical Operation Flows

## Write flow

```text
Client publish request
    ->
Broker write path
    ->
LogManager.append(topic, msgs)
    ->
SegmentRegistry locates active segment
    ->
SegmentPolicy.should_rollover(segment, new_bytes_size)
    ->
If rollover needed:
    seal current segment
    create new segment
    register new active segment
    ->
SegmentMemory.acquire(segment_meta)
    ->
append messages
    ->
update offsets and sizes
    ->
SegmentMemory.release(segment_meta)
    ->
AppendResult
```

---

## Read flow

```text
Client pull request
    ->
LogManager.read(topic, offset, size)
    ->
SegmentRegistry.get_segment(topic, offset)
    ->
SegmentMemory.acquire(segment_meta)
    ->
segment.read(offset, size)
    ->
SegmentMemory.release(segment_meta)
    ->
BatchResult
```

---

## Cleanup flow

```text
CleanerService tick
    ->
LogManager.get_expired_segments(now)
    ->
SegmentRegistry.list_segments(SEALED)
    ->
SegmentPolicy.is_expired(segment)
    ->
SegmentRegistry.progress_state(segment, EXPIRED)
    ->
LogManager.cleanup(segment)
    ->
SegmentMemory.try_evict(segment)
    ->
if successful:
    SegmentRegistry.remove_segment(segment)
    filesystem delete
```

---

## Suggested Data Types

These are conceptual only and can be refined in implementation.

## SegmentMeta

```python
class SegmentMeta:
    topic: str
    base_offset: int
    max_offset: int
    created_at: float
    last_modified_at: float
    size: int
    capacity: int
    state: SegmentState
    path: str
```

## SegmentState

```python
from enum import Enum

class SegmentState(Enum):
    ACTIVE = "ACTIVE"
    SEALED = "SEALED"
    EXPIRED = "EXPIRED"
    DELETED = "DELETED"
```

## Entry

```python
class Entry:
    id: str
    refcount: int
    mmap: object | None
    file_obj: object | None
    capacity: int
    size: int
    lock: threading.Lock
```

---

## Invariants

The following should always hold:

1. A topic has at most one `ACTIVE` segment at a time.
2. Only `ACTIVE` segments are writable.
3. `SEALED` and `EXPIRED` segments are read-only.
4. A segment cannot be physically deleted while its refcount is greater than zero.
5. `SegmentRegistry` is the source of truth for segment metadata.
6. `SegmentMemory` is the source of truth for loaded resource residency.
7. Cleaner actions must be idempotent where possible.
8. Per-topic writes are serialized.

---

## Future Extensions

This design should support future improvements without major restructuring.

### Possible additions

* compression policy
* compaction policy
* replication-aware metadata hooks
* tiered storage
* checksum validation inside segment reads
* async prefetching for hot segments
* snapshotting registry metadata
* metrics for refcount, cache hits, and evictions

---

## Summary

This refactor proposes a storage subsystem with clean boundaries:

* `LogManager` orchestrates
* `SegmentRegistry` owns metadata and state
* `SegmentPolicy` decides rollover and expiry
* `SegmentMemory` owns loaded resources and eviction
* `CleanerService` periodically triggers cleanup routines

The overall system remains:

* append-only
* replay-capable
* predictable
* concurrency-aware
* easy to extend

This separation should make the log layer easier to reason about, safer under concurrency, and more maintainable as the broker grows.
