Replication design

This document defines the leader/follower behaviour and the `ReplicaManager` which manages replication for all topics/partitions assigned to a broker. It captures the invariants the cluster must maintain and the public responsibilities / API of the ReplicaManager.

Core concepts and invariants

- LEO (Log End Offset): current write end offset on the leader for a topic/partition.
- Replica offset: the last offset that a replica (follower or leader) has successfully fetched and applied.
- ISR (In-Sync Replicas): the set of replicas that are considered "in-sync" with the leader.
    - A replica is in the ISR when it has recently caught up to within configured bounds of leader LEO.
    - A replica may be removed from ISR if it falls behind (lag threshold) or becomes unresponsive.
- HW (High Watermark): the largest offset such that all replicas in ISR have acknowledged at least that offset.
    - Only offsets <= HW are considered committed and visible to consumers.
    - HW monotonicity: HW must never decrease.

Behavioural rules

- Consumers can only read up to the current HW. This guarantees they only see committed data.
- Followers are allowed to fetch beyond HW and may read data that is not yet committed (used for replication/catching up).
- When the replica set of ISR shrinks below configured minimum (min.insync.replicas), leader behavior may change (for example, reject produce with acks=-1 if configured) — this policy is controlled by broker-level configuration.


High-level responsibilities (summary):
- Maintain per-topic replication state (LEO, HW, ISR, replica offsets, fetchers) for partitions hosted on this broker.
- Reconcile and apply topic metadata received from the controller (leader id, replica set, partition assignments) via `apply_metadata` / `apply_metadata_list`.
- Track per-topic LEO via `LogManager` and start leader/fetcher components as role changes.
- Periodically recompute ISR (leader loop) and serve replica fetch requests when leader.
ReplicaManager overview

The repository's current `ReplicaManager` (see `broker/src/pylogstream_broker/replication/manager.py`) is implemented as a broker-level manager that maintains per-topic `TopicState` for every topic/partition assigned to this broker. It receives TopicMetaData updates from the controller and exposes a small set of broker-facing APIs.

High-level responsibilities (as implemented):

- Keep a mapping `topics: dict[topic, TopicState]` containing per-topic replication state.
- Apply controller metadata updates via `apply_metadata` (single topic) and `apply_metadata_list` (batch).
- Start and stop per-topic leaders (`ReplicaLeader`) or fetchers (`ReplicaFetcher`) depending on role.
- Periodically recompute ISR via a leader loop (calls `ReplicaLeader._update_isr()` at configured intervals).
- Route replication requests through `handle(cmd: ReplicaCommand) -> ReplicaResponse` to the appropriate TopicState leader.

Broker-level API (actual implemented methods)

- apply_metadata(meta: TopicMetaDataResponse) -> None
    - Called by controller/network layer with `TopicMetaDataResponse` to create/upgrade/downgrade topic state.
- apply_metadata_list(meta_list: TopicMetaDataListResponse) -> None
    - Batch apply of topic metadata; removes topics no longer assigned and applies updates to existing topics.
- handle(cmd: ReplicaCommand) -> ReplicaResponse
    - Generic entrypoint used by the broker network layer to route replication commands (e.g., ReplicaFetchCommand). The manager dispatches to the topic's leader handler; it raises if the topic is not found or the broker is not the leader.
- get_leader(topic: str) -> ReplicaLeader
    - Return the per-topic `ReplicaLeader` instance (raises if the topic doesn't exist or this broker is not leader).
- close(topic: str), close_all()
    - Lifecycle helpers to stop per-topic state and background tasks.

Internally the manager also provides `_start_topic`, `_close_state`, and `_remove_topic` helpers used during metadata application and shutdown.


Internal state (per-broker and per-topic)

- broker_id: str
- topics: dict[topic, TopicReplicaState]
    - TopicReplicaState fields:
        - leader_id: str
        - replicas: dict[replica_id, ReplicaMetadata]
            - ReplicaMetadata: {last_offset: int, last_heartbeat_ms: int, last_caught_up_ms: int, is_in_isr: bool}
        - is_leader: bool (derived from broker_id == leader_id)
        - leo: int (mirror of LogManager's LEO for that topic)
        - hw: int
        - fetcher_task: Optional[Task] (if follower)

The manager globally schedules periodic reconciliation tasks to recompute ISR/HW and to garbage collect stale topic state.

Leader behaviour

When a given TopicReplicaState in the ReplicaManager is leader for its partition:

1. Accept writes via the partition's LogManager and update the partition LEO.
2. Track acknowledgements from replicas for that partition (via mark_replica_heartbeat or network messages). For each follower update, update that replica's last_offset and last_heartbeat_ms.
3. Recompute ISR and HW on events:
    - After a replica heartbeat/acknowledgement
    - Periodically (background task) to remove stale replicas
    - On controller-provided metadata changes (leader or replica-list updates)

ISR update rules (summary):
- A replica stays/enters ISR if it has fetched up to near LEO (within configured thresholds) and its last_caught_up_ms is recent (current_time - last_caught_up_ms <= replica.lag.time.max.ms).
- A replica is removed when it is too stale or unresponsive.
- If ISR size < min.insync.replicas the leader will apply configured safety rules (for example, change produce acceptance semantics).

HW calculation:

- Compute HW as the minimum of last_offsets of all replicas currently in ISR.
- Ensure HW is monotonic: hw = max(hw, computed_min).

Serving fetch requests (leader):

- ReplicaManager.handle(ReplicaFetchCommand) -> ReplicaResponse
    - Accept parameters: topic, offset, size, replica_id
    - Read bytes from local LogManager starting at offset up to size for the requested topic/partition
    - Return payload and current LEO (so follower knows leader LEO)
    - This call is routed through the broker network layer to the per-broker ReplicaManager which dispatches to the correct TopicReplicaState.


Follower behaviour

When the TopicReplicaState is a follower:

1. ReplicaManager starts and manages a ReplicaFetcher worker (per-topic/partition) that periodically issues ReplicaFetchCommand requests to the current leader.
2. ReplicaFetcher writes fetched records to local storage (LogManager) and updates the local offset in the TopicReplicaState.
3. After applying data, the fetcher (or manager network path) reports its offset/heartbeat back to the leader (via the broker network layer); this enables the leader to adjust ISR / HW.

ReplicaFetcher responsibilities (high-level):
- Pull batch from leader: (offset, max_size) -> (bytes, leader_leo)
- Append to local log, update local offset
- Optionally commit last fetched offset (depending on local design)
- Report progress/heartbeat to leader


APIs / commands

- ReplicaFetchCommand(topic, offset, size, replica_id)
    - Request: leader returns contiguous bytes and leader LEO
- ReplicaFetchResponse(...) variants: bytes/mmap/file depending on transport
- apply_metadata(meta: TopicMetaDataResponse) -> None
    - Controller -> Broker: push updated topic/partition metadata (leader id, replica list, etc.). The `ReplicaManager.apply_metadata` method in the code applies this metadata and starts/stops per-topic leaders/fetchers as appropriate.
- apply_metadata_list(meta_list: TopicMetaDataListResponse) -> None
    - Batch metadata apply (used to reconcile the full set of topics assigned to this broker).
- ReplicaManager.handle(cmd: ReplicaCommand) -> ReplicaResponse
    - Generic broker-level entrypoint used by the network layer to route replication commands to the appropriate per-topic leader; the manager raises if the topic is not found or the node is not leader for that topic.

Edge cases and implementation notes

- Leader failover: when leader changes, the new leader must reconcile offsets, re-evaluate ISR, and compute HW before allowing consumers to read.
- Slow followers: to avoid oscillation, use hysteresis when re-adding replicas to ISR (e.g., require stable caught-up time for a short window).
- Durable state: any ISR / HW metadata that must survive restart should be persisted or derivable from logs on startup.
- Safety on rolling changes: when ISR temporarily becomes small, expose config hooks (or metrics) so controller code can react (e.g., stop accepting acks=-1 produces).