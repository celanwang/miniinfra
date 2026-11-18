# Changelog

Component changes are listed newest first as they land on `main`.

## Unreleased

- MiniMQ orders only the earliest batch of expired leases while preserving retry and dead-letter delivery order.
- MiniDB prefers fully bound non-null unique indexes for equality lookups, reducing waits on tenant-scoped queries.
- MiniKV verifies ordered pipelined responses and documents the latency tradeoff for small page reads.
- MiniDB copies only matching rows during single-table queries, reducing allocations and writer waits on selective scans.
- MiniKV verifies large GET replies under TCP backpressure and records the latency cost of vector writes.
- MiniMQ releases one extra bounded batch between maintenance ticks to shorten waits during scheduled delivery bursts.
- MiniDB stops canceled row scans at bounded intervals so writers can regain the table lock promptly.
- MiniKV documents when a near-full shard can accept a 64 KiB refill after keys expire.
- MiniDB releases locks promptly when its Go client cancels an in-flight request and preserves final replies after a TCP half-close.
- MiniKV verifies TTL visibility, overwrite safety, and conditional refills during clustered expirations.
- MiniMQ verifies that recent short tails stay inline for fast history reads across checkpoints and restarts.
- MiniDB measures foreground lock waits during version reclamation under concurrent reads and writes.
- MiniKV documents its memory limit under mixed-size writes and verifies successful and rejected requests in one workload.
- MiniKV replaces idle buffers when write sizes change so near-full caches can reuse memory across workloads.
- MiniDB reclaims row and index versions between active statement snapshots during long-running queries.
- MiniMQ assembles message frames directly in sealed segments to reduce checkpoint copying and allocations.
- MiniKV measures same-shard and cross-shard read latency during large overwrites.
- MiniMQ batches directory syncs for new checkpoint files before publishing the durable checkpoint head.
- MiniDB shares its MVCC cleanup budget across tables so quiet tables do not delay reclamation on busy ones.
- MiniKV reuses receive buffers for nearby large write sizes to reduce memory pressure when a full cache rejects writes.
- MiniMQ reads sealed message bodies from disk on demand and protects active history reads during segment reclamation.
- MiniDB reclaims obsolete row and index versions online and coalesces commits queued during a WAL flush.
- MiniKV documents latency and memory use under mixed workloads with a reproducible Redis comparison.
- MiniMQ supports durable single-partition messaging with delayed delivery, acknowledgments, and dead-letter redrive.
- MiniDB supports concurrent Read Committed transactions and recovers committed data from checkpoints and the write-ahead log.
- MiniKV serves a sharded in-memory cache over TCP with per-key expiry and bounded eviction.
