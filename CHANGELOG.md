# Changelog

Component changes are listed newest first as they land on `main`.

## Unreleased

- MiniMQ reads sealed message bodies from disk on demand and protects active history reads during segment reclamation.
- MiniDB reclaims obsolete row and index versions online and coalesces commits queued during a WAL flush.
- MiniKV documents latency and memory use under mixed workloads with a reproducible Redis comparison.
- MiniMQ supports durable single-partition messaging with delayed delivery, acknowledgments, and dead-letter redrive.
- MiniDB supports concurrent Read Committed transactions and recovers committed data from checkpoints and the write-ahead log.
- MiniKV serves a sharded in-memory cache over TCP with per-key expiry and bounded eviction.
