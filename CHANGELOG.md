# Changelog

Component changes are listed newest first as they land on `main`.

## Unreleased

- MiniMQ supports durable single-partition messaging with delayed delivery, acknowledgments, and dead-letter redrive.
- MiniDB supports concurrent Read Committed transactions and recovers committed data from checkpoints and the write-ahead log.
- MiniKV serves a sharded in-memory cache over TCP with per-key expiry and bounded eviction.
