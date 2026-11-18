# MiniMQ

MiniMQ currently runs one durable partition. Start the HTTP service from the `minimq/` directory.

```sh
go run ./cmd/minimqd -dir ./minimq-data
```

The development service listens on loopback by default. The protocol uses JSON, with base64 for message bodies, RFC 3339 timestamps for absolute times, and nanoseconds for subscription durations.

Pass settings as flags. The daemon reserves `-listen` before opening `-dir` and starts serving HTTP after recovery. Use `SIGINT` or `SIGTERM` to stop it and wait for the process to exit before restarting on the same address and data directory. HTTP shutdown gives active requests up to five seconds to finish before closing their connections. Maintenance stops before the partition closes.

Same-disk restart recovers committed history and retained receipts, including after an abrupt process exit. Recovery assumes the filesystem preserves the committed files. If a stop interrupts a reply, use the status and retry rules below.

```sh
curl -fsS localhost:7400/v1/subscriptions/register -d '{"Subscription":"notify"}'
curl -fsS localhost:7400/v1/publish -d '{"ID":"order-731-shipped","Body":"c2hpcHBlZA=="}'
curl -fsS localhost:7400/v1/history/read -d '{"Offset":0,"Limit":{"Messages":10,"Bytes":4096}}'
curl -fsS localhost:7400/v1/deliveries/pull -d '{"Subscription":"notify","Worker":"mailer","Limit":{"Messages":10,"Bytes":4096}}'
```

Send the returned `Instance` and `Token` to `POST /v1/deliveries/ack` along with the same `Subscription` and `Worker`. Send those fields and a `Reason` to `/v1/deliveries/fail` to report a failed attempt. A `DeliverAt` on publish schedules the first release. `POST /v1/publish/status` takes an `ID` and returns its visibility and offset.

Treat a lost Pull reply as an unknown outcome. The broker may already have committed a lease, so the next Pull can return an empty batch. If attempts remain, the task becomes eligible after the lease deadline and retry delay. A new delivery carries the same `Instance`, a higher `Attempt`, and a new `Token`. Use that token to settle the new attempt.

If an ACK reply is lost, retry the same `Subscription`, `Worker`, `Instance` and `Token`. While the receipt is retained, `previously_accepted` confirms that the broker already committed the ACK.

Use `POST /v1/dead-letters/list` with `Subscription` and `Limit` to inspect failures. Continue a listing using the last ID as `After`. To requeue a letter, send `Subscription`, `DeadLetterID`, and a unique `OperationID` to `POST /v1/dead-letters/redrive`. Reuse that operation ID if the outcome is unknown. `GET /v1/status` reports the retained offset range, accounted storage, and pending work.

The service runs bounded scheduling and recovery maintenance in the background. The Go package exposes these operations as `AdvanceScheduled`, `AdvanceRetries`, and `Compact` for callers that manage the partition directly.

```sh
go test -race ./...
```

See the [design](docs/plan/design.md) for failure guarantees and the [recovery notes](docs/plan/recovery.md) for the clock contract and replication plan.

The [checkpoint experiment](docs/checkpoint-experiment.md) covers repeatable backlog and history workloads under the broker's default maintenance cycle.

The [scheduled release experiment](docs/scheduling-experiment.md) covers simultaneous deadlines, foreground traffic, and broker restarts.

The [lease expiration experiment](docs/retry-experiment.md) measures lock contention during retry and dead-letter batches.

The [dead-letter page experiment](docs/dead-letter-page-experiment.md) measures cursor pages and concurrent delivery traffic.

## Source layout

- Import `github.com/celanwang/miniinfra/minimq` and call `Open` to use a `Partition` directly. `partition.go` contains startup and the transaction logic shared by commits and replay. `history.go` handles page reads and segment references. `checkpoint.go` restores checkpoints and reclaims eligible history.
- `internal/journal` handles WAL framing, the committed `HEAD`, and generation files.
- `cmd/minimqd` serves the HTTP API and runs partition maintenance.

For historical performance comparisons, build the experiment from the `minimq/` checkout at the revision named in the experiment notes.
