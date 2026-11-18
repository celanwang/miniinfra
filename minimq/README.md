# MiniMQ

MiniMQ currently runs one durable partition. Use the Go package directly or start the HTTP service.

```sh
go run ./cmd/minimqd -dir ./minimq-data
```

The development service listens on loopback by default. The protocol uses JSON, with base64 for message bodies, RFC 3339 timestamps for absolute times, and nanoseconds for subscription durations.

```sh
curl -fsS localhost:7400/v1/subscriptions/register -d '{"Subscription":"notify"}'
curl -fsS localhost:7400/v1/publish -d '{"ID":"order-731-shipped","Body":"c2hpcHBlZA=="}'
curl -fsS localhost:7400/v1/history/read -d '{"Offset":0,"Limit":{"Messages":10,"Bytes":4096}}'
curl -fsS localhost:7400/v1/deliveries/pull -d '{"Subscription":"notify","Worker":"mailer","Limit":{"Messages":10,"Bytes":4096}}'
```

Send the returned `Instance` and `Token` to `POST /v1/deliveries/ack` along with the same `Subscription` and `Worker`. Send those fields and a `Reason` to `/v1/deliveries/fail` to report a failed attempt. A `DeliverAt` on publish schedules the first release. `POST /v1/publish/status` takes an `ID` and returns its visibility and offset.

Use `POST /v1/dead-letters/list` with `Subscription` and `Limit` to inspect failures. Continue a listing using the last ID as `After`. To requeue a letter, send `Subscription`, `DeadLetterID`, and a unique `OperationID` to `POST /v1/dead-letters/redrive`. Reuse that operation ID if the outcome is unknown. `GET /v1/status` reports the retained offset range, accounted storage, and pending work.

The service runs bounded scheduling and recovery maintenance in the background. The Go package exposes these operations as `AdvanceScheduled`, `AdvanceRetries`, and `Compact` for callers that manage the partition directly.

```sh
go test -race ./...
```

See the [design](docs/plan/design.md) for failure guarantees and the [recovery notes](docs/plan/recovery.md) for the clock contract and replication plan.

The [checkpoint experiment](docs/checkpoint-experiment.md) covers repeatable backlog and history workloads under the broker's default maintenance cycle.

The [scheduled release experiment](docs/scheduling-experiment.md) covers simultaneous deadlines, foreground traffic, and broker restarts.

The [lease expiration experiment](docs/retry-experiment.md) measures lock contention during retry and dead-letter batches.
