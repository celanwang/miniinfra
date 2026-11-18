# Checkpoint latency experiment

Run from the MiniMQ Go module with an idle host and a local filesystem. Run variants serially and record the host, filesystem, Go version, and competing workload.

```sh
MINIMQ_CHECKPOINT_EXPERIMENT=default go test -run '^TestCheckpointExperiment$' -count=1 -v
MINIMQ_CHECKPOINT_EXPERIMENT=forced go test -run '^TestCheckpointExperiment$' -count=1 -v
MINIMQ_CHECKPOINT_EXPERIMENT=default MINIMQ_CHECKPOINT_CACHE=cold go test -run '^TestCheckpointExperiment$' -count=1 -v
```

The test makes its own data directory. The default mode uses the same 30-second timer and retry maintenance loop as the broker. At the default settings it publishes 1,488 messages at a target of 16 per second, performs 372 four-message history reads, and confirms 1,520 deliveries. One subscription pins the backlog while a second grants and acknowledges tasks concurrently. The forced mode publishes 160 messages, performs 40 reads, and checkpoints after every 16 publications. Both modes reopen the partition at the end to verify history and confirm that acknowledged work stays settled.

`MINIMQ_CHECKPOINT_BACKLOG` defaults to 2,048 messages of 64 KiB each. Set it to `8192` to start with 512 MiB of sealed history. `MINIMQ_CHECKPOINT_RATE` sets the target publication rate in default mode. Compare runs at the same rate and backlog. The initial backlog is materialized as a checkpoint outside the timed workload.

On Linux amd64 and arm64, the cold mode evicts only the test's sealed files after the broker has reopened and validated them. It requires at least 99% of those pages to be absent as measured by `mincore`. Later reads can refill the cache. In a VM, host-side and device caches can still serve those reads. Warm runs use the cache left by the recovery scan. Platforms without the Linux page check print zero tracked pages.

ACK phases use the checkpoint's actual lock interval. A `during` ACK waits on that interval. The `before` and `after` buckets cover arrivals within one second of the nearest checkpoint boundary. Each bucket reports sample count, nearest-rank P99, and sampled Go heap peak. Small buckets can report the maximum as P99. Sync time is the time spent inside file and directory `Sync` calls. Heap sampling runs every 50 ms and may miss short spikes.
