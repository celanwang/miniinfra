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

## First comparison

The delivery and format baseline was `af9cb7d`. The measured baseline `df23e6e` adds the experiment and uses the same 30-second schedule as the broker. The candidate `aee60f2` combines directory syncs while staging a checkpoint. All runs used Go 1.27.3 and 8,192 sealed messages of 64 KiB each. The native runs used macOS 15.5 on arm64 and the local APFS volume. The Linux runs used an arm64 OrbStack guest and its container overlay storage on the same host.

Runs were serialized. Native one-minute load averages at the default run boundaries ranged from about 1.5 to 2.1. The host and its storage were shared with background activity. Compare variants within one environment. The default native results below are one run per variant, with three checkpoint intervals per run.

| Native default period | Baseline | Candidate |
| --- | ---: | ---: |
| ACK P99, 1,520 samples per run | 92.9 ms | 87.1 ms |
| Before checkpoint, ACK P99 | 47.9 ms (54) | 60.9 ms (47) |
| During checkpoint, ACK P99 | 178.0 ms (2) | 124.0 ms (1) |
| After checkpoint, ACK P99 | 61.9 ms (56) | 57.9 ms (56) |
| Checkpoint lock hold, median | 128.0 ms | 109.1 ms |
| Checkpoint sync time, median | 63.8 ms | 46.8 ms |
| Sync calls per checkpoint | 9, 11, 11 | 7, 8, 8 |
| Sampled heap peak, whole run | 257.3 MiB | 257.7 MiB |

Each run completed 372 four-message history pages. Reopening before the workload added about 2.56 MiB of live Go heap in both variants. The longest candidate checkpoint held the lock for 132.7 ms. Non-sync work still occupied 62–78 ms per checkpoint in this run.

| Native ACK phase | Lock wait P99 | Lock hold P99 | ACK sync P99 | Sampled heap peak |
| --- | ---: | ---: | ---: | ---: |
| Before | 32.9 → 46.2 ms | 16.0 → 22.0 ms | 14.1 → 20.7 ms | 62.1 → 61.9 MiB |
| During | 160.5 → 109.1 ms | 17.5 → 14.9 ms | 14.9 → 13.8 ms | 152.6 → 152.6 MiB |
| After | 48.0 → 44.2 ms | 18.2 → 17.1 ms | 14.9 → 16.0 ms | 157.6 → 157.7 MiB |

Each component percentile is calculated separately. The phase counts are in the preceding table.

The forced comparison used three runs per cell. Each run completed 192 ACKs, 40 four-message history pages, and ten checkpoints. Values below are medians of the per-run ACK percentiles and the pooled checkpoint hold times.

| Forced workload | Overall ACK P99, baseline → candidate | During-checkpoint ACK P99, baseline → candidate | Checkpoint hold, baseline → candidate |
| --- | ---: | ---: | ---: |
| Native warm | 87.0 → 89.8 ms | 99.8 → 93.0 ms | 60.0 → 57.0 ms |
| Linux warm | 32.5 → 30.5 ms | 39.9 → 31.5 ms | 23.9 → 24.8 ms |
| Linux cold | 29.8 → 33.0 ms | 32.2 → 39.1 ms | 23.9 → 25.1 ms |

The staging change saved one sync per forced checkpoint. The effect on whole-run ACK P99 varied between runs and environments. On Linux, all measured sealed pages were resident at the warm trial starts and none were resident at the cold trial starts. In both cases the guest cache can warm up during the run.

| Linux candidate, default period | Cold start | Warm start |
| --- | ---: | ---: |
| Resident pages at start | 0 / 175,035 | 175,032 / 175,032 |
| Overall ACK P99, 1,520 samples per run | 27.3 ms | 26.4 ms |
| Before checkpoint, ACK P99 | 27.9 ms (43) | 28.2 ms (46) |
| During checkpoint, ACK P99 | 144.8 ms (3) | 142.6 ms (3) |
| After checkpoint, ACK P99 | 19.0 ms (52) | 19.2 ms (52) |
| Checkpoint lock hold, median | 119.5 ms | 132.1 ms |
| Checkpoint sync time, median | 21.4 ms | 26.3 ms |
| Sampled heap peak, whole run | 257.4 MiB | 257.6 MiB |

Both Linux default runs completed 372 history pages and reopened successfully with the final receipt intact. Each used 7, 8, and 8 sync calls across the three checkpoints. Guest one-minute load averages at these run boundaries were about 0.8–1.3. The cache comparison is one run per condition. A steady stream of cold reads, multiple partitions on the same volume, and device power loss need separate validation.

The candidate retains the sync for each new file, the directory barrier before publishing `HEAD`, and the separate barrier that makes the new `HEAD` durable. The process interruption test exits at four points spanning staging and publication, then verifies the retained history, acknowledged receipts, and an outstanding lease. A separate compatibility check opened a partition created by `af9cb7d`, installed a new checkpoint, and reopened it with the older binary.

The next question is how much of the remaining checkpoint hold comes from encoding, file writes, and collection. That split can justify whether to prepare sealed segments outside the state lock.
