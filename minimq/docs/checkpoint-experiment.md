# Checkpoint latency experiment

Run from the MiniMQ Go module with an idle host and a local filesystem. Run variants serially and record the host, filesystem, Go version, and competing workload.

```sh
MINIMQ_CHECKPOINT_EXPERIMENT=default go test -run '^TestCheckpointExperiment$' -count=1 -v
MINIMQ_CHECKPOINT_EXPERIMENT=forced go test -run '^TestCheckpointExperiment$' -count=1 -v
MINIMQ_CHECKPOINT_EXPERIMENT=default MINIMQ_CHECKPOINT_CACHE=cold go test -run '^TestCheckpointExperiment$' -count=1 -v
```

The test makes its own data directory. The default mode uses the same 30-second timer and retry maintenance loop as the broker. At the default settings it publishes 1,488 messages at a target of 16 per second, performs 372 four-message history reads, and confirms 1,520 ordinary deliveries. One subscription pins the backlog while a second grants and acknowledges tasks concurrently. The forced mode publishes 160 messages, performs 40 reads, and checkpoints after every 16 publications. Both modes reopen the partition at the end to verify history and confirm that acknowledged work stays settled.

`MINIMQ_CHECKPOINT_BACKLOG` defaults to 2,048 messages of 64 KiB each. Set it to `8192` to start with 512 MiB of sealed history. `MINIMQ_CHECKPOINT_RATE` sets the target publication rate in default mode. Compare runs at the same rate and backlog. The initial backlog is materialized as a checkpoint outside the timed workload.

On Linux amd64 and arm64, the cold mode evicts only the test's sealed files after the broker has reopened and validated them. It requires at least 99% of those pages to be absent as measured by `mincore`. Later reads can refill the cache. In a VM, host-side and device caches can still serve those reads. Warm runs use the cache left by the recovery scan. Platforms without the Linux page check print zero tracked pages.

ACK phases use the checkpoint's actual lock interval. A `during` ACK waits on that interval. The `before` and `after` buckets cover arrivals within one second of the nearest checkpoint boundary. Each bucket reports sample count, P50, nearest-rank P99, and sampled Go heap peak. Small buckets can report the maximum as P99. Sync time is the time spent inside file and directory `Sync` calls. Heap sampling runs every 50 ms and may miss short spikes.

Up to eight additional deliveries per checkpoint are leased before the timed workload. Their workers submit ACKs when the checkpoint acquires the lock. The test reports these ACKs in separate `probe_*` buckets. These requests compete with ordinary work for the same lock and durable journal. Compare runs made with the same probe workload.

Checkpoint timings distinguish state preparation, JSON encoding, length/CRC frame assembly and its explicit copies, file `Write` calls, file and directory `Sync`, and collection outside `Sync`. The remaining time includes hashing, file management and scheduling. Copying inside the JSON encoder is part of the JSON timing.

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

## Checkpoint stage comparison

The integrated baseline was `c87d9a4`. The measured baseline `747c3f6` adds stage timing and the overlapping ACK probes. Candidate `7b2f6d7` assembles the frame directly in the segment buffer. The comparison used Go 1.27.3, macOS 15.5 arm64 and a local APFS directory. Runs were serial on a shared host and the one-minute load averages around the default runs were about 2–3. The cache was warm after reopen. The cold-cache comparison above predates the new probes.

```sh
GOWORK=off MINIMQ_CHECKPOINT_EXPERIMENT=default MINIMQ_CHECKPOINT_BACKLOG=8192 go test -run '^TestCheckpointExperiment$' -count=1 -v
GOWORK=off MINIMQ_CHECKPOINT_EXPERIMENT=forced MINIMQ_CHECKPOINT_BACKLOG=8192 go test -run '^TestCheckpointExperiment$' -count=1 -v
GOWORK=off go test -run '^$' -bench '^BenchmarkEncodeCheckpointSegment$' -benchtime=8x -count=5
```

Both default runs completed three 30-second periods with 512 MiB of sealed message bodies, 372 history pages, 1,520 ordinary ACKs, and 24 probe ACKs. The stage values are medians across the three checkpoints. The ACK values are per-run percentiles.

| Checkpoint stage | Baseline | Candidate |
| --- | ---: | ---: |
| Wait for state lock | 36.87 ms | 35.95 ms |
| Hold state lock | 121.19 ms | 107.57 ms |
| Copy and prune state | 1.13 ms | 0.77 ms |
| JSON encoding | 42.10 ms | 40.85 ms |
| Frame assembly, including CRC and explicit copies | 19.60 ms | 10.61 ms |
| File writes | 5.45 ms | 5.23 ms |
| File and directory syncs | 42.74 ms | 44.98 ms |
| Collection outside sync | 1.51 ms | 1.48 ms |
| Remaining lock hold | 8.20 ms | 8.05 ms |

| ACK cohort | Baseline P50 / P99 (samples) | Candidate P50 / P99 (samples) |
| --- | ---: | ---: |
| All ordinary | 27.93 / 68.88 ms (1,520) | 26.95 / 55.02 ms (1,520) |
| Before | 26.99 / 48.03 ms (46) | 25.11 / 44.94 ms (46) |
| During | 159.01 / 183.46 ms (2) | 138.01 / 153.45 ms (2) |
| After | 30.01 / 158.82 ms (54) | 35.90 / 139.01 ms (56) |
| Probe during | 195.07 / 279.04 ms (24) | 187.36 / 248.58 ms (24) |

Both runs sampled a heap peak of about 265 MiB and reopened with about 2.56 MiB of additional live heap before the workload. Probe tail time includes queueing behind other durable ACKs. Small during buckets report near-maximum observations. The end-to-end P99 changes above describe one default run per branch.

For the segment microbenchmark, the executable order alternated across five pairs. Each sample sealed a segment of 256 messages of 64 KiB each. Median time went from 10.71 to 9.55 ms per operation, with ranges of 10.60–11.33 and 9.40–9.79 ms. Allocated bytes fell from 92.42 to 69.27 MB per operation. The on-disk length and CRC bytes match the original format. The process interruption suite and a round trip between the `c87d9a4` and candidate binaries verified history, leases and receipts.

The forced runs published 160 messages in each of three runs per binary. With segments of 256 messages, every checkpoint in this control retained its new messages in the short tail and frame assembly measured zero. Each run completed 192 ordinary ACKs, 80 probe ACKs and 40 history pages. All probes overlapped the checkpoints. Ordinary P99 ranges were 133.0–153.7 ms on the baseline and 120.8–142.0 ms on the candidate. Probe P99 ranges were 200.9–214.8 and 190.9–201.9 ms. Differences in this control show the run-to-run variation even without segment assembly.

Keep the in-buffer frame path for its repeatable allocation and encoding benefit. JSON is the largest remaining measured non-sync phase. In the forced baseline, writing repeated short-tail checkpoints took 8.4–27.8 ms in JSON encoding alone. Measuring that repeated work separately from sealed-record encoding is the next constraint to investigate before changing lock boundaries.

## Short tails with ACK-only and sparse traffic

```sh
GOWORK=off MINIMQ_SHORT_TAIL_EXPERIMENT=default go test -run '^TestShortTailCheckpointExperiment$' -count=1 -v
GOWORK=off MINIMQ_SHORT_TAIL_EXPERIMENT=quiet go test -run '^TestShortTailCheckpointExperiment$' -count=1 -v
GOWORK=off MINIMQ_SHORT_TAIL_EXPERIMENT=sparse go test -run '^TestShortTailCheckpointExperiment$' -count=1 -v
```

The setup installs 2,048 sealed messages and a short tail of 128 messages of 64 KiB each. One subscription pins history. A second takes leases via `Pull` and acknowledges them through the normal `Ack` path. The prepared delivery handles release their body buffers before the measured reopen.

The initial checkpoint and reopen run outside the measured workload. Default mode uses the broker's 30-second schedule for three periods with only ACK updates. Quiet mode does the same work across six forced checkpoints. Sparse mode adds one 64 KiB publication before each of six forced checkpoints. The ordinary worker reads across the archive boundary and each checkpoint releases a separate cohort of pregranted ACKs. The test reopens the partition and checks history, durable ACKs and an outstanding lease. `MINIMQ_SHORT_TAIL_ROWS` changes the starting tail size.

The output separates the first image encode from later encodes of overlapping immutable offsets. Repeated bytes count the raw bodies represented again in checkpoint images. Image bytes include JSON and base64 expansion. CPU is process user plus system time over the measured workload. Heap deltas are live Go heap after two garbage collections relative to the empty process before reopen. The raw tail count excludes payloads retained for independent delivery such as scheduled items and dead letters.

The integrated baseline was `6b3c0db`, the measured baseline was `877e362`, and the candidate was `3b67f6c`. The candidate seals an already checkpointed prefix once it holds at least one read batch of body bytes (8 MiB today). A small or newly appended suffix stays inline. It uses the existing segment format and checkpoint protocol. The compatibility check used the older binary to write an inline tail, the candidate to seal its prefix, and the older binary again to read the new segment and suffix and retry the recorded ACKs.

Trials ran serially on macOS 15.5 arm64 with Go 1.27.3 and local APFS. Host one-minute load averages around the two default runs ranged from 2.7 to 5.7. Background work on the shared host may affect timing. The data was warm after reopen. Both native default runs had 24 ordinary ACKs, 24 ACKs that overlapped checkpoints and 24 history reads.

| Default ACK-only workload | Baseline | Candidate |
| --- | ---: | ---: |
| Initial image encoding, before the timed workload | 22.0 ms | 21.6 ms |
| Original bodies encoded again during three checkpoints | 3 times, 24 MiB | 1 time, 8 MiB |
| Written checkpoint image size | 11.31 MiB each | 0.63 MiB each |
| First checkpoint lock hold | 55.2 ms | 53.9 ms |
| Later checkpoints, median lock hold | 42.7 ms | 26.8 ms |
| Later checkpoints, median JSON time | 9.0 ms | 2.4 ms |
| Ordinary ACK P99 | 23.2 ms | 21.5 ms |
| Overlapping probe ACK P99 | 153.1 ms | 166.4 ms |
| Process CPU | 181.9 ms | 198.9 ms |
| Live heap delta after workload and GC | 8.70 MiB | 0.69 MiB |

The candidate encodes the existing 8 MiB once into the new segment and pays one additional file sync in that first checkpoint. The table counts this conversion. The test's `repeated_bytes` counter focuses on image encodes. Final accounted disk use differed by 458 bytes in this default comparison.

The forced controls ran three times per variant in alternating order. Each large-tail run had six checkpoints, 48 ordinary ACKs, 48 overlapping probe ACKs, and 48 history reads.

| Forced control | Baseline | Candidate |
| --- | ---: | ---: |
| Quiet 8 MiB tail, repeated body bytes including conversion | 48 MiB | 8 MiB |
| Quiet 8 MiB tail, later checkpoint lock median | 37.8 ms | 24.5 ms |
| Quiet 8 MiB tail, process CPU median | 175.1 ms | 117.0 ms |
| Sparse 8 MiB tail, repeated body bytes including conversion | 48.94 MiB | 8.94 MiB |
| Sparse 8 MiB tail, later checkpoint lock median | 41.2 ms | 28.0 ms |
| Sparse 8 MiB tail, process CPU median | 200.0 ms | 165.5 ms |
| Sparse publication P99, median of three runs with six publishes each | 14.2 ms | 15.0 ms |
| Quiet 1 MiB tail, repeated body bytes | 6 MiB | 6 MiB |

Final accounted disk use in the large-tail controls rose roughly 1–1.5 KiB. The 1 MiB control created no new segments in either variant. The capacity regression exercises a budget that fits the inline layout and rejects early-segment staging. The checkpoint falls back to the inline layout before any files from the failed attempt are written.

The large-prefix candidate reduced repeated encoding, later lock holds, and live heap under the default cadence. Overlapping ACK P99 and CPU increased in that default trial. The recent-read comparison below determines the final policy.

## Recent reads and delivery

```sh
GOWORK=off MINIMQ_SHORT_TAIL_EXPERIMENT=default MINIMQ_SHORT_TAIL_RECENT=1 go test -run '^TestShortTailCheckpointExperiment$' -count=1 -v
GOWORK=off MINIMQ_SHORT_TAIL_EXPERIMENT=quiet MINIMQ_SHORT_TAIL_RECENT=1 go test -run '^TestShortTailCheckpointExperiment$' -count=1 -v
```

This variant adds a fixed 96-page recent history scan after each checkpoint. Each page reads four 64 KiB messages with the public `Read` method and verifies their offsets and bodies, for 24 MiB per scan. It then performs four actual two-message `Pull` batches and durably acknowledges the deliveries. The setup leaves some recent work unleased for those pulls and keeps a separate outstanding lease to verify recovery. The test retries a receipt from the real pull workload after reopening.

Use `877e362` with the updated `short_tail_experiment_test.go` for the measured baseline, `638f007` for the early-seal candidate, and `2381ec8` for the restored policy. The baseline has the production behavior from `6b3c0db` and includes the checkpoint counters used by the harness. Build the binaries with `GOWORK=off go test -c`, then alternate the order of the variants in quiet mode to compare them on the same filesystem. Default mode uses the 30-second broker schedule.

The quiet trials ran three times per binary in rotating order on macOS 15.5 arm64, Go 1.27.3, and local APFS. They used warm data on a shared host, with one-minute load averages of 2.2–2.6 at trial starts. Each run performed six checkpoints, 576 recent history pages, 24 Pull batches, 84 ordinary ACKs, and 36 overlapping probe ACKs. The history row reports the median and range of the 18 scans per binary. The other rows report the median of the three trials.

| Quiet workload | Baseline | Early seal | Restored |
| --- | ---: | ---: | ---: |
| Recent Read, 96 pages (24 MiB) | 3.27 ms (1.51–7.91) | 61.00 ms (34.24–64.57) | 5.73 ms (1.57–8.13) |
| Pull P99 | 16.02 ms | 15.08 ms | 16.02 ms |
| Ordinary ACK P99 | 15.67 ms | 16.71 ms | 18.89 ms |
| Overlapping probe ACK P99 | 134.20 ms | 126.80 ms | 139.43 ms |
| Later checkpoint lock hold | 49.23 ms | 30.62 ms | 54.52 ms |
| Process CPU | 388.79 ms | 656.08 ms | 497.09 ms |
| Live heap delta after workload and GC | 8.75 MiB | 0.74 MiB | 8.75 MiB |

Early sealing sends recent reads through the sealed-record path, which opens a segment, reads and verifies its frames, and decodes JSON and base64. Inline reads copy the body already in memory. The quiet trial's Pull P99 is the maximum of 24 batches and includes writing and syncing the delivery grant. The measured differences in Pull and ordinary ACK latency are small relative to the run-to-run spread. The repeatable change was in recent history reads.

The default comparison ran once per policy with three 30-second periods, 288 recent history pages, 12 Pull batches, 48 ordinary ACKs, and 24 overlapping probe ACKs per run.

| Default workload | Early seal | Restored |
| --- | ---: | ---: |
| Recent Read, 96 pages (24 MiB), median of three scans | 56.92 ms | 1.95 ms |
| Pull P99 | 27.06 ms | 19.93 ms |
| Ordinary ACK P99 | 32.93 ms | 25.90 ms |
| Overlapping probe ACK P99 | 243.81 ms | 196.60 ms |
| Later checkpoint lock hold, median of the last two | 41.33 ms | 46.43 ms |
| Process CPU | 446.39 ms | 257.84 ms |
| Live heap delta after workload and GC | 0.70 MiB | 8.71 MiB |

The two default runs corroborate the recent-read penalty. The Pull and ACK percentiles reflect small samples in one run per policy. A separate restored sparse run completed six additional publications and verified the resulting history and receipts on reopen.

Keep recent short tails inline and seal by the usual segment size and retention rules. Subscription progress governs task eligibility and retention. A later `Read` can still request a retained offset after every task has been acknowledged. A reader's temporary segment reference covers only its current operation. The existing task and reader state therefore provide no boundary for predicting future historical reads.

The regression test checks the inline layout with an outstanding lease and after all recent tasks settle. It also verifies receipt recovery, normal sealing after additional publication, and an offset gap after retention. The restored binary opened a directory containing an actual early-sealed prefix and an inline suffix written by `69bce55`, and verified history and ACK receipts in that directory.

## Idle checkpoints with retained dead letters

Reuse the on-disk [dead-letter fixture](dead-letter-page-experiment.md). Its two sizes contain 576 and 11,328 letters in total. The checkpoint holds five subscriptions and the letters, and the WAL starts empty. The clock is fixed at the fixture's time during each idle measurement. The benchmark opens the existing partition through normal recovery, then checks that idle rounds leave HEAD byte-for-byte equal and issue zero sync calls.

```sh
idle_checkpoint_dir=$(mktemp -d)
mkdir "$idle_checkpoint_dir/fixture"
GOWORK=off MINIMQ_DEAD_PAGE_SEED="$idle_checkpoint_dir/fixture" go test -run '^TestSeedDeadLetterPageFixture$' -count=1 .
GOWORK=off MINIMQ_IDLE_CHECKPOINT_FIXTURE="$idle_checkpoint_dir/fixture" go test -run '^$' -bench '^BenchmarkIdleCheckpoint$' -benchtime=192x .
cp -R "$idle_checkpoint_dir/fixture" "$idle_checkpoint_dir/active"
GOWORK=off MINIMQ_IDLE_CHECKPOINT_ACTIVE_DIR="$idle_checkpoint_dir/active" go test -run '^TestIdleCheckpointActiveControl$' -count=1 -v .
cp -R "$idle_checkpoint_dir/fixture" "$idle_checkpoint_dir/wire"
GOWORK=off MINIMQ_IDLE_CHECKPOINT_TCP_DIR="$idle_checkpoint_dir/wire" go test -run '^TestIdleCheckpointsOverTCP$' -count=1 -v ./cmd/minimqd
```

Set `MINIMQ_DEAD_PAGE_ROWS=128` when seeding to create the smaller control. Use a fresh copy for each active or TCP run because both perform durable operations during setup. The active control first publishes a checkpoint with a subscription registration in the WAL and all the letters still valid. It then advances the trusted time to the letters' retention boundary with an empty WAL and verifies a new generation, all letters removed, and the registration recovered.

The TCP control uses the production routes on a loopback listener. It publishes, claims and acknowledges a real delivery, then checkpoints it. Each measured phase sends 160 empty Pulls and 160 retries of the prior ACK. They keep the WAL empty. The quiet phase has no concurrent checkpoint call. The comparison phase forces 32 explicit idle Compact calls at 10 ms intervals to sample overlap. It checks that HEAD is byte-identical, then restarts via the normal daemon entry point and rechecks the stored page, ACK receipt and empty Pull.

The measured baseline `0eff974` adds the workload to `c12d6e8`. The lazy-copy candidate was `2adec63`. Runs interleaved separate test binaries on macOS 15.5 arm64, Go 1.27.3 and local APFS. Each timed loopback and kernel run used the shared benchmark lock. One-minute load averages at trial starts ranged from 2.1 to 2.4. There were three runs per version at the larger size and two at the smaller size. Kernel figures are medians of per-run means. TCP figures are medians of per-run percentiles.

| Retained dead letters | Idle lock hold, baseline → lazy copy | Allocated per idle call |
| --- | ---: | ---: |
| 576 | 28.24 → 9.01 µs | about 147,332 → 0 B |
| 11,328 | 447.30 → 159.97 µs | about 2,793,506 → 0 B |

| Active checkpoints, 11,328 starting letters | Prepare, baseline → lazy copy | Entire lock hold, baseline → lazy copy |
| --- | ---: | ---: |
| New WAL registration | 969 → 209 µs | 44.00 → 43.86 ms |
| Expiry with an empty WAL | 708 → 855 µs | 20.83 → 20.87 ms |

Both active controls issued six syncs per checkpoint. The large image was 13,398,705 bytes with the retained letters and 1,240 bytes after removal, before the checksum. In the small fixture, the active WAL control measured 28.11 → 30.35 ms of lock hold and 20.39 → 22.26 ms of sync time. The smaller sample includes storage variation.

| TCP, 11,328 retained letters | Baseline P50 / P99 | Lazy copy P50 / P99 |
| --- | ---: | ---: |
| Quiet empty Pull | 145 / 496 µs | 158 / 691 µs |
| Quiet repeated ACK | 145 / 416 µs | 157 / 505 µs |
| Empty Pull during forced idle calls | 225 / 3,012 µs | 347 / 1,352 µs |
| Repeated ACK during forced idle calls | 209 / 2,656 µs | 348 / 801 µs |

The forced overlap lowered P99 and raised P50 for both endpoints. These TCP measurements describe the artificial high-frequency overlap. The lock hold and allocation measurements describe individual Compact calls. The normal daemon schedules one call every 30 seconds.

Keep the lazy copy. Each metadata map is scanned once and shared while it needs no removals. The first actual expiry clones that map for the image, and deletions affect only the copy before HEAD is durable. The normal checkpoint path still evaluates WAL contents, tail sealing, receipt and dead-letter expiry, subscription floors and active reader pins. The benchmark's zero allocations apply to this fixture with no retained history segments. Checkpoints can still copy segment metadata and encode live state when publishing.

The boundary tests cover new WAL contents, expiry exactly at the cutoff and a capacity failure before publication, scheduled receipts with due items, an outstanding redrive after receipt cleanup, full and aged tails with an empty WAL, and history retention across an active read. The baseline fixture opens with the new implementation. The original `c12d6e8` binary also reopened the candidate's active control after it pruned the expired letters, and recovered the registration from the prior active generation.

```sh
idle_old_dir=$(mktemp -d)
git -C .. archive c12d6e8 minimq | tar -xf - -C "$idle_old_dir"
cp idle_checkpoint_test.go "$idle_old_dir/minimq/"
(cd "$idle_old_dir/minimq" && GOWORK=off MINIMQ_IDLE_CHECKPOINT_VERIFY_DIR="$idle_checkpoint_dir/active" go test -run '^TestVerifyIdleCheckpointActiveControl$' -count=1 -v .)
```

## Retiring tail segments

The fixture publishes 32 messages of 64 KiB through the Go API and checkpoints them as an inline tail. Every trial opens a fresh copy with an empty WAL. The expired controls advance the trusted lower bound to the retention boundary and use segment widths of 256 and 32. The recent control opens the same data with width 32 before expiry. The protected control publishes an identical workload with a subscription that still needs the messages and opens at the expiry boundary.

```sh
retiring_tail_dir=$(mktemp -d)
mkdir "$retiring_tail_dir/ordinary" "$retiring_tail_dir/protected"
GOWORK=off MINIMQ_TAIL_RETIRE_SEED="$retiring_tail_dir/ordinary" go test -run '^TestSeedRetiringTailCheckpoint$' -count=1 .
GOWORK=off MINIMQ_TAIL_RETIRE_SEED="$retiring_tail_dir/protected" MINIMQ_TAIL_RETIRE_PROTECT=1 go test -run '^TestSeedRetiringTailCheckpoint$' -count=1 .
GOWORK=off MINIMQ_TAIL_RETIRE_FIXTURE="$retiring_tail_dir/ordinary" MINIMQ_TAIL_RETIRE_PROTECTED_FIXTURE="$retiring_tail_dir/protected" go test -run '^$' -bench '^BenchmarkRetiringTailCheckpoint$' -benchtime=6x .
GOWORK=off go test -run '^TestRetiringTailCheckpointOverTCP$' -count=1 -v ./cmd/minimqd
```

The timing harness compiles separate test binaries at the measured baseline `5ebb7f5` and candidate `bcde9fd`. It runs each with `-test.run '^$' -test.bench '^BenchmarkRetiringTailCheckpoint$' -test.benchtime=6x` using the same directories above. Four alternating runs per version use an arm64 macOS 15.5 host, Go 1.27.3 and local APFS. The runs used the shared benchmark lock and started at one-minute load averages around 4.5–4.8. Fixture copying and recovery are outside the measured interval. Each table cell is a median of per-run means.

| Empty WAL and 32 messages | Allocation, baseline → candidate | JSON encoding | Framing | Entire lock hold |
| --- | ---: | ---: | ---: | ---: |
| Expired partial segment | 8,861,952 → 61,537 B | 1.24 → 0.055 ms | 0.82 → 0 ms | 24.50 → 21.99 ms |
| Expired full segment | 8,843,363 → 61,408 B | 1.30 → 0.058 ms | 0.94 → 0 ms | 25.01 → 22.75 ms |
| Recent full segment | 8.87 → 8.87 MB | 1.32 → 1.36 ms | 0.93 → 0.96 ms | 30.21 → 30.98 ms |
| Expired partial with a protecting subscription | 8.90 → 8.89 MB | 1.36 → 1.32 ms | 0.92 → 0.89 ms | 31.13 → 30.53 ms |

JSON encoding includes the image. Both expired controls produce a 9,333-byte image before the checksum and stage zero segments under both policies. Their sync times remain roughly 21 ms. Both retained controls stage one segment under each policy. The recent full-segment sync median increased from 26.42 to 27.36 ms in these trials, alongside the smaller increase in total lock hold.

Keep the deferred encoding. The checkpoint builds the same segment descriptors and decides the removable prefix once, using the subscription floors, active reader pin, and retention boundary. It encodes only retained new segments. When a segment blocks prefix removal, the checkpoint keeps it and every later segment. A reader that releases its pin after prefix selection can leave reclamation to the next round. The metadata is committed with the existing publication sequence and capacity accounting.

The regression checks the instant before and at the retention boundary, a mixed sealed and inline suffix behind a retired prefix, an older live segment blocking a later expired tail, and separate capacity and staging failures. After the failed staging, reopening the original HEAD recovers every old body from the checkpoint and WAL. Existing active read tests exercise the pin, and process interruption tests exercise checkpoint sync boundaries.

The TCP test publishes five messages through the production routes, verifies the retired prefix returns HTTP 410 with first offset 2, and checks all remaining bodies through the normal daemon after reopening. The original and candidate independently produced byte-identical HEAD, checkpoint, journal and retained segment files for this fixture. Each broker also reopened the other's output and checked it through TCP. To check the candidate-written directory with the original broker, save the fixture and use the same test driver in an archive of the old code.

```sh
mkdir "$retiring_tail_dir/wire"
GOWORK=off MINIMQ_TAIL_RETIRE_TCP_DIR="$retiring_tail_dir/wire" go test -run '^TestRetiringTailCheckpointOverTCP$' -count=1 -v ./cmd/minimqd
retiring_tail_old=$(mktemp -d)
git -C .. archive dd3bac8 minimq | tar -xf - -C "$retiring_tail_old"
cp cmd/minimqd/retiring_tail_test.go "$retiring_tail_old/minimq/cmd/minimqd/"
(cd "$retiring_tail_old/minimq" && GOWORK=off MINIMQ_TAIL_RETIRE_TCP_VERIFY_DIR="$retiring_tail_dir/wire" go test -run '^TestReadRetiredTailWithBroker$' -count=1 -v ./cmd/minimqd)
```
