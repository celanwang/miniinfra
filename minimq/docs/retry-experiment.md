# Lease expiration experiment

Run from the MiniMQ module, serially on a local filesystem.

```sh
GOWORK=off MINIMQ_RETRY_EXPERIMENT=live go test -run '^TestRetryExperiment$' -count=1 -v .
GOWORK=off MINIMQ_RETRY_EXPERIMENT=expired go test -run '^TestRetryExperiment$' -count=1 -v .
GOWORK=off MINIMQ_RETRY_EXPERIMENT=expired MINIMQ_RETRY_DEADLINES=staggered go test -run '^TestRetryExperiment$' -count=1 -v .
```

`MINIMQ_RETRY_ROWS` defaults to 8,192. Use `32768` for the large control. The test reuses the checkpoint experiment's sealed history fixture, then grants leases through real pulls. The slow subscription holds one lease for every 256-byte record. The final-attempt subscription holds 144 leases for 64 KiB sealed records. The first expiration batch for those final attempts fills the 8 MiB limit at 128 records. The next can include the remaining 16 final attempts and ordinary retries. An ordinary subscription is registered after the fixture and consumes its own 48 records while maintenance runs.

Setup and reopen run outside measurement. In `live` the trusted clock still precedes the cohort deadlines. In `expired` it has advanced past the deadlines. The tied control grants the leased cohort with one clock value. The staggered control puts 1 ms between slow-subscription grants. Each real pull still shares a deadline across its batch of up to 256 records. The timed part runs six normal 250 ms maintenance ticks alongside 24 ordinary Pulls and 48 ACKs. The controls start warm after recovery and finish before a checkpoint.

The retry log separates scanning, ordering, reading and decoding sealed bodies for dead letters, other transaction preparation, and commit. Sync time is included within commit. In the candidate, scan time also includes computing each eligible instance ID once, while ordering includes every sort of the bounded candidate list. The ordinary log separates time waiting for the partition lock from time holding it. An operation that arrives during a retry lock hold reports the overlap with that batch. Per-run P99 is the maximum for cohorts of 24 or 48 requests, so compare repeated runs. The live control also samples five empty polls of the busy subscription.

After the timed phase, the expired control drains the remaining work through normal maintenance calls, rejects an expired token, and confirms a new attempt. Reopen verifies the ordinary ACK receipt and checks each persisted dead letter for uniqueness and its original payload. The live control confirms an original lease that was still valid and verifies an empty dead-letter list.

The measured baseline is `4ffd90e`, which adds timing and the fixture to production behavior from `59c33e4`. The candidate is `040a13e`. To alternate timing runs, compile both with `GOWORK=off go test -c -o /tmp/minimq-retry.test .` using a different output path for each binary. Then invoke each binary with the workload environment variables, `-test.run '^TestRetryExperiment$' -test.v`.

## Bounded ordering

A transaction can only finish the earliest prefix by deadline, subscription name, and textual instance ID. Each count limit is known before the scan. The candidate keeps the earliest `limit.Messages` whenever the local collection grows to twice that size. At most 512 candidates are buffered for a 256-message batch, and each eligible candidate's textual instance ID is computed once. Removing later candidates cannot change the chosen prefix. A byte limit can still stop that prefix early. The remaining tasks keep their existing leases for the next call.

The scan still visits the relevant task records. The collection exists only during one expiry call, including the subscription-scoped expiry inside Pull.

## Results

Runs were serial on macOS 15.5 arm64, Go 1.27.3, and local APFS. The shared host's one-minute load averages at trial starts ranged from 1.6 to 3.7. The 8,192-row expired controls ran three times per version and deadline shape, interleaving the binaries. The live control ran twice per version. Background results are medians over the indicated batches. Foreground results are medians of per-run percentiles.

| 8,192 expired retry leases plus the 144 final attempts | Tied, baseline → candidate | Staggered, baseline → candidate |
| --- | ---: | ---: |
| Pure retry scan and order, 12 batches per version | 18.91 → 8.78 ms | 17.62 → 9.34 ms |
| Pure retry lock hold | 35.46 → 24.84 ms | 32.24 → 24.85 ms |
| Pure retry commit, including sync | 14.86 → 15.59 ms | 14.74 → 14.93 ms |
| First 8 MiB dead-letter batch, scan and order | 23.99 → 9.67 ms | 19.77 → 10.32 ms |
| First dead-letter batch, sealed body read and decode | 17.51 → 22.13 ms | 19.11 → 22.85 ms |
| First dead-letter batch, commit / sync within commit | 26.46 / 15.78 → 28.66 / 16.61 ms | 28.50 / 16.26 → 28.10 / 16.41 ms |
| First dead-letter batch, lock hold | 70.11 → 59.45 ms | 67.37 → 59.40 ms |
| Ordinary Pull P99 | 67.48 → 59.75 ms | 63.60 → 54.21 ms |
| Ordinary Pull lock wait P99 | 54.12 → 45.03 ms | 49.70 → 41.41 ms |
| Ordinary ACK P99 | 18.02 → 41.78 ms | 20.86 → 37.95 ms |

In the 8,192-row expired controls, zero to two ordinary ACKs per trial arrived inside a background retry lock hold. Their P99 values varied with phase and sync latency. In the tied burst the per-run ACK P99 ranges were 15.99–77.04 ms at baseline and 17.75–52.16 ms with the candidate. The data support a shorter retry lock interval and a reduction in the observed Pull wait. They are insufficient to establish the change in aggregate ACK P99.

When the 8,336 starting leases were still valid, the background lock hold median was 0.56 ms at baseline and 0.54 ms with the candidate. Nothing was committed. The busy subscription's five empty polls produced per-run P99s of 1.55–2.21 ms at baseline and 1.75–1.81 ms after the change. That path also walks the subscription's delivery window.

The larger control ran once per condition and version. With 32,768 retry leases and staggered grants, the four pure retry batches had a median scan plus ordering time of 50.39 → 15.58 ms and a lock hold of 74.49 → 28.11 ms. Sync alone accounted for 19.33 → 11.76 ms, so the full hold difference includes storage variation. When the same leases were still valid, the six scans held the lock for 0.64–2.75 ms at baseline and 0.36–2.16 ms with the candidate. The empty busy Pull had a 7.07 and 4.39 ms per-run P99 respectively. These single pairs give scale references. The smaller alternating controls establish the repeatable ordering improvement.

Keep the bounded selection. Ordering a small durable prefix saves work in both expiry bursts. The first large dead-letter conversion still spends most of its lock time reading sealed bodies and committing the resulting transaction. The live measurements support retaining the existing scan at this scale. These trials cover warm segments, one partition, and a finite burst.

The boundary regression exercises the trusted lower time bound, textual ordering of tied history offsets, subscription order and scope, count limits, and a byte limit that stops before a smaller later candidate. It reopens between batches, rejects the superseded token, and verifies each persisted dead letter and body. A second case ties a history lease with a redrive lease and verifies the original order and source link. The pre-existing interrupted conversion test checks replay at the previous HEAD.

In a two-binary check, `59c33e4` created the segments, leases, and an ordinary ACK receipt. The candidate opened that directory, committed one final-attempt conversion, and exited before closing the partition. The original binary reopened, verified the committed letter, finished both subscriptions' remaining expirations, and recovered the ordinary receipt and a new attempt on the original retry instance.

## Dead-letter transaction framing

Run the same staggered expiry workload to break down the first 8 MiB dead-letter batch. Run the live control with the same fixture and no expirations.

```sh
GOWORK=off MINIMQ_RETRY_EXPERIMENT=expired MINIMQ_RETRY_DEADLINES=staggered go test -run '^TestRetryExperiment$' -count=1 -v .
GOWORK=off MINIMQ_RETRY_EXPERIMENT=live MINIMQ_RETRY_DEADLINES=staggered go test -run '^TestRetryExperiment$' -count=1 -v .
GOWORK=off go test -run '^$' -bench '^BenchmarkPrepareWALFrame$' -benchtime=120x ./internal/journal
```

The baseline `5bfa9c8` adds phase measurements to production behavior from `9fa25a7`. Candidate `f6b360a` uses two positional WAL writes for JSON payloads of at least 1 MiB. It writes the eight-byte length/CRC header followed by the existing JSON buffer. Smaller records still use the contiguous frame and one WAL write. The bytes and accounted frame size are identical. The WAL sync still precedes staging and syncing the replacement HEAD and syncing its directory.

Runs were serial on a shared macOS 15.5 arm64 host using Go 1.27.3 and local APFS. One-minute load at trial starts was 1.4–3.1. Five alternating trials per version ran the expired workload and framing microbenchmark. Three per version ran the live workload. Compile the module and journal test binaries for each revision with `GOWORK=off go test -c -o <output> .` and `GOWORK=off go test -c -o <output> ./internal/journal`. Run the module test binary with the workload environment variables above and `-test.run '^TestRetryExperiment$' -test.v`. Run the journal test binary with `-test.run '^$' -test.bench '^BenchmarkPrepareWALFrame$' -test.benchtime=120x`.

| First dead-letter batch, 128 × 64 KiB | Baseline median | Split WAL median |
| --- | ---: | ---: |
| Read sealed frame, including open and CRC | 3.94 ms | 3.63 ms |
| Decode sealed JSON and body | 18.73 ms | 18.27 ms |
| Encode transaction JSON | 8.80 ms | 8.43 ms |
| Prepare length/CRC frame | 1.96 ms | 1.48 ms |
| Write WAL and staged HEAD contents | 1.11 ms | 1.13 ms |
| Sync WAL, staged HEAD, and directory | 15.40 ms | 15.24 ms |
| Whole commit, including sync | 27.84 ms | 26.91 ms |
| Whole lock hold | 59.48 ms | 57.84 ms |

The committed frame was 11,255,882 bytes in every trial. The write column measures file write calls. The commit total also covers capacity checking, file administration, and applying state. The lock total includes candidate selection and constructing the dead-letter records. Each row has its own median. One baseline trial reached 75.87 ms of lock hold and 24.97 ms of sync time. The largest costs remain decoding and synchronization.

| Isolated frame preparation | Baseline median | Split WAL median | Bytes allocated per operation |
| --- | ---: | ---: | ---: |
| Small ACK, 564-byte payload | 168 ns | 143 ns | 576 → 576 |
| Pure retry, 64,417-byte payload | 11.23 µs | 10.86 µs | 65,536 → 65,536 |
| Dead-letter batch, 11,255,874-byte payload | 1.323 ms | 0.945 ms | 11,264,024 → 8 |

The microbenchmark isolates frame preparation. JSON encoding still allocates its output, and dead letters still hold the decoded bodies. In the mixed workload the ordinary ACK and Pull P99 medians were 38.91 → 37.04 ms and 53.99 → 51.72 ms. With all leases live they were 16.98 → 16.11 ms and 16.77 → 16.94 ms. Pure retry batches held the lock for 23.90 → 24.10 ms with median commits of 15.18 → 14.83 ms across 20 batches per version. The stable benefit is removal of one approximately 10.74 MiB frame allocation and copy per full batch. The trade is one extra WAL write call on large frames. Keep the split on that basis.

### WAL recovery and TCP checks

```sh
GOWORK=off go test -run '^TestDeadLetterWALProcessExit$' -count=1 -v .
GOWORK=off MINIMQ_WAL_TCP_TEST=1 go test -run '^TestFullDeadLetterBatchOverTCP$' -count=1 -v ./cmd/minimqd
```

The journal tests compare bytes on both sides of the split threshold, count all three durability syncs, and force an error on the body write. The process test starts from copies of an on-disk fixture with 128 expired leases on 64 KiB sealed messages. It exits after the header write, after the WAL sync, and after the HEAD directory sync. Recovery discards the two unpublished attempts and reads the full batch at the published boundary. The same test checks capacity rejection, reservation accounting, payload identity, an ordinary ACK receipt, and one dead letter per lease.

To reopen these same candidate-written directories with the original production code, build a test binary from `9fa25a7` after copying only the recovery driver into its module. The candidate still performs each write and interruption.

```sh
wal_compat_dir=$(mktemp -d)
git -C .. archive 9fa25a7 minimq | tar -xf - -C "$wal_compat_dir"
cp wal_test.go "$wal_compat_dir/minimq/"
(cd "$wal_compat_dir/minimq" && GOWORK=off go test -c -o "$wal_compat_dir/original.test" .)
GOWORK=off MINIMQ_WAL_RECOVERY_BINARY="$wal_compat_dir/original.test" go test -run '^TestDeadLetterWALProcessExit$' -count=1 -v .
```

All three boundaries also passed with the original binary. The TCP test runs the broker's actual listener and maintenance loop, confirms one full visible batch of 128, and checks the same IDs and bodies plus an ordinary ACK receipt after the daemon exits and restarts. These checks exercise process interruption and local recovery.
