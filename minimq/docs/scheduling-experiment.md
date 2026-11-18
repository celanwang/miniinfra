# Scheduled release experiment

Run from the MiniMQ module on a local filesystem. Run one trial at a time and record background load.

```sh
GOWORK=off MINIMQ_DUE_EXPERIMENT=burst go test -run '^TestDueExperiment$' -count=1 -v ./cmd/minimqd
GOWORK=off MINIMQ_DUE_EXPERIMENT=spread go test -run '^TestDueExperiment$' -count=1 -v ./cmd/minimqd
GOWORK=off MINIMQ_DUE_EXPERIMENT=restart go test -run '^TestDueExperiment$' -count=1 -v ./cmd/minimqd
GOWORK=off MINIMQ_DUE_EXPERIMENT=partial MINIMQ_DUE_SIZE=large go test -run '^TestDueExperiment$' -count=1 -v ./cmd/minimqd
```

The standard fixture schedules 640 messages of 1 KiB each. Set `MINIMQ_DUE_SIZE=large` for 384 messages of 64 KiB each. A simultaneous release requires three batches in both fixtures. The standard fixture hits the 256-message limit. The large fixture hits the 8 MiB limit at 128 messages. The spread control schedules the same messages in five groups 150 ms apart and accepts the groups in reverse order within each set of five publications.

Setup persists the schedules and two lease probes using the Go API before starting the timed daemon. The broker subprocess uses the normal TCP listener, HTTP routes, 250 ms delivery tick, and 30-second checkpoint timer. The fixture clock is fixed during seeding and advances at wall-clock speed after setup, with a common origin across restarts. In `restart` the daemon is killed before the scheduled time and the next process starts 125 ms after it. In `partial` the daemon is killed with at least one release visible and at least one still pending. A final abrupt reopen checks the offset and receipt of every scheduled record, idempotent publish and ACK, and an open lease that can still be acknowledged.

The subscriber that existed at admission polls via HTTP `Pull` roughly every 12 ms. The measured latency ends when a validated first delivery reaches that client. It includes the publication's durable write, wait for the next pull, the durable lease, and the HTTP response. The test checks the stored publication timestamp against the requested time and verifies delivery order by deadline and acceptance order. The drain time runs from the first deadline to the last observed delivery. In spread mode that interval includes 600 ms of intentional spacing.

A later subscriber runs ordinary publish, pull, and ACK cycles with a 45 ms minimum cycle period. It receives only its own immediate publications. The output records completed ordinary cycles during the release window, ordinary operation P99 and the longest gap between completions. One probe observes a genuine second attempt after lease expiry. Pull can settle that retry itself. A separate probe waits for a final-attempt lease to reach dead letters through background maintenance without pulling that subscription. The timed trials finish before the first checkpoint.

## Initial 25 ms follow-up

The measured baseline is `cdc3018`. It adds the experiment to the production behavior from `6a716dc`. The prototype let a regular scheduling pass request one extra pass, after running retries and leaving a 25 ms interval for foreground operations. It treats eight or more releases as a possible batch limit. Eight maximum-size messages could fill the byte limit. Fewer than eight leaves room for any valid next message, so a pass of that size already exhausted the work ready at its clock sample.

The 25 ms variant lowered the median per-run P99 from 685 to 428 ms for the 640-message burst, from 945 to 708 ms for the same burst overdue after restart, and from 774 to 516 ms for the 384-message burst. The large burst still committed three batches of 128. The longest gap between ordinary completions in the large-burst trials was 97–109 ms at baseline and 93–227 ms with the 25 ms variant. Ordinary Pull P99 in that workload went from 45 to 74 ms at the median. The short interval saved a tick for delayed delivery and could concentrate two large commits into the same foreground stall.

## 100 ms follow-up

The second experiment left a 100 ms interval after the regular scheduling and retry passes. The regular 250 ms ticker keeps its phase. Processing a regular tick clears any still-pending follow-up. Each regular round can arm one follow-up, which can produce a smaller additional durable batch when scattered deadlines fall inside the interval. Each pass still uses 256 messages and 8 MiB as upper limits. An idle broker checks schedules and retries four times per second.

The trials ran serially on macOS 15.5 arm64 with Go 1.27.3 and local APFS. One-minute load averages at trial starts ranged from 1.6 to 3.5. The broker binaries used the same experiment code. To reproduce the baseline, compile the test binary at `cdc3018` with `GOWORK=off go test -c -o /tmp/minimqd-baseline.test ./cmd/minimqd`. Compile the current version the same way at a different output path. Run both with `MINIMQ_DUE_EXPERIMENT` and `MINIMQ_DUE_SIZE` as above, plus `-test.run '^TestDueExperiment$' -test.v`. Alternate binary order across repetitions.

The latency columns show the median of per-trial percentiles. The drain column gives the longest observed time from the first deadline to the last first delivery, in milliseconds.

| Workload | Trials, baseline / 100 ms | Baseline P50 / P99 | 100 ms P50 / P99 | Longest drain, baseline → 100 ms |
| --- | ---: | ---: | ---: | ---: |
| 640 × 1 KiB, simultaneous | 4 / 3 | 448 / 692 | 353 / 445 | 704 → 447 |
| 640 × 1 KiB, spread | 4 / 3 | 147 / 249 | 101 / 251 | 733 → 742 |
| 640 × 1 KiB, overdue at restart | 4 / 3 | 706 / 954 | 608 / 713 | 973 → 744 |
| 384 × 64 KiB, simultaneous | 3 / 3 | 543 / 791 | 445 / 528 | 829 → 563 |
| 384 × 64 KiB, spread | 1 / 1 | 316 / 404 | 157 / 307 | 1,004 → 759 |
| 384 × 64 KiB, overdue at restart | 1 / 1 | 855 / 1,077 | 712 / 785 | 1,119 → 824 |

The small controls include one baseline rerun per shape during the 100 ms series. The large simultaneous row uses the baselines from that series. Both large spread and large restart were single-run checks. The large overdue restart listener came up 198 and 203 ms after the initial deadline. In the small overdue tests it came up 139–156 ms after the deadline. That time is included in the reported delays.

| Ordinary traffic | Baseline / 100 ms completed cycles | Baseline / 100 ms longest completion gap per trial | Baseline → 100 ms Publish / Pull / ACK P99, medians |
| --- | ---: | ---: | ---: |
| 640 × 1 KiB, simultaneous | 31–34 / 33 | 85–140 / 106–111 ms | 49 / 56 / 44 → 49 / 64 / 48 ms |
| 640 × 1 KiB, spread | 31–33 / 31–32 | 108–140 / 109–158 ms | 43 / 62 / 46 → 44 / 51 / 51 ms |
| 640 × 1 KiB, overdue at restart | 17–26 / 16–27 | 107–168 / 108–192 ms | 46 / 54 / 63 → 42 / 61 / 64 ms |
| 384 × 64 KiB, simultaneous | 31–32 / 32–33 | 112–114 / 104–177 ms | 87 / 43 / 62 → 46 / 45 / 78 ms |

Every trial made ordinary progress during the release window. In the byte-limited burst, the normal worker completed 11–12 cycles during the longer baseline release and seven during the shorter 100 ms release. The retry and background-expiry probes both completed inside the release window. Their 100 ms variant times in the large burst were 49–70 ms and 205–216 ms after the lease deadline. Percentiles for individual foreground operations are sensitive to short samples and background disk activity.

Keep the 100 ms follow-up. It removed roughly one regular tick from simultaneous and overdue bursts while allowing ordinary work and the retry loop to progress. It still raises some foreground tail latencies when a release writes close to the 8 MiB limit. The small spread control showed little P99 benefit and an additional 9 ms in the longest observed drain. The checkpoint timer and durable release transaction are unchanged.

All three policies preserved pre-deadline invisibility, release order, and membership in these trials. Every scheduled record appeared once at the same offset after reopening. The partial-release crash with the 100 ms interval kept the already visible offset, released the remaining work in order, and preserved ACK receipts and an active lease after another kill. These are process-crash and short release-window tests. Checkpoint overlap, sustained saturation and device power loss were outside the measured conditions.
