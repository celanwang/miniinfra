# Dead-letter page experiment

Run from the MiniMQ module. The seed test materializes an on-disk checkpoint using the same approach as the backlog fixture. It registers subscriptions and writes a manifest of expected IDs independently of the paging code. The fixture has 8,192 billing letters, 1,024 letters each for email, analytics and webhooks, and 64 bulk letters. The first four subscriptions use 256-byte bodies. Each bulk body is 64 KiB. Set `MINIMQ_DEAD_PAGE_ROWS=128` for the small control, which has 128 letters per small-body subscription and the same bulk cohort.

```sh
dead_page_dir=$(mktemp -d)
MINIMQ_DEAD_PAGE_SEED="$dead_page_dir" GOWORK=off go test -run '^TestSeedDeadLetterPageFixture$' -count=1 .
MINIMQ_DEAD_PAGE_FIXTURE="$dead_page_dir" GOWORK=off go test -run '^$' -bench '^BenchmarkDeadLetterPage$' -benchtime=256x .
dead_page_tcp_dir=$(mktemp -d)
cp -R "$dead_page_dir/." "$dead_page_tcp_dir/"
MINIMQ_DEAD_PAGE_TCP_DIR="$dead_page_tcp_dir" GOWORK=off go test -run '^TestDeadLetterPagesOverTCP$' -count=1 -v ./cmd/minimqd
```

The kernel benchmark measures the full page call, lock hold, and candidate selection separately from constructing the result and cloning its bodies. It reads 16 results from the start and midpoint of the billing sequence and from a cursor with 32 eligible letters left. It also tests a small sibling subscription, a 256-entry billing page, and the bulk subscription. Allocation measurements cover the whole page call.

The TCP test launches the normal broker listener and maintenance loop in a test subprocess. Each quiet small-page case sends 96 requests. The bulk case sends 24 requests with 1 MiB of raw bodies per page. The encode control calls the same JSON response writer on those actual pages without a socket. It measures encoding separately from client decoding and transport time. These samples and the kernel measurements describe different stages and their percentiles should be read independently.

The mixed phase starts two browsers on staggered 22 ms intervals and runs 32 normal Pull/ACK pairs on 44 ms intervals. Each of the four small-page shapes gets 32 requests. The test checks every returned ID against the manifest and checks the subscription, body length, and boundary bytes. It kills and reopens the broker and repeats the same cursor queries and the last ACK. Always give the TCP test a fresh fixture copy because the ordinary requests write to the partition.

The measured baseline was `0f91eb6`, which adds instrumentation to production behavior from `bc5e115`. The bounded candidate was `37ce81b`. To alternate binaries, compile both revisions using `GOWORK=off go test -c -o <path> .` and `GOWORK=off go test -c -o <path> ./cmd/minimqd`. Run the kernel binary with the fixture environment variable and `-test.run '^$' -test.bench '^BenchmarkDeadLetterPage$' -test.benchtime=256x`. Run the TCP binary with the copied fixture environment variable and `-test.run '^TestDeadLetterPagesOverTCP$' -test.v`.

## Results

Three interleaved runs per version used the larger fixture. Two per version used the small fixture. The timing runs shared the temporary benchmark lock on a macOS 15.5 arm64 host with Go 1.27.3 and local APFS. Trial start load averages were about 1.2–1.6. The table uses medians across runs. The lock figures are per-operation means within each run. The TCP figures are per-run percentiles.

| Large fixture | Lock hold, baseline → bounded | Allocated per call | Quiet TCP P50 / P99, baseline → bounded |
| --- | ---: | ---: | ---: |
| First 16 | 700 → 204 µs | 496,604 → 11,120 B | 842 / 1,420 → 367 / 944 µs |
| Middle 16 | 407 → 177 µs | 250,800 → 11,120 B | 544 / 908 → 316 / 399 µs |
| Tail, 32 eligible before taking 16 | 119 → 119 µs | 11,824 → 11,120 B | 251 / 477 → 253 / 414 µs |
| Sibling subscription, first 16 | 186 → 135 µs | 70,576 → 11,120 B | 315 / 498 → 226 / 338 µs |
| Billing, first 256 | 714 → 287 µs | 662,981 → 182,148 B | — |
| Bulk, first 16 | 167 → 164 µs | 1,057,456 → 1,055,620 B | 2,534 / 3,492 → 2,518 / 3,561 µs |

On the first 16 small-body page, selecting the IDs took 698 → 202 µs and building the copied response took 1.51 → 1.62 µs. Isolated server JSON encoding P50 was about 7 µs. For the bulk page, cloning and building the result cost about 42–45 µs and encoding about 289 µs. Its TCP P50 stayed around 2.5 ms. Larger responses still pay the cost of encoding, client decoding, and transmission. In the small fixture the quiet first-page TCP P50 held at 161 µs. The 256-limit call over 128 eligible letters took about 3 µs longer under the lock.

| Mixed workload, large fixture | Baseline P99 | Bounded P99 |
| --- | ---: | ---: |
| First page | 19.89 ms | 18.59 ms |
| Middle page | 20.35 ms | 16.88 ms |
| Tail page | 17.00 ms | 18.34 ms |
| Sibling page | 14.52 ms | 14.83 ms |
| Ordinary Pull | 20.10 ms | 22.24 ms |
| Ordinary ACK | 20.96 ms | 19.97 ms |

Each mixed P99 is the maximum of 32 samples per run. The ordinary Pull P99 ranges overlapped at 18.93–21.05 ms and 17.81–22.65 ms. Shorter selection reliably improved quiet early pages and reduced allocation. The mixed run still spent much longer on requests once durable work competed for the partition, and produced no consistent foreground latency gain.

Keep the bounded selection. A sorted slice stores up to the requested number of IDs. Once it fills, only an earlier candidate can enter the slice. The scan still visits the partition's dead-letter map, which accounts for the similar tail cost. The list clones bodies under the lock and stops before the first letter that would exceed the byte budget. A later smaller letter cannot bypass that boundary.

The boundary test uses a cursor at an existing ID and one between IDs, verifies that mutating a returned body leaves the stored body intact, and checks the strict expiration instant, subscription separation, byte prefix, empty end page and restart. The mixed TCP test checks its materialized cursor manifest before and after restart. The separate full dead-letter TCP test also verifies pages of real timeout conversions across a daemon exit.
