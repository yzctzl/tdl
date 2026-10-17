# Resident enhance backend

`tdl enhance serve` extends the disposable duplex prototype with a persistent
JSON-lines interface over standard input/output. One existing tdl namespace,
logged-in client, peer manager and DC pool serve all requests. No TCP listener,
additional login, Python Bot changes or production backend switch is added.

```sh
go build -o .prototype/tdl-enhance .
.prototype/tdl-enhance --storage type=bolt,path=/path/to/tdl/data -n default \
  -l 8 -t 8 enhance serve --downloads 2 --uploads 2 --queue 64 \
  --temp-dir /path/to/existing/scratch
```

Windows builds use the `.exe` suffix. Standard streams avoid Unix socket and
platform-specific process signaling requirements for normal shutdown.

## Capacity and lifetimes

`-l` bounds active requests; `--queue` bounds additional waiting requests.
`--downloads` and `--uploads` are account-wide file budgets shared by all requests,
not budgets multiplied by the number of workers. Per-file chunk threading still
uses `-t`. Temporary directories are private to a request and are removed before
its completion record. A failed request does not cancel unrelated requests.

Queued cancellation removes the request immediately and frees queue capacity.
Active cancellation propagates to its RPCs and waits for native cleanup; it does
not disconnect the shared client or close its pools. The protocol loop continues
accepting control messages while all workers are occupied. Finished requests are
removed from the registry, avoiding an unbounded history in the resident process.

EOF drains accepted work, then closes the account's connection pools. Parent
context cancellation or broken output cancels accepted work and waits for cleanup.
The input stream is closed when the server exits, unblocking its reader.

## Protocol version 1

The first output record has `id: 0` and `event: "ready"`. Its `data` contains
`protocol`, `workers`, `queue`, and `info`. The latter contains the verified
`user_id`, `username`, and account-wide `downloads`/`uploads` budgets. The caller
must compare this identity with the intended execution account.

Every input has a positive integer `id` strictly greater than the preceding input
ID, including controls and rejected requests. This rejects replays with constant
history storage. A new parent connection starts a new ID sequence; it does not
make resubmission of an earlier transfer safe.

Examples below are individual lines; replace the sample source and destination:

```json
{"id":1,"method":"clone","from":"https://t.me/example/123","to":"@test_channel"}
{"id":2,"method":"clone","from":"https://t.me/example/456","to":"@test_channel","dry_run":true}
{"id":3,"method":"cancel","target":1}
{"id":4,"method":"ping"}
```

A clone request represents one source URL and its complete media group. Its
`to` value has the same peer/router syntax as the finite prototype. `dry_run`
resolves and plans without sending. `reupload: true` forces byte transfer for
benchmarking rather than using direct references. Spoilers are removed from
copies; original captions, video attributes, previews and media order are kept.

Records contain the request `id`, `event`, an emission `time`, and optional
`data`/`error`. Accepted work produces:

1. `accepted`, emitted before any task execution or progress
2. `running`, when a worker slot is allocated
3. Native `start`, `plan`, `stage_start`, `progress`, `stage_end`, `sent`, and
   `group_done` events; the native JSON record is nested under `data`
4. Exactly one `complete` with `data.status` equal to `succeeded`, `cancelled`, or
   `failed`, after request cleanup

`sent` contains confirmed message IDs in source order. A `complete` event alone
is not a receipt for an album. A lost acknowledgement can leave an uncertain
write. Cancellation never rolls back already sent messages; preserve any `sent`
receipts and check uncertain results before retrying.

Invalid requests, stale IDs, missing tasks and full queues produce `rejected`.
`cancel` produces `cancelling` for its own ID, followed later by the affected
request's `complete`. A task can finish successfully while cancellation races
with its completion, so the cancellation acknowledgement does not promise a
cancelled outcome. `ping` returns `pong` without waiting for transfer slots.

Control records and progress share a synchronized encoder. Output failures stop
execution rather than accepting tasks whose acknowledgement cannot be delivered.
Request lines are limited to 1 MiB; this is an internal local process protocol.

## Verification

Offline/race tests cover queue saturation, immediate queued-slot reuse, control
responsiveness, ID replay rejection, broken output, EOF draining, cleanup before
completion, and cancellation isolation. A 32-worker test runs 64 requests and
checks that one failed request leaves the other 63 successful.

Live validation used the existing authorized workspace account:

- Two source requests ran in the same resident process
- The single-video request was cancelled after its first downloaded bytes
- The other request sent one complete ten-member mixed album
- A subsequent dry-run request succeeded through the same connection
- Cancelled work had no acknowledged final message; the scratch directory was
  empty after the process drained
- Combined telemetry stayed within two downloads and two uploads; download and
  upload operations overlapped

The Bot was stopped for exclusive session ownership and restored afterward.
Posts are retained for inspection. Logs containing account identities and
receipts remain in ignored local prototype storage.

```sh
go test -race ./core/dispatch ./core/forwarder ./core/middlewares/recovery \
  ./app/forward ./cmd ./pkg/kv
python scripts/enhance_report.py resident.jsonl
```

The same report helper accepts finite and resident logs and aggregates active
operations across requests. A cancelled group is counted separately from failed
groups in new-format logs.

## Sustained resident check before same-file streaming

A later run kept the same account open for a 1.13 GiB three-video album and a
dynamically submitted ten-member mixed album. It used two request workers, two
account-wide downloads, two uploads, eight chunk workers per file and the default
connection pool size of eight. A third queued request was cancelled, then a new
dry-run request completed while the large album was still transferring.

| Measurement | Observation |
| --- | --- |
| End-to-end process wall time | 358.54 seconds |
| Active-direction overlap | 118.09 seconds |
| Download active average | 5.17 MiB/s |
| Upload active average | 4.77 MiB/s |
| Global direction peaks | Two downloads, two uploads |
| Local stdio ping and queued-cancel response | About 0.3 ms each |
| New dry-run completion during the large transfer | 2.82 seconds |
| Confirmed delivery | Two albums, thirteen media |
| Request results | Three succeeded (one dry run), one queued cancellation |
| Peak temporary data / resident memory, sampled each second | About 961.75 MiB / 110.34 MiB |

Readback of the three destination videos matched document sizes, source order,
video dimensions, durations and original-thumbnail presence. The scratch
directory was empty and the Bot was restored. No cancelled request produced a
confirmed destination message.

Worker admission and control responsiveness do not promise immediate bandwidth
for a new request. In this deliberately limited two-download configuration, the
small album waited behind already admitted large files and completed about
187.77 seconds after submission. Byte transfer still obeys the configured
account-wide budget; future tuning should measure latency as well as throughput.
There is no one-way baseline or high-fanout live load result yet, so these numbers
do not establish account limits or bandwidth saturation.

The report helper counts native group results and resident request results
separately: queued cancellation has a terminal request but no native group.

## Before production integration

The Python task engine should retain permission checks, durable jobs, retry
policy, result caching and final Bot delivery. It will own the child's standard
streams and route responses by ID, persisting confirmed receipts. Frontend reload
can continue while this existing engine and its resident child remain alive.

Do not attach the current Telethon shared session alongside this resident process.
Message resolution, history reads, target lookups, joining and other MTProto
operations must be moved behind the same account owner or otherwise given a
verified independent authorization. Only changing the upload method leaves the
current session coordination bottleneck in place.

Files now download and upload concurrently through a disk spool, as described
in [same-file-streaming.md](same-file-streaming.md). Crash adoption of a detached
child, a transport-side durable ledger and an adaptive bandwidth tuner are not
added.
A 1.13 GiB finite-album benchmark is recorded in
[duplex-prototype.md](duplex-prototype.md#sustained-large-album-benchmark-before-same-file-streaming). Higher-fanout resident tests and one-way baselines are still needed before
choosing production defaults.
