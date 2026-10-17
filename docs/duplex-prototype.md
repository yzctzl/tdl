# Disposable duplex prototype

This branch adds `tdl enhance`, an experimental clone command. It uses one
existing tdl namespace and one client lifetime for concurrent downloads and
uploads, and sends each source album directly as an album. The Bot has not been
switched to this backend.

## Build and run

Run these commands from this checkout, keeping `go.work` enabled so that the
modified local `core` module is used:

```sh
go build -o .prototype/tdl-enhance .
go test ./core/forwarder ./app/forward ./cmd
go test -race ./core/forwarder ./app/forward
```

On Windows, use `-o .prototype/tdl-enhance.exe`. Go is a developer requirement;
users of an eventual release would receive a prebuilt executable.

Use the storage and namespace already configured for tdl. The storage path below
is the directory containing the namespace database, usually `TDL_DIR/data`, not
its parent:

```sh
.prototype/tdl-enhance --storage type=bolt,path=/path/to/tdl/data -n default \
  -l 4 -t 8 enhance \
  --from https://t.me/example/123 \
  --to '@test_channel' --downloads 2 --uploads 2 --dry-run
```

Remove `--dry-run` to send. A topic destination uses the same router syntax as
`forward`, for example `--to '{Peer: "test_channel", Thread: 42}'`.

`--reupload` forces photo/video byte transfer even when server-side copying is
possible. This is useful for measuring transfer behavior; production should
continue preferring server-side copies. `--from` may be repeated for a batch.
The original `forward` command keeps its defaults. The enhanced defaults enable
clone, duplex, JSON output and removal of destination media/text spoilers.

Stop the Bot before testing with its namespace and restart it afterward. Separate
processes must not concurrently own the same Bolt database or uncoordinated
shared authorization. This experiment does not change Bot configuration, replace
its installed tdl executable, or use its normal task ledger.

## Execution model

- `-l/--limit` bounds concurrent source groups in the batch
- `--downloads` bounds files actively downloading across all groups
- `--uploads` independently bounds files actively uploading across all groups
- `-t/--threads` bounds chunk concurrency for each file, using upstream sizing
- `--pool` retains upstream connection-pool behavior

An upload does not retain a download slot. Each group prepares its members
concurrently, stores results at their source indices, and submits the final
album only after every member succeeds. Video attributes and original previews
use the native clone path; thumbnails are not reconstructed or padded.

Workers have private deduplication maps and random generators. A single
coordinator deduplicates source messages/albums before dispatch. Only transfer
budgets and a synchronized progress reporter are shared.

One failing group cancels other in-flight work in this experimental batch. A
failed member prevents submission of that group's final album. Completed groups
are not rolled back; a lost acknowledgement must be checked at the destination
before retrying. Native temporary files are closed and removed when transfer
workers exit. A hard process kill can still leave local files.

## JSON lines

Standard output contains one JSON record per event; terminal errors go to
standard error. Events include:

- `start`: source group accepted by its worker
- `plan`: resolved members, types, byte sizes and video metadata before writes
- `stage_start` / `stage_end`: actual download/upload operation boundaries,
  excluding direction-slot waits
- `progress`: completed download/upload byte deltas accumulated separately
- `sent`: confirmed destination message IDs in source order
- `done`: group completion, with `error` on failure

Each event identifies source, source message, target and optional topic. Active
operation counters describe the group; a record with both counters above zero
witnesses overlapping directions. Byte counters include native thumbnail
transfers, so they can exceed the primary-media plan size slightly. Telegram may
recompress photos when reuploaded; document byte size and metadata can be checked
exactly, while photo byte sizes need not match.

Dry runs resolve sources and targets but do not transfer bytes or send media.
They produce plans with `dry_run: true` and no confirmed `sent` results. The
`sent` event is emitted only after matching server acknowledgements to the
request's random IDs, never by scanning recent channel messages.

An optional standard-library Python helper summarizes a captured log without
printing chat identities:

```sh
python scripts/enhance_report.py enhance.jsonl
```

## Validation performed

Linux live checks used one existing namespace, an explicitly permitted internal
storage destination, and forced byte transfer:

| Check | Observation |
| --- | --- |
| Single video | About 34 MiB; transfer completed in 23.71 seconds |
| Mixed album | Nine photos and one video; one native album with ten acknowledged IDs |
| Album elapsed | 17.38 seconds for about 6.18 MiB of primary media |
| Direction overlap | Active download and upload operations overlapped for about 3.57 seconds |
| Direction budgets | Peak two downloads and two uploads, matching configuration |
| Readback | Media types/order, video byte size, dimensions, duration and thumbnail presence matched |
| Temporary files | Scratch directory empty after both live transfers |

The single video retained 1080 x 1922 dimensions; the album video retained
720 x 1280 dimensions. Test posts were retained for visual inspection. These
small tests establish correctness and overlap, not maximum sustainable bandwidth.

Offline tests exercise actual gotd chunk transfer, media preparation and RPC
serialization with a fake transport, including video preview preservation,
source order, failure cleanup, missing acknowledgements and cancellation
isolation. A batch test submits 32 distinct sources plus a duplicate and verifies
that eight workers run concurrently, without duplicate delivery. The JSON
reporter is also exercised concurrently. Race tests and repeated runs are part
of the check commands above.

## Deliberate limits

- This is a finite CLI batch, not the future resident Bot backend
- Each individual file is downloaded fully before its upload starts; different
  files overlap, including members of the same album
- No single-file streaming, adaptive bandwidth tuner or resume ledger is added
- Ctrl+C cancels the batch; there is no external per-job cancel API yet
- A final acknowledgement failure remains uncertain, rather than safe to retry
- Windows/macOS cross-compilation does not replace native device testing

The next decision should follow a larger-file and longer-running benchmark.
If worthwhile, the next stage is a resident request/response interface that owns
all MTProto operations for an account. Python keeps permissions, durable business
state and Bot API delivery, avoiding two clients competing over one authorization.
