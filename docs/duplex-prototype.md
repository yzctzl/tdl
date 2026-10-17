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

Workers have private deduplication maps. Send IDs use Go's concurrency-safe
global random generator, avoiding per-worker time-based seeds. A single
coordinator deduplicates source messages/albums before dispatch. Transfer budgets
and a synchronized progress reporter are shared.

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
- Enhanced clone streams each file through a temporary disk spool while its
  download is still running; see [same-file-streaming.md](same-file-streaming.md)
- No adaptive bandwidth tuner or resume ledger is added
- Ctrl+C cancels the batch; there is no external per-job cancel API yet
- A final acknowledgement failure remains uncertain, rather than safe to retry
- Windows/macOS cross-compilation does not replace native device testing

The resident follow-up is documented in [enhance-resident.md](enhance-resident.md).

## Sustained large-album benchmark before same-file streaming

A subsequent Linux amd64 run forced byte transfer for three videos, totaling
1,217,223,048 bytes (about 1.13 GiB). Settings were two download slots, two upload
slots, eight per-file chunk workers, four group workers and the default pool size
of eight. The existing workspace network/proxy configuration was retained.

| Measurement | Observed value |
| --- | --- |
| Wall time including connection/setup/shutdown | 348.71 seconds |
| Group event elapsed time | 344.32 seconds |
| Download operations active | 219.97 seconds |
| Upload operations active | 244.37 seconds |
| Both directions active | 121.09 seconds |
| Download rate over its active time | 5.28 MiB/s |
| Upload rate over its active time | 4.75 MiB/s |
| Direction peaks | Two downloads and two uploads |
| Peak temporary file bytes, sampled each second | About 961.75 MiB |
| Peak resident memory, sampled each second | About 111.07 MiB |
| Result | One native album, three confirmed destination IDs |

Both direction counters finished at 1,217,244,417 bytes, including original
thumbnails. Destination readback matched all three document sizes, source order,
880 x 1906 video dimensions, duration and thumbnail presence. The scratch
directory was empty afterward. The Bot was restored and test posts retained.

These are active-direction averages, not peak line rates. The test demonstrates
sustained overlap, not network saturation or the account's maximum throughput.
There was no isolated one-way baseline; those rates cannot quantify degradation
under duplex load. This earlier implementation downloaded each file fully
before uploading it, with a download-first startup and upload tail. The
subsequent same-file implementation removes that dependency.

The resident protocol was separately exercised with two simultaneous live
requests, cancellation of one, and another request on the surviving connection.
Large-file throughput above belongs to the finite command, not a resident
many-request benchmark. Before production integration, move all account MTProto
operations behind that resident owner, retaining Python permissions, durable
business state and Bot API delivery.

Targeted package tests, race checks, native builds and Windows amd64/macOS arm64
cross-builds passed. A broader root/core test run passed its offline packages but
the upstream `test` suite timed out trying to connect to its separately required
local Telegram test server at `127.0.0.1:10443`; that suite was not validated.
