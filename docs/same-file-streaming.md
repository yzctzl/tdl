# Same-file download/upload overlap

Enhanced clone now uploads each file while that same file is still downloading.
Both the finite command and resident backend use this path. Ordinary `forward`
without duplex retains its sequential behavior. No Bot backend switch is made.

## Data path

Large files (above gotd's 10 MiB small-upload threshold) use `parts.go`. The
existing gotd downloader fetches 1 MiB blocks in parallel. Each completed block
is split into 512 KiB upload parts with indices derived from its source offset,
then handed to a bounded channel. Parallel upload workers save those parts using
`upload.saveBigFilePart`. Offset zero need not arrive first: a completed later
block can upload immediately, independently of missing earlier blocks.

The large-file path creates no media temporary file. Queued bytes are copied
because the download buffers belong to gotd. Queue capacity follows per-file
chunk workers; memory is bounded by transfer concurrency, not media size. If an
upload is slower than its download, queue backpressure limits read-ahead instead
of retaining an entire file in RAM. Infinite simultaneous bandwidth utilization
with unequal rates would require unbounded storage, so this queue deliberately
bounds retained data.

Small files and original thumbnails retain `stream.go` plus the native uploader,
which computes their ordered MD5 checksum. This path writes downloaded blocks
at their original offsets into a temporary file, tracks the contiguous ready
prefix, and allows the uploader to read only those ready bytes. Small-file
previews and checksums are preserved without a second custom upload algorithm.

Both paths share download/upload budgets and an error-group context. Failure in
either direction cancels the other, including queued writes or waiting readers.
Successful download must cover the declared size; premature EOF fails the job.
Upload responses of false are retried without duplicating confirmed progress;
FloodWait and cancellation retain native behavior. Upload slots are acquired
only after the first data arrives. No final file handle is returned until both
directions have completed, and small-file temporary files are removed afterward.

Native media attributes, original thumbnails, captions and album submission are
unchanged. An album is submitted only after every member has fully uploaded.
Temporary upload parts are not destination messages, and cancellation does not
undo an album that was already sent.

## Verification

A fake RPC transport uses one 12 MiB file plus a partial final part. One scenario
withholds later download blocks until the same file starts uploading. Another
withholds offset zero until a later part has uploaded: a contiguous-prefix
implementation cannot pass. Every uploaded part is checked against its exact
expected bytes, offset-derived index and length, and confirmed byte accounting
must match the whole file even after a false upload response is retried.

Tests cover download failure, upload failure, premature EOF, external
cancellation and slot release. The large-file test deliberately supplies a
nonexistent temporary directory, proving that this path does not stage media on
disk. Native mixed-album, metadata, receipt and cancellation-isolation tests remain
in place.

A live functional check then forced reupload of one 35,780,617-byte video through
the ready-part path. It completed without staging the main media file on disk;
destination readback preserved document size, dimensions, duration and thumbnail
presence. A resident check cancelled a ready-part video while both directions
were active: an unrelated ten-member album and a subsequent dry run succeeded
on the same connection. The ready handshake confirmed the automatic per-DC pool
capacity of 32 for four workers, eight chunk threads and explicit two-file
limits per direction. Scratch cleanup was verified and the Bot was restored.
These checks validate behavior; no speed-ratio or pool-size sweep was performed.

## Earlier disk-spool measurements

The timings below describe the preceding contiguous-prefix implementation;
they are historical correctness evidence, not speed claims for the ready-part
implementation. No environment-specific speed comparison is required for the
new data path.

A real forced-reupload test used one 35,780,617-byte video. Its first upload
acknowledgement arrived with only 12,582,912 bytes downloaded. Download and upload
operations overlapped for 7.86 seconds; the process finished in 21.41 seconds.
Destination readback preserved byte size, dimensions, duration and thumbnail
presence. Logs and destination receipts remain in ignored private test storage.
These timings demonstrate overlap and are not a controlled speed comparison.

A second live run selected one 462,151,305-byte video with `--single` and forced
reupload. The first upload acknowledgement arrived after only 20,971,520 bytes
had downloaded. Wall time was 116.96 seconds, with download and upload operations
overlapping for 98.04 seconds. Active-direction averages were 4.22 MiB/s down and
4.16 MiB/s up. Both byte counters finished at 462,157,893 bytes including the
original thumbnail. Destination readback matched the exact document size,
880 x 1906 dimensions, duration and thumbnail presence, and local scratch files
were removed. These measurements used eight native chunk workers and the
existing account/network configuration; they do not establish bandwidth limits.

A live resident test then cancelled a single-video request after it had both
acknowledged upload bytes and an active download. A concurrent ten-member album
completed successfully, and a subsequent dry run succeeded on the same resident
connection. Cancelled work had no confirmed destination message, and request
scratch directories were empty after draining. The workspace Bot was restored.

Native builds, Windows amd64/macOS arm64 cross-builds, vet and five repeated race
runs of the affected forwarder, forward app, command and dispatcher packages
passed. Cross-builds do not replace native device execution.

```sh
go test -race ./core/forwarder ./app/forward ./cmd ./core/dispatch
```
