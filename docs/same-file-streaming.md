# Same-file download/upload overlap

Enhanced clone now uploads each file while that same file is still downloading.
Both the finite command and resident backend use this path. Ordinary `forward`
without duplex retains its sequential behavior. No Bot backend switch is made.

## Data path

The existing gotd downloader still fetches 1 MiB blocks in parallel and writes
them at their original offsets in a temporary file. A small readiness map tracks
completed block boundaries and advances a contiguous prefix. It never treats a
sparse file's length as evidence that its missing bytes have arrived.

Once the first upload-sized prefix is ready, the existing uploader starts,
reading sequential 512 KiB parts and sending them with its existing parallel
chunk workers. Reads wait for the requested prefix and observe cancellation.
Media bytes stay on disk; only readiness metadata and native chunk buffers are
in memory. A faster download can still fill the whole temporary file while an
upload catches up; this is a disk spool, not a bounded-memory media buffer or a
resumable download cache.

Download and upload share an error-group context. Failure in either direction
cancels the other, including a reader waiting for missing data. Successful EOF
from the downloader must cover the entire declared size; a premature EOF fails
instead of leaving the uploader waiting forever. Both directions finish before
the file is closed and removed. Upload slots are acquired only after initial
bytes become available, and download slots are released independently.

Native media attributes, original thumbnails, captions and album submission are
unchanged. An album is submitted only after every member has fully uploaded.
Temporary upload parts are not destination messages, and cancellation does not
undo an album that was already sent.

## Verification

A fake RPC transport uses one 12 MiB file plus a partial final part. It withholds
all later download blocks until that same file's first upload request arrives.
A sequential implementation cannot pass. Every uploaded part is checked against
its exact expected bytes and length. Tests cover successful completion, download
failure, upload failure, premature EOF and external cancellation, including slot
release and temporary-file cleanup. Existing native mixed-album, metadata,
receipt and cancellation-isolation tests remain in place.

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
