package forwarder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"

	"github.com/gotd/td/constant"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"golang.org/x/sync/errgroup"

	tdownloader "github.com/iyear/tdl/core/downloader"
	tuploader "github.com/iyear/tdl/core/uploader"
)

// Small uploads require an ordered MD5 checksum; retain the native uploader there.
const bigUploadThreshold = constant.UploadMaxSmallSize

type uploadPart struct {
	index int
	data  []byte
}

// partSink hands downloaded blocks to the upload queue by their source offset.
// The downloader owns its buffers, so queued parts take their own bounded copy.
// gotd serializes WriterAt calls; byte accounting and first-block signaling
// therefore belong to this one writer, without another lock.
type partSink struct {
	ctx        context.Context
	queue      chan<- uploadPart
	size       int64
	bytes      int64
	onDownload func(int64)
	first      chan struct{}
	started    bool
}

func (s *partSink) WriteAt(data []byte, offset int64) (int, error) {
	const partSize = tuploader.MaxPartSize
	if offset < 0 || offset%partSize != 0 || offset+int64(len(data)) > s.size ||
		(len(data)%partSize != 0 && offset+int64(len(data)) != s.size) {
		return 0, fmt.Errorf("download block does not match upload part boundaries")
	}
	// A later block can unlock uploading even when offset zero is still pending.
	if len(data) > 0 && !s.started {
		close(s.first)
		s.started = true
	}
	s.bytes += int64(len(data))
	s.onDownload(int64(len(data)))
	for sent := 0; sent < len(data); {
		end := min(sent+partSize, len(data))
		part := uploadPart{index: int((offset + int64(sent)) / partSize), data: bytes.Clone(data[sent:end])}
		select {
		case s.queue <- part:
			sent = end
		case <-s.ctx.Done():
			return sent, s.ctx.Err()
		}
	}
	return len(data), nil
}

func saveReadyPart(ctx context.Context, client *tg.Client, id int64, total int, part uploadPart) error {
	request := &tg.UploadSaveBigFilePartRequest{FileID: id, FilePart: part.index, FileTotalParts: total, Bytes: part.data}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, err := client.UploadSaveBigFilePart(ctx, request)
		if flood, err := tgerr.FloodWait(ctx, err); err != nil {
			if flood {
				continue
			}
			return err
		}
		if ok {
			return nil
		}
	}
}

func (f *Forwarder) cloneParts(ctx context.Context, opts cloneOptions, threads int) (tg.InputFileClass, error) {
	if threads < 1 {
		return nil, fmt.Errorf("upload threads must be positive")
	}
	parts := make(chan uploadPart, threads)
	first := make(chan struct{})
	id := rand.Int64()
	total := int((opts.media.Size + tuploader.MaxPartSize - 1) / tuploader.MaxPartSize)
	download := func(ctx context.Context) error {
		defer close(parts)
		f.stageProgress(opts.elem, "download", true)
		defer f.stageProgress(opts.elem, "download", false)
		sink := &partSink{ctx: ctx, queue: parts, size: opts.media.Size, first: first, onDownload: func(n int64) {
			opts.progress.add(n)
			f.transferProgress(opts.elem, "download", n)
		}}
		_, err := downloader.NewDownloader().WithPartSize(tdownloader.MaxPartSize).
			Download(f.opts.Pool.Client(ctx, opts.media.DC), opts.media.InputFileLoc).
			WithThreads(threads).Parallel(ctx, sink)
		if err == nil && sink.bytes != opts.media.Size {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	upload := func(ctx context.Context) error {
		f.stageProgress(opts.elem, "upload", true)
		defer f.stageProgress(opts.elem, "upload", false)
		client := f.opts.Pool.Default(ctx)
		workers, ctx := errgroup.WithContext(ctx)
		for range threads {
			workers.Go(func() error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case part, ok := <-parts:
						if !ok {
							return nil
						}
						if err := saveReadyPart(ctx, client, id, total, part); err != nil {
							return err
						}
						n := int64(len(part.data))
						opts.progress.add(n)
						f.transferProgress(opts.elem, "upload", n)
					}
				}
			})
		}
		return workers.Wait()
	}
	err := f.pipeline.transfer(ctx, download, upload, func(ctx context.Context) error {
		select {
		case <-first:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		return nil, fmt.Errorf("clone parts: %w", err)
	}
	return &tg.InputFileBig{ID: id, Parts: total, Name: opts.media.Name}, nil
}
