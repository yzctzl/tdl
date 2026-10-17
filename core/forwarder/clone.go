package forwarder

import (
	"context"
	"io"
	"os"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"go.uber.org/atomic"
	"go.uber.org/multierr"

	tdownloader "github.com/iyear/tdl/core/downloader"
	"github.com/iyear/tdl/core/tmedia"
	tuploader "github.com/iyear/tdl/core/uploader"
	"github.com/iyear/tdl/core/util/tutil"
)

type cloneOptions struct {
	elem     Elem
	media    *tmedia.Media
	progress progressAdd
}

type progressAdd interface {
	add(n int64)
}

func (f *Forwarder) cloneMedia(ctx context.Context, opts cloneOptions, dryRun bool) (_ tg.InputFileClass, rerr error) {
	// if dry run, just return empty input file
	if dryRun {
		// directly call progress callback
		opts.progress.add(opts.media.Size * 2)

		return &tg.InputFile{}, nil
	}

	if f.pipeline != nil && opts.media.Size > bigUploadThreshold {
		return f.cloneParts(ctx, opts, tutil.BestThreads(opts.media.Size, f.opts.Threads))
	}

	temp, err := os.CreateTemp(f.opts.TempDir, "tdl_*")
	if err != nil {
		return nil, errors.Wrap(err, "create temp file")
	}
	defer func() {
		multierr.AppendInto(&rerr, temp.Close())
		multierr.AppendInto(&rerr, os.Remove(temp.Name()))
	}()

	var stream *transferFile
	var destination io.WriterAt = temp
	if f.pipeline != nil {
		stream = newTransferFile(temp, opts.media.Size)
		destination = stream
	}
	threads := tutil.BestThreads(opts.media.Size, f.opts.Threads)

	download := func(ctx context.Context) error {
		f.stageProgress(opts.elem, "download", true)
		defer f.stageProgress(opts.elem, "download", false)
		_, err := downloader.NewDownloader().
			WithPartSize(tdownloader.MaxPartSize).
			Download(f.opts.Pool.Client(ctx, opts.media.DC), opts.media.InputFileLoc).
			WithThreads(threads).
			Parallel(ctx, writeAt{f: destination, opts: opts, notify: func(n int64) { f.transferProgress(opts.elem, "download", n) }})
		if err == nil && stream != nil {
			err = stream.finish()
		}
		return err
	}
	var file tg.InputFileClass
	upload := func(ctx context.Context) error {
		f.stageProgress(opts.elem, "upload", true)
		defer f.stageProgress(opts.elem, "upload", false)
		var source io.Reader = temp
		if stream != nil {
			source = &transferReader{file: stream, ctx: ctx}
		} else if _, err := temp.Seek(0, io.SeekStart); err != nil {
			return errors.Wrap(err, "seek")
		}
		var err error
		file, err = uploader.NewUploader(f.opts.Pool.Default(ctx)).
			WithPartSize(tuploader.MaxPartSize).
			WithThreads(threads).
			WithProgress(uploaded{opts: opts, prev: atomic.NewInt64(0), notify: func(n int64) { f.transferProgress(opts.elem, "upload", n) }}).
			Upload(ctx, uploader.NewUpload(opts.media.Name, source, opts.media.Size))
		return err
	}
	if f.pipeline != nil {
		err = f.pipeline.transfer(ctx, download, upload, func(ctx context.Context) error { return stream.wait(ctx, min(opts.media.Size, tuploader.MaxPartSize)) })
	} else {
		err = download(ctx)
		if err == nil {
			err = upload(ctx)
		}
	}
	if err != nil {
		return nil, errors.Wrap(err, "clone transfer")
	}

	return file, nil
}

type writeAt struct {
	f      io.WriterAt
	opts   cloneOptions
	notify func(int64)
}

func (w writeAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := w.f.WriteAt(p, off)
	if err != nil {
		return 0, err
	}

	w.opts.progress.add(int64(n))
	w.notify(int64(n))

	return n, nil
}

type uploaded struct {
	opts   cloneOptions
	prev   *atomic.Int64
	notify func(int64)
}

func (u uploaded) Chunk(_ context.Context, state uploader.ProgressState) error {
	// Chunk acknowledgements can arrive out of order across upload workers.
	// Publish only the new high-water mark so progress never moves backwards.
	for previous := u.prev.Load(); state.Uploaded > previous; previous = u.prev.Load() {
		if u.prev.CompareAndSwap(previous, state.Uploaded) {
			delta := state.Uploaded - previous
			u.opts.progress.add(delta)
			u.notify(delta)
			break
		}
	}
	return nil
}

func (f *Forwarder) transferProgress(elem Elem, direction string, bytes int64) {
	if progress, ok := f.opts.Progress.(TransferProgress); ok {
		progress.OnTransfer(elem, direction, bytes)
	}
}

func (f *Forwarder) stageProgress(elem Elem, direction string, started bool) {
	if progress, ok := f.opts.Progress.(StageProgress); ok {
		progress.OnStage(elem, direction, started)
	}
}
