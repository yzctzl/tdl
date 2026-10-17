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

	temp, err := os.CreateTemp("", "tdl_*")
	if err != nil {
		return nil, errors.Wrap(err, "create temp file")
	}
	defer func() {
		multierr.AppendInto(&rerr, temp.Close())
		multierr.AppendInto(&rerr, os.Remove(temp.Name()))
	}()

	threads := tutil.BestThreads(opts.media.Size, f.opts.Threads)

	download := func(ctx context.Context) error {
		f.stageProgress(opts.elem, "download", true)
		defer f.stageProgress(opts.elem, "download", false)
		_, err := downloader.NewDownloader().
			WithPartSize(tdownloader.MaxPartSize).
			Download(f.opts.Pool.Client(ctx, opts.media.DC), opts.media.InputFileLoc).
			WithThreads(threads).
			Parallel(ctx, writeAt{f: temp, opts: opts, notify: func(n int64) { f.transferProgress(opts.elem, "download", n) }})
		return err
	}
	var file tg.InputFileClass
	upload := func(ctx context.Context) error {
		f.stageProgress(opts.elem, "upload", true)
		defer f.stageProgress(opts.elem, "upload", false)
		if _, err := temp.Seek(0, io.SeekStart); err != nil {
			return errors.Wrap(err, "seek")
		}
		var err error
		file, err = uploader.NewUploader(f.opts.Pool.Default(ctx)).
			WithPartSize(tuploader.MaxPartSize).
			WithThreads(threads).
			WithProgress(uploaded{opts: opts, prev: atomic.NewInt64(0), notify: func(n int64) { f.transferProgress(opts.elem, "upload", n) }}).
			Upload(ctx, uploader.NewUpload(opts.media.Name, temp, opts.media.Size))
		return err
	}
	if f.pipeline != nil {
		err = f.pipeline.transfer(ctx, download, upload)
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
	delta := state.Uploaded - u.prev.Swap(state.Uploaded)
	u.opts.progress.add(delta)
	u.notify(delta)

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
