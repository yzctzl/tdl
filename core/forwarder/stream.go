package forwarder

import (
	"context"
	"io"
	"os"
	"sync"
)

// transferFile bridges the downloader's non-overlapping, out-of-order blocks
// to the uploader's sequential reads. Sparse file holes are never published.
// Only block boundaries live in memory; media bytes stay in the temporary file.
type transferFile struct {
	file    *os.File
	size    int64
	mu      sync.Mutex
	ready   map[int64]int64
	prefix  int64
	changed chan struct{}
}

func newTransferFile(file *os.File, size int64) *transferFile {
	return &transferFile{file: file, size: size, ready: make(map[int64]int64), changed: make(chan struct{})}
}

func (f *transferFile) WriteAt(p []byte, offset int64) (int, error) {
	n, err := f.file.WriteAt(p, offset)
	if err != nil || n == 0 {
		return n, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if offset >= f.prefix {
		f.ready[offset] = offset + int64(n)
		for {
			end, ok := f.ready[f.prefix]
			if !ok {
				break
			}
			delete(f.ready, f.prefix)
			f.prefix = end
		}
	}
	close(f.changed)
	f.changed = make(chan struct{})
	return n, nil
}

func (f *transferFile) finish() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.prefix != f.size {
		return io.ErrUnexpectedEOF
	}
	return nil
}

type transferReader struct {
	file   *transferFile
	ctx    context.Context
	offset int64
}

func (r *transferReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.offset == r.file.size {
		return 0, io.EOF
	}
	size := min(int64(len(p)), r.file.size-r.offset)
	if err := r.file.wait(r.ctx, r.offset+size); err != nil {
		return 0, err
	}

	n, err := r.file.file.ReadAt(p[:size], r.offset)
	r.offset += int64(n)
	return n, err
}

func (f *transferFile) wait(ctx context.Context, end int64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		f.mu.Lock()
		ready, changed := f.prefix >= end, f.changed
		f.mu.Unlock()
		if ready {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
