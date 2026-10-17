package forwarder

import (
	"context"
	"sync"
	"testing"

	"github.com/gotd/td/telegram/uploader"
	"go.uber.org/atomic"
)

type byteTotal struct{ value atomic.Int64 }

func (b *byteTotal) add(n int64) { b.value.Add(n) }

func TestUploadProgressAcceptsConcurrentOutOfOrderAcknowledgements(t *testing.T) {
	progress := &byteTotal{}
	var notified atomic.Int64
	reporter := uploaded{opts: cloneOptions{progress: progress}, prev: atomic.NewInt64(0), notify: func(delta int64) {
		if delta <= 0 {
			t.Errorf("nonpositive progress delta: %d", delta)
		}
		notified.Add(delta)
	}}
	var workers sync.WaitGroup
	for i := 1024; i >= 1; i-- {
		workers.Add(1)
		go func(total int64) {
			defer workers.Done()
			_ = reporter.Chunk(context.Background(), uploader.ProgressState{Uploaded: total})
		}(int64(i))
	}
	workers.Wait()
	// A late stale acknowledgement must not lower the final total.
	_ = reporter.Chunk(context.Background(), uploader.ProgressState{Uploaded: 1})
	if progress.value.Load() != 1024 || notified.Load() != 1024 || reporter.prev.Load() != 1024 {
		t.Fatalf("totals: progress=%d notified=%d previous=%d", progress.value.Load(), notified.Load(), reporter.prev.Load())
	}
}
