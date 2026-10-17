package recovery

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

func TestConcurrentCallsOwnTheirBackoff(t *testing.T) {
	var created atomic.Int32
	middleware := New(context.Background(), func() backoff.BackOff {
		created.Add(1)
		return backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 2)
	})
	var attempts sync.Map
	next := telegram.InvokeFunc(func(_ context.Context, input bin.Encoder, _ bin.Decoder) error {
		counter, _ := attempts.LoadOrStore(input, &atomic.Int32{})
		if counter.(*atomic.Int32).Add(1) < 3 {
			return errors.New("connection interrupted")
		}
		return nil
	})
	invoke := middleware.Handle(next)
	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() {
			if err := invoke(context.Background(), &tg.MessagesGetHistoryRequest{Limit: 1}, nil); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	if created.Load() != 32 {
		t.Fatal("backoff shared between RPCs")
	}
}

func TestCancellationInterruptsRecoveryDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := make(chan struct{})
	var once sync.Once
	invoke := New(ctx, func() backoff.BackOff { return backoff.NewConstantBackOff(time.Hour) }).Handle(telegram.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		once.Do(func() { close(called) })
		return errors.New("connection interrupted")
	}))
	done := make(chan error, 1)
	go func() { done <- invoke(ctx, &tg.MessagesGetHistoryRequest{Limit: 1}, nil) }()
	<-called
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled RPC succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("recovery delay ignored cancellation")
	}
}
