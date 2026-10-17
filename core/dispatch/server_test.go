package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type harness struct {
	input  *io.PipeWriter
	events chan Event
	done   chan error
	ctx    context.Context
}

func start(t *testing.T, opts Options, handler Handler) *harness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	input, write := io.Pipe()
	read, output := io.Pipe()
	h := &harness{write, make(chan Event, 128), make(chan error, 1), ctx}
	go func() { h.done <- Serve(ctx, input, output, opts, handler); output.Close() }()
	go func() {
		defer close(h.events)
		decoder := json.NewDecoder(read)
		for {
			var event Event
			if decoder.Decode(&event) != nil {
				return
			}
			h.events <- event
		}
	}()
	t.Cleanup(func() { cancel(); write.Close(); read.Close() })
	h.expect(t, 0, "ready")
	return h
}
func (h *harness) send(t *testing.T, r Request) {
	t.Helper()
	if err := json.NewEncoder(h.input).Encode(r); err != nil {
		t.Fatal(err)
	}
}
func (h *harness) expect(t *testing.T, id uint64, kind string) Event {
	t.Helper()
	for {
		select {
		case event, ok := <-h.events:
			if !ok {
				t.Fatalf("stream ended waiting for %d %s", id, kind)
			}
			if event.ID == id && event.Event == kind {
				return event
			}
		case <-h.ctx.Done():
			t.Fatalf("timeout waiting for %d %s", id, kind)
		}
	}
}
func status(event Event) string { return event.Data.(map[string]any)["status"].(string) }

func TestControlsRemainResponsiveAndCancellationFreesQueuedCapacity(t *testing.T) {
	release := make(chan struct{})
	cleaned := make(chan struct{})
	h := start(t, Options{Workers: 2, Queue: 1}, func(ctx context.Context, r Request, _ io.Writer) error {
		switch r.ID {
		case 1:
			defer close(cleaned)
			<-ctx.Done()
			return ctx.Err()
		case 2:
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	h.send(t, Request{ID: 1, Method: "clone", From: "first"})
	h.expect(t, 1, "accepted")
	h.send(t, Request{ID: 2, Method: "clone", From: "second"})
	h.expect(t, 2, "running")
	h.send(t, Request{ID: 3, Method: "clone", From: "queued"})
	h.expect(t, 3, "accepted")
	h.send(t, Request{ID: 4, Method: "clone", From: "full"})
	h.expect(t, 4, "rejected")
	h.send(t, Request{ID: 5, Method: "cancel", Target: 3})
	h.expect(t, 5, "cancelling")
	if status(h.expect(t, 3, "complete")) != "cancelled" {
		t.Fatal("queued task not cancelled")
	}
	h.send(t, Request{ID: 6, Method: "clone", From: "replacement"})
	h.expect(t, 6, "accepted")
	h.send(t, Request{ID: 7, Method: "ping"})
	h.expect(t, 7, "pong")
	h.send(t, Request{ID: 8, Method: "cancel", Target: 1})
	h.expect(t, 8, "cancelling")
	if status(h.expect(t, 1, "complete")) != "cancelled" {
		t.Fatal("running task not cancelled")
	}
	select {
	case <-cleaned:
	default:
		t.Fatal("reported cancellation before cleanup")
	}
	if status(h.expect(t, 6, "complete")) != "succeeded" {
		t.Fatal("cancelling one task affected another")
	}
	h.send(t, Request{ID: 4, Method: "clone", From: "replay"})
	h.expect(t, 4, "rejected")
	h.input.Close()
	select {
	case err := <-h.done:
		t.Fatalf("EOF did not drain active work: %v", err)
	default:
	}
	close(release)
	if status(h.expect(t, 2, "complete")) != "succeeded" {
		t.Fatal("unrelated task failed")
	}
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-h.ctx.Done():
		t.Fatal("server did not finish draining")
	}
}

type brokenOutput struct{}

func (brokenOutput) Write([]byte) (int, error) { return 0, fmt.Errorf("closed output") }
func TestOutputFailureStopsInsteadOfRunningUnacknowledgedWork(t *testing.T) {
	err := Serve(context.Background(), io.NopCloser(strings.NewReader(`{"id":1,"method":"clone","from":"source"}`+"\n")), brokenOutput{}, Options{Workers: 1}, func(context.Context, Request, io.Writer) error {
		t.Error("task started without acceptance output")
		return nil
	})
	if err == nil {
		t.Fatal("lost protocol output ignored")
	}
}

func TestInputErrorsAndProgressAreFramedWithoutReplaying(t *testing.T) {
	h := start(t, Options{Workers: 1, Queue: 1}, func(_ context.Context, _ Request, out io.Writer) error {
		_, err := io.WriteString(out, "{\"event\":\"sent\",\"message_ids\":[10]}\n")
		return err
	})
	if _, err := io.WriteString(h.input, "not-json\n"); err != nil {
		t.Fatal(err)
	}
	h.expect(t, 0, "rejected")
	h.send(t, Request{ID: 1, Method: "unknown"})
	h.expect(t, 1, "rejected")
	h.send(t, Request{ID: 2, Method: "clone", From: "source"})
	h.expect(t, 2, "accepted")
	event := h.expect(t, 2, "sent")
	if len(event.Data.(map[string]any)["message_ids"].([]any)) != 1 {
		t.Fatal("progress lost its request identity")
	}
	h.expect(t, 2, "complete")
	h.send(t, Request{ID: 2, Method: "clone", From: "replay"})
	h.expect(t, 2, "rejected")
	h.send(t, Request{ID: 3, Method: "cancel", Target: 2})
	h.expect(t, 3, "rejected")
	h.input.Close()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-h.ctx.Done():
		t.Fatal("server did not exit")
	}
}

func TestThirtyTwoWorkersShareOneServerAndFailureStaysLocal(t *testing.T) {
	var active, peak atomic.Int32
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h := start(t, Options{Workers: 32, Queue: 64}, func(ctx context.Context, r Request, _ io.Writer) error {
		current := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); current > old; old = peak.Load() {
			if peak.CompareAndSwap(old, current) {
				break
			}
		}
		if current == 32 {
			once.Do(func() { close(reached) })
		}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		if r.ID == 1 {
			return fmt.Errorf("one source unavailable")
		}
		return nil
	})
	for id := uint64(1); id <= 64; id++ {
		h.send(t, Request{ID: id, Method: "clone", From: "source"})
	}
	h.expect(t, 64, "accepted")
	select {
	case <-reached:
	case <-h.ctx.Done():
		t.Fatal("configured concurrency not reached")
	}
	h.input.Close()
	close(release)
	succeeded, failed := 0, 0
	for succeeded+failed < 64 {
		select {
		case e := <-h.events:
			if e.Event == "complete" {
				switch status(e) {
				case "succeeded":
					succeeded++
				case "failed":
					failed++
				default:
					t.Fatal("unrelated task cancelled")
				}
			}
		case <-h.ctx.Done():
			t.Fatal("tasks did not drain")
		}
	}
	if succeeded != 63 || failed != 1 || peak.Load() != 32 || active.Load() != 0 {
		t.Fatal("incorrect worker isolation or bound")
	}
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-h.ctx.Done():
		t.Fatal("server did not close")
	}
}
