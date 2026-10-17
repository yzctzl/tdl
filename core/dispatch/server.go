// Package dispatch owns bounded resident tasks independently of Telegram RPCs.
package dispatch

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Request IDs increase monotonically for one connection, including controls.
// This rejects replay without retaining an unbounded history of finished tasks.
type Request struct {
	ID       uint64 `json:"id"`
	Method   string `json:"method"`
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
	Target   uint64 `json:"target,omitempty"`
	DryRun   bool   `json:"dry_run,omitempty"`
	Reupload bool   `json:"reupload,omitempty"`
}

type Event struct {
	Time  time.Time `json:"time"`
	ID    uint64    `json:"id"`
	Event string    `json:"event"`
	Data  any       `json:"data,omitempty"`
	Error string    `json:"error,omitempty"`
}

type Handler func(context.Context, Request, io.Writer) error

type Options struct {
	Workers, Queue int
	Info           any
}

type task struct {
	request Request
	ctx     context.Context
	cancel  context.CancelFunc
}
type completion struct {
	task *task
	err  error
}
type input struct {
	request Request
	err     error
}

type output struct {
	mu      sync.Mutex
	encoder *json.Encoder
	err     error
	cancel  context.CancelFunc
}

func (o *output) send(event Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return o.err
	}
	event.Time = time.Now().UTC()
	if o.err = o.encoder.Encode(event); o.err != nil {
		o.cancel()
	}
	return o.err
}
func (o *output) failure() error { o.mu.Lock(); defer o.mu.Unlock(); return o.err }

type progressWriter struct {
	id  uint64
	out *output
}

func (p progressWriter) Write(data []byte) (int, error) {
	var value struct {
		Event string `json:"event"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return 0, err
	}
	if value.Event == "done" {
		value.Event = "group_done"
	}
	if err := p.out.send(Event{ID: p.id, Event: value.Event, Data: json.RawMessage(data)}); err != nil {
		return 0, err
	}
	return len(data), nil
}

func read(ctx context.Context, reader io.Reader, inputs chan<- input) {
	defer close(inputs)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		var item input
		if json.Unmarshal(scanner.Bytes(), &item.request) != nil {
			item.err = fmt.Errorf("invalid request JSON")
		}
		select {
		case inputs <- item:
		case <-ctx.Done():
			return
		}
	}
	if err := scanner.Err(); err != nil {
		select {
		case inputs <- input{err: err}:
		case <-ctx.Done():
		}
	}
}

// Serve keeps admission/control responsive while individual handlers run.
// EOF drains accepted work; cancellation/output failure cancels it and waits for
// handler cleanup before returning. Only this loop owns the task registry.
func Serve(parent context.Context, reader io.ReadCloser, writer io.Writer, opts Options, handler Handler) error {
	if opts.Workers < 1 || opts.Queue < 0 {
		return fmt.Errorf("workers must be positive and queue size nonnegative")
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer reader.Close()
	out := &output{encoder: json.NewEncoder(writer), cancel: cancel}
	inputs := make(chan input, 1)
	go read(ctx, reader, inputs)
	completions := make(chan completion, opts.Workers)
	active := make(map[uint64]*task)
	pending := make([]*task, 0, opts.Queue)
	running := 0
	var last uint64
	finish := func(job *task, err error) {
		job.cancel()
		delete(active, job.request.ID)
		data := map[string]string{"status": "succeeded"}
		value := Event{ID: job.request.ID, Event: "complete", Data: data}
		if err != nil {
			value.Error = err.Error()
			data["status"] = "failed"
			if errors.Is(err, context.Canceled) {
				data["status"] = "cancelled"
			}
		}
		_ = out.send(value)
	}
	_ = out.send(Event{Event: "ready", Data: map[string]any{"protocol": 1, "workers": opts.Workers, "queue": opts.Queue, "info": opts.Info}})
	for inputs != nil || len(active) > 0 {
		// Admission precedes dispatch, so accepted always appears before progress.
		for ctx.Err() == nil && running < opts.Workers && len(pending) > 0 {
			job := pending[0]
			pending = pending[1:]
			running++
			_ = out.send(Event{ID: job.request.ID, Event: "running"})
			go func() {
				err := handler(job.ctx, job.request, progressWriter{id: job.request.ID, out: out})
				completions <- completion{job, err}
			}()
		}
		if ctx.Err() != nil {
			for _, job := range active {
				job.cancel()
			}
			for _, job := range pending {
				finish(job, context.Canceled)
			}
			pending = nil
			for running > 0 {
				done := <-completions
				running--
				finish(done.task, done.err)
			}
			if err := out.failure(); err != nil {
				return err
			}
			return parent.Err()
		}
		select {
		case <-ctx.Done():
		case done := <-completions:
			running--
			finish(done.task, done.err)
		case item, ok := <-inputs:
			if !ok {
				inputs = nil
				continue
			}
			request := item.request
			reject := func(err error) { _ = out.send(Event{ID: request.ID, Event: "rejected", Error: err.Error()}) }
			if item.err != nil {
				reject(item.err)
				continue
			}
			if request.ID == 0 || request.ID <= last {
				reject(fmt.Errorf("request ID must increase"))
				continue
			}
			last = request.ID
			switch request.Method {
			case "ping":
				_ = out.send(Event{ID: request.ID, Event: "pong"})
			case "cancel":
				job, ok := active[request.Target]
				if !ok {
					reject(fmt.Errorf("task is no longer active"))
					continue
				}
				job.cancel()
				_ = out.send(Event{ID: request.ID, Event: "cancelling", Data: map[string]uint64{"target": request.Target}})
				for index, queued := range pending {
					if queued == job {
						pending = append(pending[:index], pending[index+1:]...)
						finish(job, context.Canceled)
						break
					}
				}
			case "clone":
				if request.From == "" {
					reject(fmt.Errorf("source URL is required"))
					continue
				}
				if len(active) >= opts.Workers+opts.Queue {
					reject(fmt.Errorf("task queue is full"))
					continue
				}
				jobCtx, jobCancel := context.WithCancel(ctx)
				job := &task{request: request, ctx: jobCtx, cancel: jobCancel}
				active[request.ID] = job
				pending = append(pending, job)
				_ = out.send(Event{ID: request.ID, Event: "accepted"})
			default:
				reject(fmt.Errorf("unknown method"))
			}
		}
	}
	return out.failure()
}
