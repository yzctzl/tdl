package forward

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/iyear/tdl/core/forwarder"
	"github.com/iyear/tdl/core/tmedia"
)

// jsonProgress serializes callbacks from all groups and both transfer directions.
// Per-group counters are updated under the same lock as their output record.
type jsonProgress struct {
	mu     sync.Mutex
	out    *json.Encoder
	bytes  map[tuple][2]int64
	active map[tuple][2]int
	err    error
}

type progressEvent struct {
	Status          string      `json:"status,omitempty"`
	Event           string      `json:"event"`
	Time            time.Time   `json:"time"`
	Source          int64       `json:"source"`
	Message         int         `json:"message"`
	Target          int64       `json:"target"`
	Topic           int         `json:"topic,omitempty"`
	Direction       string      `json:"direction,omitempty"`
	Downloaded      int64       `json:"downloaded"`
	Uploaded        int64       `json:"uploaded"`
	MessageIDs      []int       `json:"message_ids,omitempty"`
	Error           string      `json:"error,omitempty"`
	DryRun          bool        `json:"dry_run,omitempty"`
	Media           []mediaPlan `json:"media,omitempty"`
	ActiveDownloads int         `json:"active_downloads"`
	ActiveUploads   int         `json:"active_uploads"`
}

func newJSONProgress(out io.Writer) *jsonProgress {
	return &jsonProgress{out: json.NewEncoder(out), bytes: make(map[tuple][2]int64), active: make(map[tuple][2]int)}
}

func (p *jsonProgress) emit(elem forwarder.Elem, event, direction string, delta int64, ids []int, failure error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := tuple{from: elem.From().ID(), msg: elem.Msg().ID, to: elem.To().ID()}
	count := p.bytes[key]
	if direction == "download" {
		count[0] += delta
	}
	if direction == "upload" {
		count[1] += delta
	}
	p.bytes[key] = count
	active := p.active[key]
	index := 0
	if direction == "upload" {
		index = 1
	}
	if event == "stage_start" {
		active[index]++
	}
	if event == "stage_end" {
		active[index]--
	}
	p.active[key] = active
	value := progressEvent{ActiveDownloads: active[0], ActiveUploads: active[1], Event: event, Time: time.Now().UTC(), Source: key.from, Message: key.msg, Target: key.to, Topic: elem.Thread(), Direction: direction, Downloaded: count[0], Uploaded: count[1], MessageIDs: ids, DryRun: elem.AsDryRun()}
	if failure != nil {
		value.Error = failure.Error()
	}
	if event == "done" {
		value.Status = "succeeded"
		if failure != nil {
			value.Status = "failed"
			if errors.Is(failure, context.Canceled) {
				value.Status = "cancelled"
			}
		}
	}
	if p.err == nil {
		p.err = p.out.Encode(value)
	}
	if event == "done" {
		delete(p.bytes, key)
		delete(p.active, key)
	}
}

func (p *jsonProgress) OnAdd(elem forwarder.Elem)                           { p.emit(elem, "start", "", 0, nil, nil) }
func (p *jsonProgress) OnClone(_ forwarder.Elem, _ forwarder.ProgressState) {}
func (p *jsonProgress) OnTransfer(elem forwarder.Elem, direction string, bytes int64) {
	p.emit(elem, "progress", direction, bytes, nil, nil)
}
func (p *jsonProgress) OnSent(elem forwarder.Elem, ids []int) { p.emit(elem, "sent", "", 0, ids, nil) }
func (p *jsonProgress) OnDone(elem forwarder.Elem, err error) { p.emit(elem, "done", "", 0, nil, err) }
func (p *jsonProgress) Err() error                            { p.mu.Lock(); defer p.mu.Unlock(); return p.err }

type mediaPlan struct {
	ID        int     `json:"id"`
	Kind      string  `json:"kind"`
	Bytes     int64   `json:"bytes"`
	Width     int     `json:"width,omitempty"`
	Height    int     `json:"height,omitempty"`
	Duration  float64 `json:"duration,omitempty"`
	Thumbnail bool    `json:"thumbnail,omitempty"`
}

func (p *jsonProgress) OnPlan(elem forwarder.Elem, messages []*tg.Message) {
	value := progressEvent{Event: "plan", Time: time.Now().UTC(), Source: elem.From().ID(), Message: elem.Msg().ID, Target: elem.To().ID(), Topic: elem.Thread(), DryRun: elem.AsDryRun()}
	for _, msg := range messages {
		item := mediaPlan{ID: msg.ID, Kind: "text"}
		if media, ok := tmedia.GetMedia(msg); ok {
			item.Bytes = media.Size
		}
		switch media := msg.Media.(type) {
		case *tg.MessageMediaPhoto:
			item.Kind = "photo"
		case *tg.MessageMediaDocument:
			item.Kind = "document"
			if document, ok := media.Document.(*tg.Document); ok {
				_, item.Thumbnail = tmedia.GetDocumentThumb(document)
				for _, attr := range document.Attributes {
					if video, ok := attr.(*tg.DocumentAttributeVideo); ok {
						item.Kind = "video"
						item.Width = video.W
						item.Height = video.H
						item.Duration = video.Duration
					}
				}
			}
		}
		value.Media = append(value.Media, item)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err == nil {
		p.err = p.out.Encode(value)
	}
}

func (p *jsonProgress) OnStage(elem forwarder.Elem, direction string, started bool) {
	event := "stage_end"
	if started {
		event = "stage_start"
	}
	p.emit(elem, event, direction, 0, nil, nil)
}
