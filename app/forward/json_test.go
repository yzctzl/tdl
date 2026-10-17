package forward

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
	"github.com/iyear/tdl/core/forwarder"
)

type jsonElem struct{ peer peers.Peer }

func (e jsonElem) Mode() forwarder.Mode { return forwarder.ModeClone }
func (e jsonElem) From() peers.Peer     { return e.peer }
func (e jsonElem) To() peers.Peer       { return e.peer }
func (e jsonElem) Msg() *tg.Message     { return &tg.Message{ID: 1} }
func (e jsonElem) Thread() int          { return 42 }
func (e jsonElem) AsSilent() bool       { return false }
func (e jsonElem) AsDryRun() bool       { return false }
func (e jsonElem) AsGrouped() bool      { return true }

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, fmt.Errorf("closed output") }

func TestConcurrentJSONEventsKeepDirectionCountersAndConfirmedIDs(t *testing.T) {
	var out bytes.Buffer
	p := newJSONProgress(&out)
	elem := jsonElem{peers.Options{}.Build(nil).Channel(&tg.Channel{ID: 1})}
	p.OnAdd(elem)
	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() { p.OnTransfer(elem, "download", 512); p.OnTransfer(elem, "upload", 256) })
	}
	workers.Wait()
	p.OnSent(elem, []int{10, 11})
	p.OnDone(elem, nil)
	decoder := json.NewDecoder(&out)
	events := 0
	for {
		var event progressEvent
		if err := decoder.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		events++
		if event.Event == "sent" && (len(event.MessageIDs) != 2 || event.MessageIDs[1] != 11) {
			t.Fatal("confirmed IDs missing")
		}
		if event.Event == "done" && (event.Downloaded != 32*512 || event.Uploaded != 32*256) {
			t.Fatal("concurrent byte counter lost updates")
		}
	}
	if events != 67 || p.Err() != nil || len(p.bytes) != 0 {
		t.Fatal("invalid JSON event lifecycle")
	}
	broken := newJSONProgress(brokenWriter{})
	broken.OnAdd(elem)
	if broken.Err() == nil {
		t.Fatal("output failure ignored")
	}
}
