package forwarder

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/tg"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestDirectionsOverlapAndCancellationReleasesSlots(t *testing.T) {
	ctx := testContext(t)
	pipeline := NewTransferPool(1, 1)
	uploading := make(chan struct{})
	downloaded := make(chan struct{})
	done := make(chan error, 2)
	cancelled, cancel := context.WithCancel(ctx)
	go func() {
		done <- pipeline.transfer(cancelled, func(context.Context) error { return nil }, func(ctx context.Context) error {
			close(uploading)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	<-uploading
	go func() {
		done <- pipeline.transfer(ctx, func(context.Context) error { close(downloaded); return nil }, func(context.Context) error { return nil })
	}()
	select {
	case <-downloaded:
	case <-ctx.Done():
		t.Fatal("download blocked behind an unrelated upload")
	}
	cancel()
	var failures int
	for range 2 {
		select {
		case err := <-done:
			if errors.Is(err, context.Canceled) {
				failures++
			} else if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("cancelled transfer leaked its slot")
		}
	}
	if failures != 1 || len(pipeline.downloads) != 0 || len(pipeline.uploads) != 0 {
		t.Fatal("incorrect cancellation isolation")
	}
}

func TestAlbumPreservesOrderAndFailsAsAUnit(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "order", true: "failure"}[fail], func(t *testing.T) {
			ctx := testContext(t)
			messages := []*tg.Message{{ID: 1}, {ID: 2}, {ID: 3}}
			last := make(chan struct{})
			var completed atomic.Int32
			sentinel := errors.New("transfer rejected")
			result, err := prepareAlbum(ctx, messages, func(ctx context.Context, message *tg.Message) (tg.InputMediaClass, error) {
				if message.ID == 3 {
					close(last)
				} else {
					select {
					case <-last:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				if fail && message.ID == 2 {
					return nil, sentinel
				}
				completed.Add(1)
				return &tg.InputMediaPhoto{ID: &tg.InputPhoto{ID: int64(message.ID)}}, nil
			})
			if fail {
				if result != nil || !errors.Is(err, sentinel) {
					t.Fatalf("partial album escaped: %v", err)
				}
				return
			}
			if err != nil || completed.Load() != 3 {
				t.Fatalf("prepare: %v", err)
			}
			for i, media := range result {
				if media.(*tg.InputMediaPhoto).ID.(*tg.InputPhoto).ID != int64(i+1) {
					t.Fatal("completion order changed album order")
				}
			}
		})
	}
}

func TestAcknowledgementsMatchRandomIDsAndRejectMissingResults(t *testing.T) {
	result := &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateMessageID{RandomID: 22, ID: 102}, &tg.UpdateMessageID{RandomID: 999, ID: 777}, &tg.UpdateMessageID{RandomID: 11, ID: 101}}}
	ids, err := resultIDs(result, []int64{11, 22})
	if err != nil || len(ids) != 2 || ids[0] != 101 || ids[1] != 102 {
		t.Fatalf("incorrect IDs: %v %v", ids, err)
	}
	if _, err := resultIDs(result, []int64{11, 33}); err == nil {
		t.Fatal("unacknowledged upload reported as success")
	}
	ids, err = resultIDs(&tg.UpdateShortSentMessage{ID: 201}, []int64{11})
	if err != nil || ids[0] != 201 {
		t.Fatal("short acknowledgement not recognized")
	}
}

func TestSpoilerRemovalClearsWireFlagsAndPreservesFormatting(t *testing.T) {
	photo := &tg.InputMediaPhoto{}
	photo.SetSpoiler(true)
	document := &tg.InputMediaDocument{}
	document.SetSpoiler(true)
	clearSpoiler(photo)
	clearSpoiler(document)
	if photo.GetSpoiler() || document.GetSpoiler() {
		t.Fatal("spoiler survived serialization flags")
	}
	f := New(Options{RemoveSpoilers: true})
	bold := &tg.MessageEntityBold{Offset: 0, Length: 4}
	entities := f.entities([]tg.MessageEntityClass{&tg.MessageEntitySpoiler{Offset: 0, Length: 4}, bold})
	if len(entities) != 1 || entities[0] != bold {
		t.Fatal("text formatting changed")
	}
}
