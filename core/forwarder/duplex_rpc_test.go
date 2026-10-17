package forwarder

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
)

type testElem struct {
	from, to peers.Peer
	message  *tg.Message
}

func (e testElem) Mode() Mode       { return ModeClone }
func (e testElem) From() peers.Peer { return e.from }
func (e testElem) To() peers.Peer   { return e.to }
func (e testElem) Msg() *tg.Message { return e.message }
func (e testElem) Thread() int      { return 42 }
func (e testElem) AsSilent() bool   { return true }
func (e testElem) AsDryRun() bool   { return false }
func (e testElem) AsGrouped() bool  { return false }

type fakePool struct{ client *tg.Client }

func (p fakePool) Client(context.Context, int) *tg.Client  { return p.client }
func (p fakePool) Default(context.Context) *tg.Client      { return p.client }
func (p fakePool) Takeout(context.Context, int) *tg.Client { return p.client }
func (p fakePool) Close() error                            { return nil }

type rpcTransport struct {
	mu                         sync.Mutex
	firstUpload, laterDownload chan struct{}
	uploadOnce, downloadOnce   sync.Once
	reads                      atomic.Int32
	uploads                    map[int64]int64
	prepared                   map[int64]tg.InputMediaClass
	sent                       []tg.InputSingleMedia
	fail                       bool
}

func (r *rpcTransport) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	var result bin.Encoder
	switch request := input.(type) {
	case *tg.UploadGetFileRequest:
		if r.reads.Add(1) > 2 {
			select {
			case <-r.firstUpload:
			case <-ctx.Done():
				return ctx.Err()
			}
			r.downloadOnce.Do(func() { close(r.laterDownload) })
		}
		size := int64(64 * 1024)
		switch loc := request.Location.(type) {
		case *tg.InputDocumentFileLocation:
			if loc.ThumbSize == "" {
				size = 1024 * 1024
			} else {
				size = 4096
			}
		}
		length := min(int64(request.Limit), max(0, size-request.Offset))
		result = &tg.UploadFile{Type: &tg.StorageFileJpeg{}, Bytes: make([]byte, length)}
	case *tg.UploadSaveFilePartRequest:
		r.uploadOnce.Do(func() { close(r.firstUpload) })
		select {
		case <-r.laterDownload:
		case <-ctx.Done():
			return ctx.Err()
		}
		if r.fail {
			return fmt.Errorf("test upload rejected")
		}
		r.mu.Lock()
		r.uploads[request.FileID] += int64(len(request.Bytes))
		r.mu.Unlock()
		result = &tg.BoolTrue{}
	case *tg.MessagesUploadMediaRequest:
		r.mu.Lock()
		id := int64(len(r.prepared) + 100)
		r.prepared[id] = request.Media
		switch media := request.Media.(type) {
		case *tg.InputMediaUploadedPhoto:
			result = &tg.MessageMediaPhoto{Photo: &tg.Photo{ID: id, AccessHash: 1}}
		case *tg.InputMediaUploadedDocument:
			result = &tg.MessageMediaDocument{Document: &tg.Document{ID: id, AccessHash: 1, Size: r.uploads[media.File.(*tg.InputFile).ID], MimeType: media.MimeType, Attributes: media.Attributes}}
		default:
			r.mu.Unlock()
			return fmt.Errorf("unexpected preparation %T", request.Media)
		}
		r.mu.Unlock()
	case *tg.MessagesSendMultiMediaRequest:
		r.mu.Lock()
		r.sent = append(r.sent, request.MultiMedia...)
		r.mu.Unlock()
		updates := make([]tg.UpdateClass, len(request.MultiMedia))
		for i, m := range request.MultiMedia {
			updates[len(updates)-1-i] = &tg.UpdateMessageID{RandomID: m.RandomID, ID: 500 + i}
		}
		result = &tg.Updates{Updates: updates}
	default:
		return fmt.Errorf("unexpected RPC %T", request)
	}
	var buffer bin.Buffer
	if err := result.Encode(&buffer); err != nil {
		return err
	}
	return output.Decode(&buffer)
}

type captureProgress struct {
	mu  sync.Mutex
	ids []int
}

func (*captureProgress) OnAdd(Elem)                  {}
func (*captureProgress) OnClone(Elem, ProgressState) {}
func (*captureProgress) OnDone(Elem, error)          {}
func (p *captureProgress) OnSent(_ Elem, ids []int)  { p.mu.Lock(); defer p.mu.Unlock(); p.ids = ids }

// Exercise actual gotd chunk download/upload, native preparation, original video
// attributes and thumbnail, album submission and acknowledgement decoding.
func TestNativeMixedAlbumDuplexAndFailedUploadCleanup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "mixed_album", true: "failed_upload"}[fail], func(t *testing.T) {
			directory := t.TempDir()
			t.Setenv("TMPDIR", directory)
			t.Setenv("TMP", directory)
			t.Setenv("TEMP", directory)
			rpc := &rpcTransport{firstUpload: make(chan struct{}), laterDownload: make(chan struct{}), uploads: make(map[int64]int64), prepared: make(map[int64]tg.InputMediaClass), fail: fail}
			manager := peers.Options{}.Build(nil)
			rawSource := &tg.Channel{ID: 1, Noforwards: true}
			rawSource.SetFlags()
			source := manager.Channel(rawSource)
			target := manager.Channel(&tg.Channel{ID: 2})
			messages := make([]*tg.Message, 6)
			for i := range messages {
				media := &tg.MessageMediaPhoto{Spoiler: true, Photo: &tg.Photo{ID: int64(i + 1), DCID: 1, Sizes: []tg.PhotoSizeClass{&tg.PhotoSize{Type: "x", W: 800, H: 600, Size: 64 * 1024}}}}
				messages[i] = &tg.Message{ID: i + 1, Message: fmt.Sprintf("caption %d", i), Media: media, Entities: []tg.MessageEntityClass{&tg.MessageEntitySpoiler{Offset: 0, Length: 3}, &tg.MessageEntityBold{Offset: 0, Length: 3}}}
				messages[i].SetFlags()
			}
			document := &tg.Document{ID: 6, DCID: 1, Size: 1024 * 1024, MimeType: "video/mp4", Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeVideo{W: 1920, H: 1080, Duration: 30, SupportsStreaming: true}}, Thumbs: []tg.PhotoSizeClass{&tg.PhotoSize{Type: "m", W: 320, H: 180, Size: 4096}}}
			document.SetFlags()
			messages[5].Media = &tg.MessageMediaDocument{Spoiler: true, Document: document}
			reporter := &captureProgress{}
			f := New(Options{Pool: fakePool{tg.NewClient(rpc)}, Threads: 2, Duplex: &DuplexOptions{Groups: 2, Downloads: 2, Uploads: 2}, RemoveSpoilers: true, Progress: reporter})
			err := f.forwardMessage(testContext(t), testElem{source, target, messages[0]}, messages...)
			files, _ := os.ReadDir(directory)
			if len(files) != 0 {
				t.Fatal("native temporary files leaked")
			}
			if fail {
				if err == nil || len(rpc.sent) != 0 {
					t.Fatalf("partial album was submitted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(rpc.sent) != 6 || len(reporter.ids) != 6 {
				t.Fatal("missing album members or receipts")
			}
			for i, m := range rpc.sent {
				if m.Message != fmt.Sprintf("caption %d", i) || reporter.ids[i] != 500+i || len(m.Entities) != 1 {
					t.Fatal("album order, captions, formatting or IDs changed")
				}
				switch media := m.Media.(type) {
				case *tg.InputMediaPhoto:
					if media.GetSpoiler() {
						t.Fatal("photo spoiler retained")
					}
				case *tg.InputMediaDocument:
					if media.GetSpoiler() {
						t.Fatal("video spoiler retained")
					}
				}
			}
			var video *tg.InputMediaUploadedDocument
			for _, media := range rpc.prepared {
				if value, ok := media.(*tg.InputMediaUploadedDocument); ok {
					video = value
				}
			}
			if video == nil || video.Thumb == nil {
				t.Fatal("original video preview lost")
			}
			attr := video.Attributes[0].(*tg.DocumentAttributeVideo)
			if attr.W != 1920 || attr.H != 1080 || attr.Duration != 30 || !attr.SupportsStreaming {
				t.Fatal("video metadata changed")
			}
			if len(f.pipeline.downloads) != 0 || len(f.pipeline.uploads) != 0 {
				t.Fatal("direction slots leaked")
			}
		})
	}
}

type elemIter struct {
	values []Elem
	index  int
}

func (i *elemIter) Next(ctx context.Context) bool {
	if ctx.Err() != nil || i.index == len(i.values) {
		return false
	}
	i.index++
	return true
}
func (i *elemIter) Value() Elem { return i.values[i.index-1] }
func (i *elemIter) Err() error  { return nil }

type concurrentRPC struct {
	active, sent, max atomic.Int32
	capacity          int32
	reached           chan struct{}
	release           sync.Once
}

func (r *concurrentRPC) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	request, ok := input.(*tg.MessagesSendMessageRequest)
	if !ok {
		return fmt.Errorf("unexpected RPC %T", input)
	}
	active := r.active.Add(1)
	defer r.active.Add(-1)
	for old := r.max.Load(); active > old; old = r.max.Load() {
		if r.max.CompareAndSwap(old, active) {
			break
		}
	}
	if active == r.capacity {
		r.release.Do(func() { close(r.reached) })
	}
	select {
	case <-r.reached:
	case <-ctx.Done():
		return ctx.Err()
	}
	id := r.sent.Add(1)
	var buffer bin.Buffer
	result := &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateMessageID{RandomID: request.RandomID, ID: int(id)}}}
	if err := result.Encode(&buffer); err != nil {
		return err
	}
	return output.Decode(&buffer)
}

func TestGroupConcurrencyHonorsBudgetAndDeduplicatesSources(t *testing.T) {
	rpc := &concurrentRPC{capacity: 8, reached: make(chan struct{})}
	manager := peers.Options{}.Build(nil)
	peer := manager.Channel(&tg.Channel{ID: 1})
	iter := &elemIter{}
	for index := range 32 {
		iter.values = append(iter.values, testElem{peer, peer, &tg.Message{ID: index + 1, Message: "text"}})
	}
	iter.values = append(iter.values, iter.values[0])
	f := New(Options{Pool: fakePool{tg.NewClient(rpc)}, Iter: iter, Progress: &captureProgress{}, Duplex: &DuplexOptions{Groups: 8, Downloads: 2, Uploads: 2}})
	if err := f.Forward(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if rpc.sent.Load() != 32 || rpc.max.Load() != 8 {
		t.Fatalf("incorrect concurrency/dedup: sent=%d maximum=%d", rpc.sent.Load(), rpc.max.Load())
	}
}
