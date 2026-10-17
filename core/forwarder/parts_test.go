package forwarder

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/iyear/tdl/core/tmedia"
)

const readyPartsSize = 12*1024*1024 + 37

type partsRPC struct {
	firstUpload chan struct{}
	once        sync.Once
	mu          sync.Mutex
	parts       map[int][]byte
	failure     string
	firstPart   int
	attempts    int
}

func pattern(offset int64, length int) []byte {
	data := make([]byte, length)
	for i := range data {
		data[i] = byte((offset + int64(i)) % 251)
	}
	return data
}
func (r *partsRPC) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	var result bin.Encoder
	switch q := input.(type) {
	case *tg.UploadGetFileRequest:
		if q.Offset == 0 && r.failure == "reordered" {
			select {
			case <-r.firstUpload:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if q.Offset > 0 && r.failure != "reordered" {
			// Later blocks cannot arrive until this very file starts uploading.
			select {
			case <-r.firstUpload:
			case <-ctx.Done():
				return ctx.Err()
			}
			if r.failure == "cancel" {
				<-ctx.Done()
				return ctx.Err()
			}
			if r.failure == "download" {
				return fmt.Errorf("download rejected")
			}
			if r.failure == "short" {
				result = &tg.UploadFile{Type: &tg.StorageFileMp4{}}
				break
			}
		}
		length := min(q.Limit, max(0, readyPartsSize-int(q.Offset)))
		result = &tg.UploadFile{Type: &tg.StorageFileMp4{}, Bytes: pattern(q.Offset, length)}
	case *tg.UploadSaveBigFilePartRequest:
		r.once.Do(func() { r.firstPart = q.FilePart; close(r.firstUpload) })
		if r.failure == "cancel" {
			<-ctx.Done()
			return ctx.Err()
		}
		if r.failure == "upload" {
			return fmt.Errorf("upload rejected")
		}
		offset := int64(q.FilePart) * 512 * 1024
		expected := pattern(offset, int(min(int64(512*1024), readyPartsSize-offset)))
		if !bytes.Equal(q.Bytes, expected) {
			return fmt.Errorf("corrupt part %d", q.FilePart)
		}
		r.mu.Lock()
		r.attempts++
		if r.failure == "retry" && r.attempts == 1 {
			r.mu.Unlock()
			result = &tg.BoolFalse{}
			break
		}
		r.parts[q.FilePart] = bytes.Clone(q.Bytes)
		r.mu.Unlock()
		result = &tg.BoolTrue{}
	default:
		return fmt.Errorf("unexpected RPC %T", input)
	}
	var b bin.Buffer
	if err := result.Encode(&b); err != nil {
		return err
	}
	return output.Decode(&b)
}
func TestReadyPartsUploadOutOfOrderAndCleanUpOnFailure(t *testing.T) {
	for _, failure := range []string{"", "download", "upload", "short", "cancel", "reordered", "retry"} {
		name := failure
		if name == "" {
			name = "success"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			rpc := &partsRPC{firstUpload: make(chan struct{}), parts: make(map[int][]byte), failure: failure}
			f := New(Options{Pool: fakePool{tg.NewClient(rpc)}, Threads: 4, TempDir: filepath.Join(directory, "must-not-be-created"), Duplex: &DuplexOptions{Groups: 1, Downloads: 1, Uploads: 1}})
			ctx := testContext(t)
			if failure == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				go func() {
					select {
					case <-rpc.firstUpload:
						cancel()
					case <-ctx.Done():
					}
				}()
			}
			progress := &byteTotal{}
			result, err := f.cloneMedia(ctx, cloneOptions{media: &tmedia.Media{Name: "video.mp4", Size: readyPartsSize, DC: 1, InputFileLoc: &tg.InputDocumentFileLocation{ID: 1}}, progress: progress}, false)
			if failure != "" && failure != "reordered" && failure != "retry" {
				if err == nil || result != nil {
					t.Fatalf("failure returned a file: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				uploaded, ok := result.(*tg.InputFileBig)
				if !ok || uploaded.Parts != 25 || len(rpc.parts) != 25 || progress.value.Load() != 2*readyPartsSize {
					t.Fatalf("missing parts: %T %+v", result, result)
				}
			}
			if failure == "reordered" && rpc.firstPart == 0 {
				t.Fatal("waited for offset zero")
			}
			files, err := os.ReadDir(directory)
			if err != nil || len(files) != 0 {
				t.Fatal("temporary files leaked", err)
			}
			if len(f.pipeline.downloads) != 0 || len(f.pipeline.uploads) != 0 {
				t.Fatal("transfer slots leaked")
			}
		})
	}
}
