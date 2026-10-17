package forwarder

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
)

func TestTransferFilePublishesOnlyContiguousBytesAndCancelsReaders(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "stream-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	stream := newTransferFile(file, 8)
	if _, err := stream.WriteAt([]byte("efgh"), 4); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(stream.finish(), io.ErrUnexpectedEOF) {
		t.Fatal("sparse tail treated as a complete file")
	}
	ctx, cancel := context.WithCancel(testContext(t))
	cancel()
	if _, err := (&transferReader{file: stream, ctx: ctx}).Read(make([]byte, 8)); !errors.Is(err, context.Canceled) {
		t.Fatal("blocked reader ignored cancellation", err)
	}
	done := make(chan []byte, 1)
	go func() {
		b, err := io.ReadAll(&transferReader{file: stream, ctx: testContext(t)})
		if err != nil {
			t.Error(err)
		}
		done <- b
	}()
	if _, err := stream.WriteAt([]byte("abcd"), 0); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-done:
		if !bytes.Equal(b, []byte("abcdefgh")) {
			t.Fatalf("uploaded a hole: %q", b)
		}
	case <-testContext(t).Done():
		t.Fatal("contiguous reader did not wake")
	}
	if err := stream.finish(); err != nil {
		t.Fatal(err)
	}
}
