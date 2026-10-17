package forward

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/gotd/td/telegram"
	"github.com/iyear/tdl/core/dispatch"
	"github.com/iyear/tdl/core/forwarder"
	"github.com/iyear/tdl/core/storage"
)

// Serve uses one logged-in client for the whole JSON-lines connection.
func Serve(ctx context.Context, client *telegram.Client, kvd storage.Storage, input io.ReadCloser, output io.Writer, workers, queue, downloads, uploads, pool int, temporary string) (rerr error) {
	if workers < 1 || queue < 0 || downloads < 1 || uploads < 1 || pool < 0 {
		return fmt.Errorf("invalid resident concurrency settings")
	}
	session := newSession(ctx, client, kvd, pool)
	defer func() {
		if err := session.Close(); rerr == nil {
			rerr = err
		}
	}()
	session.transfers = forwarder.NewTransferPool(downloads, uploads)
	me, err := session.manager.Self(ctx)
	if err != nil {
		return err
	}
	username, _ := me.Username()
	info := map[string]any{"user_id": me.ID(), "username": username, "downloads": downloads, "uploads": uploads, "pool_size": pool}
	return dispatch.Serve(ctx, input, output, dispatch.Options{Workers: workers, Queue: queue, Info: info}, func(ctx context.Context, request dispatch.Request, output io.Writer) error {
		directory, err := os.MkdirTemp(temporary, "enhance-job-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(directory)
		return session.forward(ctx, Options{
			From: []string{request.From}, To: request.To, Mode: forwarder.ModeClone,
			Duplex: true, JSON: true, Downloads: downloads, Uploads: uploads,
			RemoveSpoilers: true, DryRun: request.DryRun, Reupload: request.Reupload,
		}, directory, output)
	})
}
