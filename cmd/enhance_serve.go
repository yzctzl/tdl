package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/gotd/td/telegram"
	"github.com/spf13/cobra"

	"github.com/iyear/tdl/app/forward"
	"github.com/iyear/tdl/core/storage"
)

func newEnhanceServe() *cobra.Command {
	downloads, uploads, queue := 0, 0, 64
	temporary := ""
	command := &cobra.Command{
		Use: "serve", Short: "Keep one account open for bounded JSON-lines tasks on stdin/stdout",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			workers, down, up, pool, err := enhanceConcurrency(command, downloads, uploads)
			if err != nil {
				return err
			}
			if queue < 0 {
				return fmt.Errorf("queue must be nonnegative")
			}
			return tRun(command.Context(), func(ctx context.Context, client *telegram.Client, kvd storage.Storage) error {
				return forward.Serve(ctx, client, kvd, os.Stdin, os.Stdout, workers, queue, down, up, pool, temporary)
			})
		},
	}
	command.Flags().IntVar(&downloads, "downloads", 0, "account-wide file downloads; zero follows task concurrency")
	command.Flags().IntVar(&uploads, "uploads", 0, "account-wide file uploads; zero follows task concurrency")
	command.Flags().IntVar(&queue, "queue", 64, "maximum queued tasks beyond the active worker limit")
	command.Flags().StringVar(&temporary, "temp-dir", "", "parent directory for per-task temporary files")
	return command
}
