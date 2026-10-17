package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/gotd/td/telegram"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/iyear/tdl/app/forward"
	"github.com/iyear/tdl/core/storage"
	"github.com/iyear/tdl/pkg/consts"
)

func newEnhanceServe() *cobra.Command {
	downloads, uploads, queue := 2, 2, 64
	temporary := ""
	command := &cobra.Command{
		Use: "serve", Short: "Keep one account open for bounded JSON-lines tasks on stdin/stdout",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			workers := viper.GetInt(consts.FlagLimit)
			if workers < 1 || queue < 0 || downloads < 1 || uploads < 1 {
				return fmt.Errorf("invalid resident concurrency settings")
			}
			return tRun(command.Context(), func(ctx context.Context, client *telegram.Client, kvd storage.Storage) error {
				return forward.Serve(ctx, client, kvd, os.Stdin, os.Stdout, workers, queue, downloads, uploads, temporary)
			})
		},
	}
	command.Flags().IntVar(&downloads, "downloads", 2, "account-wide simultaneous file downloads")
	command.Flags().IntVar(&uploads, "uploads", 2, "account-wide simultaneous file uploads")
	command.Flags().IntVar(&queue, "queue", 64, "maximum queued tasks beyond the active worker limit")
	command.Flags().StringVar(&temporary, "temp-dir", "", "parent directory for per-task temporary files")
	return command
}
