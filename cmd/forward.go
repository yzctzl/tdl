package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/gotd/td/telegram"
	"github.com/spf13/cobra"

	"github.com/iyear/tdl/app/forward"
	"github.com/iyear/tdl/core/forwarder"
	"github.com/iyear/tdl/core/logctx"
	"github.com/iyear/tdl/core/storage"
)

func NewForward() *cobra.Command { return newForward(false) }

// NewEnhance is an opt-in prototype; the normal forward command is unchanged.
func NewEnhance() *cobra.Command { return newForward(true) }

func newForward(enhanced bool) *cobra.Command {
	var opts forward.Options
	if enhanced {
		opts.Mode = forwarder.ModeClone
	}

	cmd := &cobra.Command{
		Use:     "forward",
		Short:   "Forward messages with automatic fallback and message routing",
		GroupID: groupTools.ID,
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.Duplex && (!opts.JSON || opts.Mode != forwarder.ModeClone) {
				return fmt.Errorf("duplex requires --json and --mode clone")
			}
			if opts.Duplex && (opts.Downloads < 1 || opts.Uploads < 1) {
				return fmt.Errorf("downloads and uploads must be positive")
			}
			return tRun(cmd.Context(), func(ctx context.Context, c *telegram.Client, kvd storage.Storage) error {
				return forward.Run(logctx.Named(ctx, "forward"), c, kvd, opts)
			})
		},
	}

	cmd.Flags().StringArrayVar(&opts.From, "from", []string{}, "messages to be forwarded, can be links or exported JSON files")
	cmd.Flags().StringVar(&opts.To, "to", "", "destination peer, can be a CHAT or router based on expression engine")
	cmd.Flags().StringVar(&opts.Edit, "edit", "", "edit message or caption with expression engine. Empty means no edit")
	cmd.Flags().Var(&opts.Mode, "mode", fmt.Sprintf("forward mode: [%s]", strings.Join(forwarder.ModeNames(), ", ")))
	cmd.Flags().BoolVar(&opts.Silent, "silent", false, "send messages silently")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "do not actually send messages, just show how they would be sent")
	cmd.Flags().BoolVar(&opts.Single, "single", false, "do not automatically detect and forward grouped messages")
	cmd.Flags().BoolVar(&opts.Desc, "desc", false, "forward messages in reverse order for each input peer")

	cmd.Flags().BoolVar(&opts.Duplex, "duplex", enhanced, "overlap downloads and uploads within one account (experimental)")
	cmd.Flags().BoolVar(&opts.JSON, "json", enhanced, "write structured progress and confirmed results as JSON lines")
	cmd.Flags().IntVar(&opts.Downloads, "downloads", 2, "maximum simultaneously downloading files in duplex mode")
	cmd.Flags().IntVar(&opts.Uploads, "uploads", 2, "maximum simultaneously uploading files in duplex mode")
	cmd.Flags().BoolVar(&opts.RemoveSpoilers, "remove-spoilers", enhanced, "remove media and text spoilers from destination copies")
	cmd.Flags().BoolVar(&opts.Reupload, "reupload", false, "force byte transfer for unprotected media (benchmarking)")
	if enhanced {
		cmd.AddCommand(newEnhanceServe())
		cmd.Use = "enhance"
		cmd.Short = "Experimental full-duplex clone with native albums and JSON results"
	}
	return cmd
}
