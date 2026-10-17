package main

import (
	"context"
	"os"
	"os/signal"

	surveyterm "github.com/AlecAivazis/survey/v2/terminal"
	"github.com/fatih/color"
	"github.com/go-faster/errors"
	"go.etcd.io/bbolt"

	"github.com/iyear/tdl/cmd"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	humanizeErrors := map[error]string{
		bbolt.ErrTimeout:        "Current database is used by another process, please terminate it first",
		surveyterm.InterruptErr: "Interrupted",
	}

	if err := cmd.New().ExecuteContext(ctx); err != nil {
		for e, m := range humanizeErrors {
			if errors.Is(err, e) {
				color.New(color.FgRed).Fprintf(os.Stderr, "%s\n", m)
				os.Exit(1)
			}
		}

		color.New(color.FgRed).Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(1)
	}
}
