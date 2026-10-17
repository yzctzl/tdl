package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/iyear/tdl/pkg/consts"
)

// Auto sizing covers both directions' configured chunk demand. Pool connections
// are shared on demand; this is capacity, not a per-task connection reservation.
func enhanceConcurrency(cmd *cobra.Command, downloads, uploads int) (workers, down, up, pool int, err error) {
	workers = viper.GetInt(consts.FlagLimit)
	threads := viper.GetInt(consts.FlagThreads)
	pool = viper.GetInt(consts.FlagPoolSize)
	if workers < 1 || threads < 1 || downloads < 0 || uploads < 0 || pool < 0 {
		err = fmt.Errorf("workers and threads must be positive; downloads, uploads and pool must be nonnegative")
		return
	}
	down, up = downloads, uploads
	if down == 0 {
		down = workers
	}
	if up == 0 {
		up = workers
	}
	envValue, envPool := os.LookupEnv("TDL_POOL")
	explicit := cmd.Flags().Changed(consts.FlagPoolSize) || cmd.Root().PersistentFlags().Changed(consts.FlagPoolSize) || viper.InConfig(consts.FlagPoolSize) || (envPool && envValue != "")
	if !explicit {
		pool = max(8*workers, threads*(down+up))
	}
	return
}
