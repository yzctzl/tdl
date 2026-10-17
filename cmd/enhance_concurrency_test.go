package cmd

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/iyear/tdl/pkg/consts"
)

func TestEnhanceCapacityTracksWorkersAndPreservesExplicitPool(t *testing.T) {
	for _, c := range []struct {
		name                       string
		workers, threads, down, up int
		args                       []string
		env                        string
		wantDown, wantUp, wantPool int
	}{
		{"defaults", 4, 4, 0, 0, nil, "", 4, 4, 32},
		{"chunk_capacity", 4, 16, 0, 0, nil, "", 4, 4, 128},
		{"custom_file_limits", 4, 4, 1, 2, nil, "", 1, 2, 32},
		{"explicit_eight", 4, 4, 0, 0, []string{"--pool=8"}, "", 4, 4, 8},
		{"explicit_unlimited", 4, 4, 0, 0, []string{"--pool=0"}, "", 4, 4, 0},
		{"environment", 4, 4, 0, 0, nil, "12", 4, 4, 12},
		{"flag_over_environment", 4, 4, 0, 0, []string{"--pool=8"}, "12", 4, 4, 8},
	} {
		t.Run(c.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			viper.SetDefault(consts.FlagLimit, c.workers)
			viper.SetDefault(consts.FlagThreads, c.threads)
			viper.SetEnvPrefix("tdl")
			viper.AutomaticEnv()
			t.Setenv("TDL_POOL", c.env)
			root := &cobra.Command{Use: "tdl"}
			root.PersistentFlags().Int(consts.FlagPoolSize, 8, "")
			if err := viper.BindPFlags(root.PersistentFlags()); err != nil {
				t.Fatal(err)
			}
			command := NewEnhance()
			root.AddCommand(command)
			if err := command.ParseFlags(c.args); err != nil {
				t.Fatal(err)
			}
			workers, down, up, pool, err := enhanceConcurrency(command, c.down, c.up)
			if err != nil || workers != c.workers || down != c.wantDown || up != c.wantUp || pool != c.wantPool {
				t.Fatalf("capacity: workers=%d down=%d up=%d pool=%d err=%v", workers, down, up, pool, err)
			}
		})
	}
}
