package cmd

import (
	"strings"
	"testing"
)

func TestEnhanceDefaultsAndRejectsIncompatibleModesBeforeConnecting(t *testing.T) {
	for _, enhanced := range []bool{false, true} {
		command := newForward(enhanced)
		for _, name := range []string{"duplex", "json", "remove-spoilers"} {
			value, err := command.Flags().GetBool(name)
			if err != nil || value != enhanced {
				t.Fatalf("incorrect %s default", name)
			}
		}
	}
	for _, args := range [][]string{{"--json=false"}, {"--mode=direct"}, {"--downloads=0"}, {"--uploads=0"}} {
		command := NewEnhance()
		if err := command.ParseFlags(args); err != nil {
			t.Fatal(err)
		}
		err := command.RunE(command, nil)
		if err == nil || (!strings.Contains(err.Error(), "requires") && !strings.Contains(err.Error(), "positive")) {
			t.Fatalf("invalid configuration reached session startup: %v", err)
		}
	}
}
