// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestPBMProcessSpecRendersGoSoftLimitAndLegacyUnknown(t *testing.T) {
	const budget = uint64(384 << 20)
	bounded := pbmProcessSpec(agentapi.MemberConfig{PBMGoMemoryLimitBytes: budget}, "mongodb://fixture")
	if !slices.Contains(bounded.env, "GOMEMLIMIT=402653184") || bounded.goMemoryLimitBytes != budget {
		t.Fatal("PBM launch did not render the derived byte limit")
	}
	legacy := pbmProcessSpec(agentapi.MemberConfig{}, "mongodb://fixture")
	if len(legacy.env) != 1 || legacy.goMemoryLimitBytes != 0 {
		t.Fatal("legacy budget was invented")
	}
}

func TestPBMGoBudgetChangeDoesNotRestartRunningChild(t *testing.T) {
	s := newSupervisor(io.Discard)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	defer s.shutdown(ctx)
	makeSpec := func(budget uint64) processSpec {
		spec := pbmProcessSpec(agentapi.MemberConfig{PBMGoMemoryLimitBytes: budget}, "mongodb://fixture")
		spec.binary, spec.args = "/bin/sleep", []string{"60"}
		return spec
	}
	s.configure("pbm-agent", makeSpec(384<<20))
	if err := s.action(ctx, "pbm-agent", "start"); err != nil {
		t.Fatal(err)
	}
	states, _ := s.snapshot()
	before := states["pbm-agent"]
	if before.GoMemoryLimitBytes != 384<<20 {
		t.Fatal("started child did not report its launch-time limit", before)
	}
	s.configure("pbm-agent", makeSpec(896<<20))
	if err := s.action(ctx, "pbm-agent", "start"); err != nil {
		t.Fatal(err)
	}
	states, _ = s.snapshot()
	after := states["pbm-agent"]
	if after.PID != before.PID || after.Launch != before.Launch || after.GoMemoryLimitBytes != before.GoMemoryLimitBytes {
		t.Fatal("desired budget change restarted or relabeled the existing child", before, after)
	}
}
