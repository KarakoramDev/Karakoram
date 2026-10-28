// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"slices"
	"strings"
	"testing"
)

func runLifecycleOperation(t *testing.T, p *Provider, rc *engine.Context, kind string, payload string) {
	t.Helper()
	op := &domain.Operation{Kind: kind, Payload: json.RawMessage(payload)}
	for range 250 {
		r, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = r.Checkpoint
		if r.Done {
			return
		}
	}
	t.Fatalf("operation stuck: %s state=%s", op.Checkpoint, rc.Cluster.Internal)
}
func TestScaleDownHighestFirstAndResume(t *testing.T) {
	for _, n := range []int{3, 5} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			p, rc, f, a := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = n
			ready(t, p, rc)
			s := state(t, rc)
			s.RollingKind, s.RollingGeneration = "resources", rc.Cluster.Generation
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			a.member("demom0").active = false
			a.member(fmt.Sprintf("demom%d", n-1)).active = true
			rc.Cluster.Spec.Components.Engine.Replicas = n - 2
			rc.Cluster.Generation++
			syncUntil(t, p, rc, func(s *internalState) bool { return s.find(fmt.Sprintf("demom%d", n-1)).RemoveStep == "verify-peer" })
			p = New(p.options)
			a.p = p
			ready(t, p, rc)
			if s := state(t, rc); s.RollingKind != "" || s.RollingGeneration != 0 {
				t.Fatal("completed scale retained superseded rolling intent")
			}
			calls := f.log.snapshot()
			first := slices.Index(calls, fmt.Sprintf("remove-peer:demom%d", n-1))
			second := slices.Index(calls, fmt.Sprintf("remove-peer:demom%d", n-2))
			if first < 0 || second <= first || slices.Index(calls, fmt.Sprintf("step-down:demom%d", n-1)) > first {
				t.Fatalf("unsafe order: %v", calls)
			}
			if len(state(t, rc).Members) != n-2 {
				t.Fatal("members not retired")
			}
		})
	}
}
func TestScaleDownQuorumBlocked(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	a.member("demom1").unreachable = true
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	for range 5 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if state(t, rc).QuorumBlocked == "" {
		t.Fatal("missing quorum block")
	}
	for _, call := range f.log.snapshot() {
		if strings.HasPrefix(call, "remove-peer:") {
			t.Fatal("removed without majority")
		}
	}
}
func TestReplaceDeadMemberJoinsBeforeRemoval(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	a.member("demom1").unreachable = true
	runLifecycleOperation(t, p, rc, domain.OperationReplaceMember, `{"member":"demom1"}`)
	s := state(t, rc)
	if s.find("demom1") != nil || s.find("demom3") == nil || !s.find("demom3").Healthy {
		t.Fatal("replacement incomplete")
	}
	calls := f.log.snapshot()
	if slices.Index(calls, "remove-peer:demom1") < 0 {
		t.Fatal("old peer not removed")
	}
	if slices.Index(calls, "unsealed:demom3") > slices.Index(calls, "remove-peer:demom1") {
		t.Fatal("removed before replacement joined")
	}
}
func TestRestartOperationSerialStandbysThenLeader(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	runLifecycleOperation(t, p, rc, domain.OperationRestart, `{}`)
	calls := f.log.snapshot()
	restarts := []string{}
	for _, call := range calls {
		if strings.HasPrefix(call, "restart:") {
			restarts = append(restarts, call)
		}
	}
	if !slices.Equal(restarts, []string{"restart:demom1", "restart:demom2", "restart:demom0"}) {
		t.Fatalf("roll order: %v", restarts)
	}
	for _, m := range state(t, rc).Members {
		if m.ProcessGeneration != 2 || m.Sealed || !m.Healthy {
			t.Fatalf("incomplete roll: %+v", m)
		}
		if a.member(m.Name).unseals != 6 {
			t.Fatal("member not re-unsealed")
		}
	}
}
func TestFingerprintRollAndHotTLS(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"logLevel":"debug"}`)
	rc.Cluster.Generation++
	ready(t, p, rc)
	for _, m := range state(t, rc).Members {
		if m.ProcessGeneration != 2 {
			t.Fatal("fingerprint did not restart")
		}
	}
	runLifecycleOperation(t, p, rc, domain.OperationRotateTLS, `{}`)
	for _, m := range state(t, rc).Members {
		if m.ProcessGeneration != 2 {
			t.Fatal("TLS restarted a member")
		}
	}
}
func TestUnreachableMemberDoesNotBlockUnseal(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	a.member("demom1").unreachable = true
	a.member("demom2").sealed = true
	syncUntil(t, p, rc, func(s *internalState) bool { return !a.member("demom2").sealed })
	m := state(t, rc).find("demom1")
	if m.Observed || m.Healthy || m.Active || m.Standby {
		t.Fatal("unreachable member retained healthy observation")
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range status.Members {
		if m.Name == "demom1" && (m.Role != "unknown" || m.Health || m.State != "UNKNOWN") {
			t.Fatalf("stale status: %+v", m)
		}
	}
}

func TestResourceAndDiskChangesRollSerially(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	rc.Cluster.Spec.Components.Engine.Resources.MinDisk++
	rc.Cluster.Spec.Components.Engine.Resources.MaxDisk++
	rc.Cluster.Spec.Components.Engine.Resources.MinCPU++
	rc.Cluster.Spec.Components.Engine.Resources.MaxCPU++
	rc.Cluster.Generation++
	ready(t, p, rc)
	for _, m := range state(t, rc).Members {
		if m.ProcessGeneration != 2 || m.Resources.MinCPU != rc.Cluster.Spec.Components.Engine.Resources.MinCPU || m.Disk.MinDisk != rc.Cluster.Spec.Components.Engine.Resources.MinDisk || m.Sealed {
			t.Fatalf("resource roll incomplete: %+v", m)
		}
	}
}
