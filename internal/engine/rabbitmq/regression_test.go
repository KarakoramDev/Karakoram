// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestCreateThreeMembersBootstrapsOnceAndJoins(t *testing.T) {
	p, rc, f, a := setup(t)
	ctx := context.Background()
	pendingObserved := false
	for range 200 {
		before := f.mutations
		if _, err := p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
		if f.mutations-before > 1 {
			t.Fatal("multiple infrastructure mutations in one Sync")
		}
		s := state(t, rc)
		if s.Pending != nil && !pendingObserved {
			pendingObserved = true
			f.done = false
			before = f.mutations
			result, err := p.Sync(ctx, rc)
			if err != nil {
				t.Fatal(err)
			}
			if f.mutations != before || result.RequeueAfter <= 0 {
				t.Fatal("advanced past pending platform operation")
			}
			f.done = true
		}
		if s.Phase == "ready" {
			break
		}
	}
	s := state(t, rc)
	if s.Phase != "ready" || !s.Bootstrapped || len(s.Members) != 3 || !pendingObserved {
		t.Fatalf("not ready: %+v", s)
	}
	calls := a.log.snapshot()
	bootstrap := 0
	joins := 0
	for _, call := range calls {
		if strings.HasSuffix(call, ":bootstrap") {
			bootstrap++
		}
		if strings.HasSuffix(call, ":join") {
			joins++
		}
	}
	if bootstrap != 1 || joins != 2 || len(f.imports) != 3 || len(f.deployments) != 3 {
		t.Fatalf("bootstrap=%d joins=%d imports=%d deploys=%d calls=%v", bootstrap, joins, len(f.imports), len(f.deployments), calls)
	}
	for _, specs := range f.imports {
		if len(specs) != 2 {
			t.Fatalf("runtime + volume import: %+v", specs)
		}
		for _, spec := range specs {
			if spec.ZeropsYAML != "" {
				t.Fatal("import must not build with an empty artifact")
			}
		}
	}
	for range 3 {
		if _, err := p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
	}
	if countCall(a.log.snapshot(), "agent:demom0:bootstrap") != 1 {
		t.Fatal("bootstrap repeated")
	}
	status, err := p.Status(ctx, rc)
	if err != nil || status.Phase != domain.PhaseReady || len(status.Members) != 3 {
		t.Fatalf("status: %+v %v", status, err)
	}
}

func TestScaleUpDownOrderingAndMonotonicNames(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	ready(t, p, rc)
	if state(t, rc).NextIndex != 5 {
		t.Fatal("unexpected next member index")
	}
	a.log.clear()
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	ready(t, p, rc)
	calls := a.log.snapshot()
	for _, name := range []string{"demom4", "demom3"} {
		memberCalls := calls
		if name == "demom3" {
			memberCalls = calls[slices.Index(calls, "infra:demom4v:delete")+1:]
		}
		shrink := "agent:demom0:shrink:rabbit@" + name
		drain := "agent:" + name + ":drain"
		stopApp := "agent:" + name + ":stop-app"
		stop := "agent:" + name + ":stop"
		del := "infra:" + name + ":delete"
		volume := "infra:" + name + "v:delete"
		assertBefore(t, memberCalls, shrink, drain)
		assertBefore(t, memberCalls, drain, stopApp)
		assertBefore(t, memberCalls, stop, del)
		assertBefore(t, memberCalls, del, volume)
		for _, survivor := range []string{"demom0", "demom1", "demom2"} {
			grow := "agent:demom0:grow:rabbit@" + survivor
			assertBefore(t, memberCalls, grow, shrink)
		}
		forget := "agent:demom0:forget:rabbit@" + name
		assertBefore(t, memberCalls, stopApp, forget)
		assertBefore(t, memberCalls, forget, stop)
		if f.services[name] != nil || f.services[name+"v"] != nil {
			t.Fatalf("removed resources survive: %s", name)
		}
	}
	assertBefore(t, calls, "infra:demom4:delete", "infra:demom3:delete")
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	ready(t, p, rc)
	s := state(t, rc)
	if s.find("demom5") == nil || s.find("demom6") == nil || s.find("demom3") != nil || s.find("demom4") != nil || s.NextIndex != 7 {
		t.Fatalf("reused old indices: %+v", s)
	}
}

func TestScaleDownRefusesWhenRemainingMemberDown(t *testing.T) {
	p, rc, f, a := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	ready(t, p, rc)
	a.log.clear()
	a.broker("demom1").down = true
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	before := f.mutations
	blocked := false
	for range 12 {
		_, err := p.Sync(context.Background(), rc)
		if err != nil {
			blocked = true
			break
		}
	}
	if !blocked {
		t.Fatal("scale-down did not report unavailable survivor")
	}
	if f.mutations != before {
		t.Fatal("platform mutated while a remaining member was down")
	}
	for _, call := range a.log.snapshot() {
		for _, action := range []string{":grow", ":shrink", ":drain", ":stop", ":forget"} {
			if strings.HasSuffix(call, action) {
				t.Fatalf("unsafe action while survivor down: %s", call)
			}
		}
	}
	a.broker("demom1").down = false
	ready(t, p, rc)
}

func TestRollingUpgradeSafetyOrder(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("members=%d", replicas), func(t *testing.T) {
			p, rc, _, a := setup(t)
			rc.Cluster.Spec.Version = "4.3.5"
			rc.Cluster.Spec.Components.Engine.Replicas = replicas
			ready(t, p, rc)
			a.log.clear()
			rc.Cluster.Spec.Version = "4.3.6"
			syncUntil(t, p, rc, func(s *internalState) bool {
				for _, m := range s.Members {
					if m.Version != "4.3.6" || m.ChangeStep != "" {
						return false
					}
				}
				return !s.FeatureFlags && !s.Rebalance && s.Pending == nil
			})
			calls := a.log.snapshot()
			for i := range replicas {
				name := fmt.Sprintf("demom%d", i)
				drain := "agent:" + name + ":drain"
				stop := "agent:" + name + ":stop"
				deploy := "infra:" + name + ":deploy"
				assertBefore(t, calls, drain, stop)
				assertBefore(t, calls, stop, deploy)
				if replicas > 1 {
					assertBefore(t, calls, "agent:"+name+":quorum-critical", "agent:"+name+":await-quorum")
					assertBefore(t, calls, "agent:"+name+":quorum-critical", drain)
				}
				if i > 0 {
					assertBefore(t, calls, fmt.Sprintf("infra:demom%d:deploy", i-1), drain)
				}
				if countCall(calls, deploy) != 1 {
					t.Fatalf("multiple deploys for %s: %v", name, calls)
				}
			}
			flags, rebalance := "", ""
			for _, call := range calls {
				if strings.HasSuffix(call, ":enable-feature-flags") {
					flags = call
				}
				if strings.HasSuffix(call, ":rebalance") {
					rebalance = call
				}
			}
			if flags == "" || rebalance == "" {
				t.Fatalf("post-upgrade finalization missing: %v", calls)
			}
			for i := range replicas {
				assertBefore(t, calls, fmt.Sprintf("infra:demom%d:deploy", i), flags)
			}
			assertBefore(t, calls, flags, rebalance)
		})
	}
}

func TestQuorumCriticalMemberBlocksRollingUpgrade(t *testing.T) {
	p, rc, f, a := setup(t)
	rc.Cluster.Spec.Version = "4.3.5"
	ready(t, p, rc)
	a.log.clear()
	a.broker("demom0").critical = true
	rc.Cluster.Spec.Version = "4.3.6"
	before := f.mutations
	blocked := false
	for range 12 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			blocked = true
			break
		}
	}
	if !blocked {
		t.Fatal("quorum-critical member was not refused")
	}
	if f.mutations != before || slices.Contains(a.log.snapshot(), "agent:demom0:drain") || slices.Contains(a.log.snapshot(), "agent:demom0:stop") {
		t.Fatalf("critical member disrupted: %v", a.log.snapshot())
	}
	a.broker("demom0").critical = false
	ready(t, p, rc)
}

func TestOwnershipCollisionsRefused(t *testing.T) {
	for _, kind := range []string{"runtime", "volume"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, f, _ := setup(t)
			s := state(t, rc)
			s.UID = "collision"
			m := s.add(rc.Cluster.Name)
			name := m.Name
			if kind == "volume" {
				name = m.Volume
			}
			f.services[name] = &infra.Service{ID: "foreign", Hostname: name}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Sync(context.Background(), rc); !errors.Is(err, engine.ErrConflict) {
				t.Fatalf("adopted foreign %s: %v", kind, err)
			}
			if f.mutations != 0 {
				t.Fatal("mutated foreign service")
			}
			f.log.clear()
			if _, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepBackups: true}); !errors.Is(err, engine.ErrConflict) {
				t.Fatalf("cleanup touched foreign %s: %v", kind, err)
			}
			if f.mutations != 0 || len(f.log.snapshot()) != 0 {
				t.Fatal("cleanup mutated unowned resources")
			}
		})
	}
}

func TestCleanupRetentionAndDeletion(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprintf("keepVolumes=%v", keep), func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			a.log.clear()
			members := slices.Clone(state(t, rc).Members)
			done := false
			for range 100 {
				var err error
				done, err = p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepVolumes: keep, KeepBackups: true})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done {
				t.Fatal("cleanup did not finish")
			}
			calls := a.log.snapshot()
			for _, m := range members {
				if f.services[m.Name] != nil {
					t.Fatalf("runtime survived: %s", m.Name)
				}
				assertBefore(t, calls, "agent:"+m.Name+":drain", "agent:"+m.Name+":stop")
				assertBefore(t, calls, "agent:"+m.Name+":stop", "infra:"+m.Name+":delete")
				if (f.services[m.Volume] != nil) != keep {
					t.Fatalf("volume retention mismatch: %s", m.Volume)
				}
			}
			retained, err := rc.Store.ListRetainedVolumes(context.Background(), rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if keep && len(retained) != len(members) || !keep && len(retained) != 0 {
				t.Fatal(retained)
			}
			if !keep {
				for _, key := range []string{"erlang-cookie", "users/admin", "users/karakoram", "users/monitor", "tls/ca.key"} {
					if _, err := rc.Secrets.GetClusterSecret(context.Background(), rc.Cluster.Ref(), key); err == nil {
						t.Errorf("secret survived: %s", key)
					}
				}
			}
		})
	}
}

func TestPlatformRetryBackoffAndUnknownStatePreserved(t *testing.T) {
	p, rc, f, _ := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	rc.Cluster.Internal = json.RawMessage(`{"peerWorkflow":{"step":"preserved"}}`)
	syncUntil(t, p, rc, func(s *internalState) bool { return s.Pending != nil && s.Pending.Action == "deploy" })
	f.failed = true
	for attempt := 1; attempt <= 7; attempt++ {
		if _, err := p.Sync(context.Background(), rc); err == nil {
			t.Fatal("platform failure hidden")
		}
		s := state(t, rc)
		if s.Pending != nil || s.Retry == nil {
			t.Fatalf("retry not checkpointed: %+v", s)
		}
		var retry *platformRetry
		for _, r := range s.PlatformRetries {
			retry = r
		}
		if retry == nil || retry.Failures != attempt {
			t.Fatalf("retry count: %+v", retry)
		}
		delay := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 10 * time.Minute}[min(attempt-1, 5)]
		clock := rc.Clock.(*testClock)
		if retry.RetryAfter.Sub(clock.at) != delay {
			t.Fatalf("backoff attempt %d: %s", attempt, retry.RetryAfter.Sub(clock.at))
		}
		before := f.mutations
		clock.at = retry.RetryAfter.Add(-time.Nanosecond)
		if _, err := p.Sync(context.Background(), rc); err == nil {
			t.Fatal("backoff wait hidden")
		}
		if f.mutations != before {
			t.Fatal("retried before deadline")
		}
		clock.at = retry.RetryAfter
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
	}
	f.failed = false
	ready(t, p, rc)
	s := state(t, rc)
	if s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 {
		t.Fatalf("successful retry did not reset: %+v", s)
	}
	if !strings.Contains(string(rc.Cluster.Internal), `"peerWorkflow"`) {
		t.Fatal("lost sibling workflow state")
	}
}

func TestRollingUpgradeWaitsForConfigAcknowledgmentBeforeNextMember(t *testing.T) {
	p, rc, f, a := setup(t)
	rc.Cluster.Spec.Version = "4.3.5"
	ready(t, p, rc)
	a.log.clear()
	rc.Cluster.Spec.Version = "4.3.6"
	syncUntil(t, p, rc, func(s *internalState) bool { return s.Members[0].ChangeStep == "wait" })
	first := state(t, rc).Members[0]
	a.broker(first.Name).config = first.ConfigVersion - 1
	before := f.mutations
	for range 4 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if f.mutations != before || state(t, rc).Members[0].ChangeStep != "wait" || slices.Contains(a.log.snapshot(), "agent:demom1:drain") {
		t.Fatal("advanced to next member without configuration acknowledgment")
	}
	a.broker(first.Name).config = first.ConfigVersion
	ready(t, p, rc)
}

func TestAlarmsAndPartitionsPreventReady(t *testing.T) {
	for _, kind := range []string{"alarm", "partition"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, _, a := setup(t)
			b := a.broker("demom0")
			if kind == "alarm" {
				b.alarms = json.RawMessage(`[{"resource":"memory"}]`)
			} else {
				b.partitions = json.RawMessage(`{"rabbit@demom0":["rabbit@demom1"]}`)
			}
			for range 30 {
				if _, err := p.Sync(context.Background(), rc); err != nil {
					t.Fatal(err)
				}
			}
			if state(t, rc).Phase == "ready" {
				t.Fatal("cluster ready despite " + kind)
			}
			b.alarms = nil
			b.partitions = nil
			ready(t, p, rc)
		})
	}
}

func TestMemberConfigDoesNotEscapeRollingGate(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	ctx := context.Background()
	before, err := p.MemberConfig(ctx, rc, "demom0")
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.TLS.Mode = "preferTLS"
	rc.Cluster.Spec.EngineConfig = "consumer_timeout = 600000"
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"plugins":["rabbitmq_stream"]}`)
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 2
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 2
	s := state(t, rc)
	s.ConfigVersion++
	if err = s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	after, err := p.MemberConfig(ctx, rc, "demom0")
	if err != nil {
		t.Fatal(err)
	}
	if after.ConfigVersion != before.ConfigVersion || after.TLS.Mode != before.TLS.Mode || string(after.EngineSpec) != string(before.EngineSpec) {
		t.Fatalf("desired config bypassed per-member rolling gate: before=%+v after=%+v", before, after)
	}
}

func TestRetainedCleanupResumesExistingVolumeIdentities(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	before := state(t, rc)
	volumes := map[string]string{}
	for _, m := range before.Members {
		volumes[m.Name] = m.VolumeID
	}
	done := false
	for range 100 {
		var err error
		done, err = p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done || rc.Cluster.Status.Phase != domain.PhaseSuspended {
		t.Fatal("retained cleanup did not suspend the cluster")
	}
	importCount := len(f.imports)
	a.log.clear()
	ready(t, p, rc)
	after := state(t, rc)
	if after.NextIndex != before.NextIndex || len(after.Members) != len(before.Members) {
		t.Fatalf("resume allocated fresh identities: %+v", after)
	}
	for _, m := range after.Members {
		if m.VolumeID != volumes[m.Name] || f.services[m.Volume] == nil {
			t.Fatalf("resume did not reuse retained volume: %+v", m)
		}
		calls := a.log.snapshot()
		assertBefore(t, calls, "infra:"+m.Name+":deploy", "agent:"+m.Name+":start")
		assertBefore(t, calls, "agent:"+m.Name+":start", "agent:"+m.Name+":revive")
		for _, peer := range after.Members {
			assertBefore(t, calls, "agent:"+peer.Name+":start", "agent:"+m.Name+":revive")
		}
	}
	if len(f.imports)-importCount != len(before.Members) {
		t.Fatal("resume did not provision exactly the missing runtimes")
	}
	for _, imported := range f.imports[importCount:] {
		if len(imported) != 1 || imported[0].Type != "ubuntu@24.04" {
			t.Fatalf("resume imported a new volume: %+v", imported)
		}
	}
	retained, err := rc.Store.ListRetainedVolumes(context.Background(), rc.Cluster.Ref())
	if err != nil || len(retained) != 0 {
		t.Fatalf("adopted retained volume records survive resume: %+v %v", retained, err)
	}
}
