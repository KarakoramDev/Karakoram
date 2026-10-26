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

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type lifecycleInfra struct {
	*fakeInfra
	containers map[string][]infra.Container
}

func (f *lifecycleInfra) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if containers, ok := f.containers[name]; ok {
		return containers, nil
	}
	return f.fakeInfra.Containers(ctx, name)
}

type lifecycleAgents struct {
	*fakeAgents
	dials []string
}

func (a *lifecycleAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	a.dials = append(a.dials, name)
	return a.fakeAgents.Dial(ctx, ref, name)
}

func TestStatusUsesObservedBrokerVersion(t *testing.T) {
	for _, cached := range []string{"", "4.3.5"} {
		t.Run("agent-version-"+cached, func(t *testing.T) {
			p, rc, _, a := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = 1
			ready(t, p, rc)
			// A newly booted definitions clone can cache an empty agent version
			// while its local broker already reports a healthy running version.
			a.broker("demom0").agentVersion = &cached
			status, err := p.Status(context.Background(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase != domain.PhaseReady || len(status.Members) != 1 {
				t.Fatalf("single broker status: %+v", status)
			}
			m := status.Members[0]
			if !m.Health || !m.Agent.Healthy || m.Role != "broker" || m.State != "RUNNING" || m.Version != "4.3.6" {
				t.Fatalf("broker observation replaced by cached agent version: %+v", m)
			}
		})
	}
}

func TestMissingBrokerVersionBlocksReadiness(t *testing.T) {
	p, rc, _, a := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	ready(t, p, rc)
	broker := a.broker("demom0")
	broker.version = ""
	cached := "4.3.6"
	broker.agentVersion = &cached
	status, err := p.Status(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != domain.PhaseDegraded || status.Members[0].Health || status.Members[0].Version != "" {
		t.Fatalf("missing broker version published healthy readiness: %+v", status)
	}
	healthy, err := p.healthy(context.Background(), rc, state(t, rc).Members[0])
	if err != nil || healthy {
		t.Fatalf("missing broker version passed reconcile readiness: healthy=%v err=%v", healthy, err)
	}
}

func TestStatusHealthyMajorityDuringBrokerShutdown(t *testing.T) {
	for _, stopped := range []string{"process", "application", "unreachable", "local-membership-negative"} {
		t.Run(stopped, func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			broker := a.broker("demom2")
			switch stopped {
			case "process":
				broker.running = false
			case "application":
				broker.appStopped = true
			case "unreachable":
				broker.down = true
			case "local-membership-negative":
				// A fresh explicit local negative can race an otherwise
				// successful in-service probe. It must not become a self-peer
				// connectivity diagnosis or affect healthy survivors.
				broker.runningNodes = []string{"rabbit@demom0", "rabbit@demom1"}
			}
			status, err := p.Status(context.Background(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase != domain.PhaseDegraded || len(status.Members) != 3 {
				t.Fatalf("shutdown status: %+v", status)
			}
			for _, member := range status.Members {
				if member.Health != (member.Name != "demom2") {
					t.Fatalf("peer shutdown changed local health: %+v", member)
				}
				if strings.Contains(member.Agent.Message, "expected peer rabbit@"+member.Name) {
					t.Fatalf("local application absence attributed to a self-peer link: %+v", member)
				}
				if member.Name == "demom2" && (stopped == "application" || stopped == "local-membership-negative") &&
					(!strings.Contains(member.Agent.Message, "own RabbitMQ application") || !strings.Contains(member.Agent.Message, "check_running/status")) {
					t.Fatalf("missing actionable local application diagnosis: %+v", member)
				}
			}
			assertLifecycleCondition(t, status, "Healthy", "False")
			partition := "False"
			if stopped == "unreachable" {
				partition = "Unknown"
			}
			assertLifecycleCondition(t, status, "Partition", partition)
			assertLifecycleCondition(t, status, "Alarms", "False")
		})
	}
}

func TestStatusLocalProblemsStillFailHealth(t *testing.T) {
	for _, problem := range []string{"alarm", "partition", "maintenance", "platform-stopped", "platform-absent"} {
		t.Run(problem, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			a.broker("demom2").running = false
			broker := a.broker("demom0")
			switch problem {
			case "alarm":
				broker.alarms = json.RawMessage(`[{"resource":"memory"}]`)
			case "partition":
				broker.partitions = json.RawMessage(`{"rabbit@demom0":["rabbit@demom1"]}`)
			case "maintenance":
				broker.maintenance = true
			case "platform-stopped":
				rc.Backend = &lifecycleInfra{fakeInfra: f, containers: map[string][]infra.Container{"demom0": {{Status: "STOPPED"}}}}
			case "platform-absent":
				rc.Backend = &lifecycleInfra{fakeInfra: f, containers: map[string][]infra.Container{"demom0": nil}}
			}
			status, err := p.Status(context.Background(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase != domain.PhaseDegraded {
				t.Fatalf("local problem did not degrade status: %+v", status)
			}
			for _, member := range status.Members {
				if member.Health != (member.Name == "demom1") {
					t.Fatalf("local problem health: %+v", member)
				}
			}
			assertLifecycleCondition(t, status, "Healthy", "False")
			partition, alarm := "False", "False"
			if problem == "partition" {
				partition = "True"
			}
			if problem == "alarm" {
				alarm = "True"
			}
			assertLifecycleCondition(t, status, "Partition", partition)
			assertLifecycleCondition(t, status, "Alarms", alarm)
		})
	}
}

func assertLifecycleCondition(t *testing.T, status domain.ClusterStatus, kind, want string) {
	t.Helper()
	for _, condition := range status.Conditions {
		if condition.Type == kind {
			if condition.Status != want {
				t.Fatalf("%s condition = %s, want %s: %+v", kind, condition.Status, want, condition)
			}
			return
		}
	}
	t.Fatalf("missing %s condition: %+v", kind, status.Conditions)
}

func TestCleanupUnreachableNonRunningRuntime(t *testing.T) {
	for _, runtime := range []struct{ status, stage string }{
		{infra.StatusStopped, "ready"},
		{infra.StatusFailed, "deployed"},
		{infra.StatusReadyToDeploy, "joined"},
	} {
		for _, keep := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/keepVolumes=%v", runtime.status, keep), func(t *testing.T) {
				p, rc, f, a := setup(t)
				ready(t, p, rc)
				s := state(t, rc)
				members := slices.Clone(s.Members)
				s.find("demom0").Stage = runtime.stage
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
				f.services["demom0"].Status = runtime.status
				a.broker("demom0").down = true
				agents := &lifecycleAgents{fakeAgents: a}
				rc.Agents = agents
				a.log.clear()
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
					t.Fatalf("cleanup stalled on %s runtime: %s", runtime.status, rc.Cluster.Internal)
				}
				if slices.Contains(agents.dials, "demom0") {
					t.Fatalf("cleanup dialed nonrunning runtime: %v", agents.dials)
				}
				calls := a.log.snapshot()
				if !slices.Contains(calls, "infra:demom0:delete") {
					t.Fatalf("unreachable runtime was not deleted: %v", calls)
				}
				for _, m := range members {
					if f.services[m.Name] != nil || (f.services[m.Volume] != nil) != keep {
						t.Fatalf("runtime/volume cleanup mismatch: %+v", m)
					}
					if keep && m.Name != "demom0" {
						if slices.Contains(calls, "agent:"+m.Name+":drain") {
							t.Fatalf("whole-cluster suspension drained %s", m.Name)
						}
						assertBefore(t, calls, "agent:"+m.Name+":/v1/maintenance", "agent:"+m.Name+":stop")
						assertBefore(t, calls, "agent:"+m.Name+":stop", "infra:"+m.Name+":delete")
					}
				}
				retained, err := rc.Store.ListRetainedVolumes(context.Background(), rc.Cluster.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if !keep {
					if len(retained) != 0 {
						t.Fatalf("deleted volumes retained: %+v", retained)
					}
					return
				}
				if len(retained) != len(members) || rc.Cluster.Status.Phase != domain.PhaseSuspended {
					t.Fatalf("retained cleanup: records=%+v phase=%s", retained, rc.Cluster.Status.Phase)
				}
				for _, volume := range retained {
					m := s.find(volume.Member)
					if m == nil || volume.Hostname != m.Volume || volume.ServiceID != m.VolumeID || volume.Index != m.Index || volume.Cluster != rc.Cluster.Name || volume.Namespace != rc.Cluster.Ref().Namespace {
						t.Fatalf("retained identity changed: %+v", volume)
					}
				}
				for _, key := range []string{"erlang-cookie", "users/karakoram", "tls/ca.key"} {
					if _, err := rc.Secrets.GetClusterSecret(context.Background(), rc.Cluster.Ref(), key); err != nil {
						t.Fatalf("retained volume secret %s lost: %v", key, err)
					}
				}
			})
		}
	}
}

func TestCleanupUnreachableRuntimeUsesPlatformFence(t *testing.T) {
	for _, status := range []string{"", infra.StatusActive, "RUNNING", "STARTING", "STOPPING", "UNKNOWN"} {
		t.Run(status, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			f.services["demom0"].Status = status
			a.broker("demom0").down = true
			events := &definitionsTestEvents{}
			rc.Events = events
			a.log.clear()
			cleanupCluster(t, p, rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
			if f.services["demom0"] != nil || f.services["demom0v"] == nil {
				t.Fatal("platform shutdown did not preserve the unreachable broker volume")
			}
			if state(t, rc).find("demom0").CleanupStopReason == "" || len(events.events) != 1 || events.events[0].Reason != "SuspendBrokerShutdownSkipped" {
				t.Fatalf("unreachable broker shutdown was not diagnosed: %+v", events.events)
			}
			calls := a.log.snapshot()
			for _, action := range []string{"drain", "stop", "forget", "shrink"} {
				if slices.Contains(calls, "agent:demom0:"+action) {
					t.Fatalf("unreachable broker received %s: %v", action, calls)
				}
			}
			assertBefore(t, calls, "agent:demom1:stop", "infra:demom0:delete")
			assertBefore(t, calls, "agent:demom2:stop", "infra:demom0:delete")
		})
	}
}

func TestCleanupNonRunningForeignRuntimeIsRefused(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	f.services["demom0"].ID = "foreign"
	f.services["demom0"].Ownership = nil
	f.services["demom0"].Status = infra.StatusStopped
	a.broker("demom0").down = true
	a.log.clear()
	before := f.mutations
	done, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepBackups: true})
	if !errors.Is(err, engine.ErrConflict) || done || f.mutations != before || len(a.log.snapshot()) != 0 {
		t.Fatalf("foreign stopped runtime touched: done=%v err=%v calls=%v", done, err, a.log.snapshot())
	}
}
