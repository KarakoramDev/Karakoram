// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func cleanupCluster(t *testing.T, p *Provider, rc *engine.Context, opts domain.DeleteOptions) {
	t.Helper()
	for range 100 {
		done, err := p.Cleanup(context.Background(), rc, opts)
		if err != nil {
			t.Fatalf("cleanup: %v; state=%s", err, rc.Cluster.Internal)
		}
		if done {
			return
		}
	}
	t.Fatalf("cleanup did not converge: %s", rc.Cluster.Internal)
}

// Keep the runtime observable until the platform has completed its shutdown.
type cleanupFenceInfra struct{ *fakeInfra }

func (f *cleanupFenceInfra) DeleteService(_ context.Context, name string) (infra.Op, error) {
	return f.op("delete", name)
}

func (f *cleanupFenceInfra) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	st, err := f.fakeInfra.OpStatus(ctx, op)
	if err == nil && st.Done && !st.Failed && op.Kind == "delete" {
		delete(f.services, op.ServiceID)
	}
	return st, err
}

func TestCleanupDoesNotDrainAfterPeerRemoval(t *testing.T) {
	for _, keep := range []bool{true, false} {
		t.Run(fmt.Sprintf("keepVolumes=%v", keep), func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			members := slices.Clone(state(t, rc).Members)
			a.log.clear()
			// Model the maintenance failure from a real cluster after its first
			// peer disappears from discovery. A serial drain/delete stalls here.
			a.requestHook = func(action string) {
				if action != "drain" {
					return
				}
				for _, peer := range members {
					if f.services[peer.Name] == nil {
						for _, survivor := range members {
							a.failures[survivor.Name+":drain"] = 503
						}
					}
				}
			}
			cleanupCluster(t, p, rc, domain.DeleteOptions{KeepVolumes: keep, KeepBackups: true})
			calls := a.log.snapshot()
			for _, call := range calls {
				for _, action := range []string{":drain", ":forget", ":shrink", ":stop-app"} {
					if strings.HasPrefix(call, "agent:") && strings.HasSuffix(call, action) {
						t.Fatalf("whole-cluster cleanup changed replicated membership/maintenance: %v", calls)
					}
				}
			}
			for _, m := range members {
				if f.services[m.Name] != nil || (f.services[m.Volume] != nil) != keep {
					t.Fatalf("wrong runtime/volume inventory for %s", m.Name)
				}
				if keep {
					for _, peer := range members {
						assertBefore(t, calls, "agent:"+peer.Name+":stop", "infra:"+m.Name+":delete")
					}
				}
			}
			// Re-enter after all runtime identities are gone: never dial them.
			for _, m := range members {
				a.broker(m.Name).down = true
			}
			cleanupCluster(t, p, rc, domain.DeleteOptions{KeepVolumes: keep, KeepBackups: true})
		})
	}
}

func TestFullDeleteUnreachableAndAbsentMembers(t *testing.T) {
	for _, absent := range []string{"none", "runtime", "runtime-and-volume", "all"} {
		t.Run(absent, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			members := slices.Clone(state(t, rc).Members)
			for _, m := range members {
				a.broker(m.Name).down = true
			}
			switch absent {
			case "runtime":
				delete(f.services, "demom0")
			case "runtime-and-volume":
				delete(f.services, "demom0")
				delete(f.services, "demom0v")
			case "all":
				clear(f.services)
			}
			agents := &lifecycleAgents{fakeAgents: a}
			rc.Agents = agents
			cleanupCluster(t, p, rc, domain.DeleteOptions{KeepBackups: true})
			cleanupCluster(t, p, rc, domain.DeleteOptions{KeepBackups: true})
			if len(f.services) != 0 || len(agents.dials) != 0 {
				t.Fatalf("full delete depended on absent/unreachable agents: services=%v dials=%v", f.services, agents.dials)
			}
		})
	}
}

func TestSuspendShutdownFailureRecordedOnceAndFenced(t *testing.T) {
	for _, action := range []string{"/v1/maintenance", "stop"} {
		t.Run(action, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			a.failures["demom0:"+action] = 503
			events := &definitionsTestEvents{}
			rc.Events = events
			a.log.clear()
			cleanupCluster(t, p, rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
			if f.services["demom0"] != nil || f.services["demom0v"] == nil {
				t.Fatal("failed local shutdown bypassed platform fencing or lost volume")
			}
			if len(events.events) != 1 || events.events[0].Reason != "SuspendBrokerShutdownSkipped" || !strings.Contains(events.events[0].Message, "platform runtime deletion must complete") {
				t.Fatalf("shutdown skip lacked actionable diagnosis: %+v", events.events)
			}
			if countCall(a.log.snapshot(), "agent:demom0:"+action) != 1 {
				t.Fatalf("failed shutdown was retried indefinitely: %v", a.log.snapshot())
			}
		})
	}
}

func TestSuspendWaitsForTerminalRuntimeBeforeRetainingVolumes(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	a.broker("demom0").down = true
	f.done = false
	rc.Backend = &cleanupFenceInfra{fakeInfra: f}
	for range 20 {
		done, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
		if err != nil || done {
			t.Fatalf("cleanup before terminal observation: done=%v err=%v", done, err)
		}
		if state(t, rc).Pending != nil {
			break
		}
	}
	if pending := state(t, rc).Pending; pending == nil || pending.Action != "cleanup-runtime" {
		t.Fatalf("runtime deletion was not submitted: %+v", pending)
	}
	for range 3 {
		done, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
		if err != nil || done {
			t.Fatalf("nonterminal delete was not awaited: done=%v err=%v", done, err)
		}
	}
	retained, err := rc.Store.ListRetainedVolumes(context.Background(), rc.Cluster.Ref())
	if err != nil || len(retained) != 0 {
		t.Fatalf("volume released before runtime terminal observation: %+v %v", retained, err)
	}
	f.done = true
	cleanupCluster(t, p, rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
}
