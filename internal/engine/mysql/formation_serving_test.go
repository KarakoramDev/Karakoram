// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Nomad-like completion waits for the serving process, not just the agent.
// Zerops-like completion is independent of the deliberately fenced process.
// Dokploy-like cold completion requires both build success and Running tasks,
// but never the serving health used by a retained-service rollout.
type formationServingBackend struct {
	*fakeBackend
	p                       *Core
	rc                      *engine.Context
	servingHealth           bool
	coldBuildAndTasks       bool
	buildDone, tasksRunning bool
	completedWhileFenced    bool
	pending, completed      int
}

func (b *formationServingBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	op, err := b.fakeBackend.Deploy(ctx, name, d)
	if err == nil && strings.HasSuffix(name, "mx") {
		op.ID, op.ServiceID = "proxy-serving", name
	}
	return op, err
}

func (b *formationServingBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID != "proxy-serving" {
		return b.fakeBackend.OpStatus(ctx, op)
	}
	cfg, err := b.p.MemberConfig(ctx, b.rc, op.ServiceID)
	if err != nil {
		return infra.OpState{}, err
	}
	if b.coldBuildAndTasks && (!b.buildDone || !b.tasksRunning) {
		b.pending++
		return infra.OpState{}, nil
	}
	if b.servingHealth && (len(cfg.DesiredProcesses) == 0 || b.applied[op.ServiceID] < cfg.ConfigVersion) {
		b.pending++
		return infra.OpState{}, nil
	}
	b.completed++
	if len(cfg.DesiredProcesses) == 0 {
		b.completedWhileFenced = true
	}
	return infra.OpState{Done: true}, nil
}

func TestPXCFormationPendingServingDeployment(t *testing.T) {
	for _, name := range []string{"nomad-like", "zerops-like", "dokploy-like"} {
		servingHealth := name == "nomad-like"
		for _, stall := range []bool{false, true} {
			outcome := "/certified"
			if stall {
				outcome = "/stalled"
			}
			t.Run(name+outcome, func(t *testing.T) {
				p, rc, b := setupPXC(t)
				backend := &formationServingBackend{fakeBackend: b, p: p, rc: rc, servingHealth: servingHealth, coldBuildAndTasks: name == "dokploy-like"}
				rc.Backend = backend
				at := time.Now().UTC()
				rc.Clock = trafficRenewalClock{at: at}
				first, last := rc.Cluster.Name+"m1", rc.Cluster.Name+"m2"
				prior := b.action
				probes := 0
				holdFirst := true
				b.action = func(w http.ResponseWriter, r *http.Request, member string) bool {
					if member == last && stall && r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraStatus {
						s, _ := load(rc.Cluster)
						_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{
							ClusterStatus: "Primary", LocalStateComment: "Joining", Connected: true,
							OfflineMode: s.RestoreGTIDFence != "",
						})
						return true
					}
					if member == first && holdFirst || member == last && stall {
						if r.URL.Path == "/v1/mysql/"+myspec.ActionStatus || r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraStatus {
							http.Error(w, "join not yet Synced", http.StatusServiceUnavailable)
							return true
						}
					}
					if r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraHistoryProbe {
						s, _ := load(rc.Cluster)
						for _, m := range s.data() {
							if m.Stage != "joined" {
								t.Errorf("probe before %s joined", m.Name)
							}
						}
						if s.RestoreGTIDFence == "" {
							t.Error("probe without client fence")
						}
						probes++
					}
					return prior(w, r, member)
				}
				boundary, joined, finished := false, false, false
				for range 160 {
					b.apply(t, p, rc)
					// Cold Dokploy deployment first finishes its build, then gets
					// Running tasks; neither event depends on proxy serving.
					backend.buildDone = backend.pending >= 1
					backend.tasksRunning = backend.pending >= 2
					s, _ := load(rc.Cluster)
					proxy, member := s.find(rc.Cluster.Name+"mx"), s.find(first)
					if !boundary && proxy != nil && proxy.Stage != "planned" && proxy.DeployOp != nil && member != nil && member.Stage == "deployed" && s.Formation.State == "formed" && (!backend.coldBuildAndTasks || backend.pending >= 2) {
						// Reproduce the seed-writer + pending proxy cycle. Native progress
						// arrives exactly when the old watchdog would reject the pass.
						holdFirst = false
						c := s.Formation.GTIDCertification
						at = c.LastProgressAt.Add(pxcFormationAdmissionTimeout)
						rc.Clock = trafficRenewalClock{at: at}
						boundary = true
					}
					_, err := p.Sync(t.Context(), rc)
					s, _ = load(rc.Cluster)
					c := s.Formation.GTIDCertification
					if err != nil {
						if !stall || !joined || c == nil || !strings.Contains(c.Blocked, "five minutes") {
							t.Fatalf("unexpected Sync error: %v", err)
						}
						if probes != 0 || !c.Checkpoint.GTIDCertification.CleanupComplete {
							t.Fatal("stalled cohort probed or cleanup incomplete")
						}
						for _, m := range s.data() {
							cfg, e := p.MemberConfig(t.Context(), rc, m.Name)
							if e != nil {
								t.Fatal(e)
							}
							if m.Name == last {
								if len(cfg.DesiredProcesses) != 0 {
									t.Fatal("stalled later joiner released")
								}
							} else if m.Stage != "joined" || len(cfg.DesiredProcesses) == 0 {
								t.Fatalf("Synced member %s stopped during failure cleanup", m.Name)
							}
						}
						b.apply(t, p, rc)
						if b.native[first].MemberState != "ONLINE" {
							t.Fatal("cleanup reload shut down Synced joiner")
						}
						finished = true
						break
					}
					if boundary && s.find(first).Stage == "joined" && !joined {
						joined = true
						if c.Blocked != "" || !c.LastProgressAt.Equal(at) {
							t.Fatal("boundary native progress was not checkpointed before expiry")
						}
						if servingHealth {
							proxy = s.find(rc.Cluster.Name + "mx")
							if proxy.Stage != "deploying" || proxy.DeployOp == nil || backend.completed != 0 || backend.pending == 0 {
								t.Fatal("pending serving deployment was lost or completed early")
							}
						}
						if s.RestoreGTIDFence == "" {
							t.Fatal("fence released before final member joined")
						}
						if stall {
							at = at.Add(pxcFormationAdmissionTimeout)
							rc.Clock = trafficRenewalClock{at: at}
						}
					}
					if !stall && joined && c != nil && c.Checkpoint.GTIDCertification != nil && c.Checkpoint.GTIDCertification.Certified && s.RestoreGTIDFence == "" && backend.completed > 0 {
						b.apply(t, p, rc)
						status, e := p.Status(t.Context(), rc)
						if e != nil {
							t.Fatal(e)
						}
						if status.Phase == domain.PhaseReady {
							finished = true
							break
						}
					}
				}
				if !boundary || !joined || !finished {
					t.Fatalf("formation did not reach outcome: boundary=%v joined=%v finished=%v pending=%d completed=%d", boundary, joined, finished, backend.pending, backend.completed)
				}
				if !stall && probes == 0 {
					t.Fatal("idle formation never executed its own certification probe")
				}
				if !stall && !servingHealth && !backend.completedWhileFenced {
					t.Fatal("supervisor-ready/cold deployment waited for proxy fence release")
				}
			})
		}
	}
}

func TestPXCFormationFailureCleanupCheckpointsSyncedJoiner(t *testing.T) {
	p, rc, b := setupPXC(t)
	first := rc.Cluster.Name + "m1"
	holdObservation := true
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if holdObservation && name == first && (r.URL.Path == "/v1/mysql/"+myspec.ActionStatus || r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraStatus) {
			http.Error(w, "observation temporarily unavailable", http.StatusServiceUnavailable)
			return true
		}
		return prior(w, r, name)
	}
	for range 100 {
		b.apply(t, p, rc)
		s, _ := load(rc.Cluster)
		m := s.find(first)
		if m != nil && m.Stage == "deployed" && b.native[first].MemberState == "ONLINE" && s.Formation.State == "formed" {
			holdObservation = false
			c := s.Formation.GTIDCertification
			if err := formationMappingOwner(c).block(t.Context(), rc, s, &c.Checkpoint, "genuine admission failure"); err == nil {
				t.Fatal("failure did not remain blocked")
			}
			s, _ = load(rc.Cluster)
			if !s.Formation.GTIDCertification.Checkpoint.GTIDCertification.CleanupComplete || s.find(first).Stage != "joined" {
				t.Fatal("failure cleanup lost fresh native Synced receipt")
			}
			b.apply(t, p, rc)
			if b.native[first].MemberState != "ONLINE" {
				t.Fatal("cleanup shut down already Synced member")
			}
			last := s.find(rc.Cluster.Name + "m2")
			cfg, err := p.MemberConfig(t.Context(), rc, last.Name)
			if err != nil || len(cfg.DesiredProcesses) != 0 {
				t.Fatal("failure cleanup admitted the still-unjoined member", err)
			}
			return
		}
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("fixture did not reach uncheckpointed Synced joiner")
}
