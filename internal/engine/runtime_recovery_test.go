// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

type recoveryClock struct{ now time.Time }

func (c *recoveryClock) Now() time.Time { return c.now }
func recoveryFixture(t *testing.T, stateful bool) (*Context, *fake.Backend, RuntimeMember) {
	t.Helper()
	b := fake.New(fake.Options{})
	m := RuntimeMember{MemberService: MemberService{Cluster: "demo", Member: "member", Token: "token", Containers: 1}, ServiceID: new(string), Recovery: &RuntimeRecovery{}}
	specs := []infra.ServiceSpec{}
	if stateful {
		m.Volume = "volume"
		specs = append(specs, VolumeServiceSpec(m.Cluster, m.Member, m.Volume, m.Token, domain.Resources{}))
	}
	specs = append(specs, RuntimeServiceSpec(b.Layout(), m.MemberService))
	op, err := b.ImportServices(t.Context(), specs)
	if err != nil {
		t.Fatal(err)
	}
	*m.ServiceID = op.Services[m.Member]
	m.VolumeID = op.Services[m.Volume]
	rc := &Context{Backend: b, Cluster: &domain.Cluster{Name: "demo"}, Clock: &recoveryClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}}
	return rc, b, m
}
func countRecoveryCalls(calls []string, method string) int {
	n := 0
	for _, call := range calls {
		if call == method {
			n++
		}
	}
	return n
}

func TestRuntimeRecoveryStopDeleteAndVolumeLoss(t *testing.T) {
	for _, family := range []string{"mongodb", "rabbitmq", "elasticsearch", "postgresql", "supabase"} {
		t.Run(family, func(t *testing.T) {
			stateful := family != "supabase"
			t.Run("stopped", func(t *testing.T) {
				rc, b, m := recoveryFixture(t, stateful)
				if _, err := b.Stop(t.Context(), m.Member); err != nil {
					t.Fatal(err)
				}
				before := countRecoveryCalls(b.Calls(), "Start")
				out, err := ReconcileRuntime(t.Context(), rc, m)
				if err != nil || !out.Busy || m.Recovery.Op == nil {
					t.Fatalf("no durable start: %+v %v", out, err)
				}
				if _, err = ReconcileRuntime(t.Context(), rc, m); err != nil {
					t.Fatal(err)
				}
				if countRecoveryCalls(b.Calls(), "Start") != before+1 {
					t.Fatal("start duplicated while observing operation")
				}
				service, _ := b.GetService(t.Context(), m.Member)
				if service.Status != infra.StatusActive {
					t.Fatal("runtime not started")
				}
			})
			t.Run("deleted_runtime", func(t *testing.T) {
				rc, b, m := recoveryFixture(t, stateful)
				oldID := *m.ServiceID
				if _, err := b.DeleteService(t.Context(), m.Member); err != nil {
					t.Fatal(err)
				}
				out, err := ReconcileRuntime(t.Context(), rc, m)
				if err != nil || !out.Busy {
					t.Fatalf("missing runtime not repaired: %+v %v", out, err)
				}
				if m.Recovery.ImportPlan == nil || m.Recovery.Op != nil {
					t.Fatal("runtime import batch was not checkpointed before submission")
				}
				if _, err = ReconcileRuntime(t.Context(), rc, m); err != nil {
					t.Fatal(err)
				}
				out, err = ReconcileRuntime(t.Context(), rc, m)
				if err != nil || !out.Imported || oldID == *m.ServiceID {
					t.Fatalf("import not adopted: %+v %v", out, err)
				}
				if stateful {
					v, _ := b.GetService(t.Context(), m.Volume)
					if v == nil || v.ID != m.VolumeID {
						t.Fatal("retained volume replaced")
					}
				}
				services, _ := b.ListServices(t.Context(), "")
				want := 1
				if stateful {
					want = 2
				}
				if len(services) != want {
					t.Fatal("unexpected replacement services")
				}
			})
			if stateful {
				t.Run("deleted_volume", func(t *testing.T) {
					rc, b, m := recoveryFixture(t, true)
					if _, err := b.DeleteService(t.Context(), m.Member); err != nil {
						t.Fatal(err)
					}
					if _, err := b.DeleteService(t.Context(), m.Volume); err != nil {
						t.Fatal(err)
					}
					before := countRecoveryCalls(b.Calls(), "ImportServices")
					out, err := ReconcileRuntime(t.Context(), rc, m)
					if err != nil || !out.VolumeLost || !m.Recovery.VolumeLost {
						t.Fatalf("storage loss not explicit: %+v %v", out, err)
					}
					if countRecoveryCalls(b.Calls(), "ImportServices") != before {
						t.Fatal("empty volume recreated")
					}
				})
			}
		})
	}
}

func TestRuntimeRecoveryFailsClosedAndBoundsRetries(t *testing.T) {
	t.Run("observation_error", func(t *testing.T) {
		rc, b, m := recoveryFixture(t, true)
		b.FailNext("GetService", infra.ErrUnavailable)
		if _, err := ReconcileRuntime(t.Context(), rc, m); !errors.Is(err, infra.ErrUnavailable) {
			t.Fatal(err)
		}
		if countRecoveryCalls(b.Calls(), "Start") != 0 {
			t.Fatal("acted on unknown observation")
		}
	})
	t.Run("new_volume_same_marker", func(t *testing.T) {
		rc, b, m := recoveryFixture(t, true)
		if _, err := b.DeleteService(t.Context(), m.Member); err != nil {
			t.Fatal(err)
		}
		b.DeleteService(t.Context(), m.Volume)
		b.ImportServices(t.Context(), []infra.ServiceSpec{VolumeServiceSpec(m.Cluster, m.Member, m.Volume, m.Token, domain.Resources{})})
		if _, err := ReconcileRuntime(t.Context(), rc, m); !errors.Is(err, ErrConflict) {
			t.Fatal("new empty incarnation adopted", err)
		}
	})
	t.Run("foreign_runtime", func(t *testing.T) {
		rc, b, m := recoveryFixture(t, false)
		b.DeleteService(t.Context(), m.Member)
		b.ImportServices(t.Context(), []infra.ServiceSpec{RuntimeServiceSpec(b.Layout(), MemberService{Cluster: "foreign", Member: m.Member, Token: "other"})})
		if _, err := ReconcileRuntime(t.Context(), rc, m); !errors.Is(err, ErrConflict) {
			t.Fatal("foreign runtime adopted", err)
		}
	})
	t.Run("submission_cooldown", func(t *testing.T) {
		rc, b, m := recoveryFixture(t, false)
		b.Stop(t.Context(), m.Member)
		b.FailNext("Start", infra.ErrUnavailable)
		if _, err := ReconcileRuntime(t.Context(), rc, m); err == nil {
			t.Fatal("failure hidden")
		}
		ReconcileRuntime(t.Context(), rc, m)
		if countRecoveryCalls(b.Calls(), "Start") != 1 {
			t.Fatal("retry storm")
		}
		rc.Clock.(*recoveryClock).now = rc.Clock.Now().Add(time.Minute)
		ReconcileRuntime(t.Context(), rc, m)
		if countRecoveryCalls(b.Calls(), "Start") != 2 {
			t.Fatal("recovery did not retry")
		}
	})
	for _, phase := range []domain.Phase{domain.PhaseSuspended, domain.PhaseDeleting} {
		t.Run(string(phase), func(t *testing.T) {
			rc, b, m := recoveryFixture(t, true)
			b.Stop(t.Context(), m.Member)
			rc.Cluster.Status.Phase = phase
			rc.Cluster.Deleting = true
			ReconcileRuntime(t.Context(), rc, m)
			if countRecoveryCalls(b.Calls(), "Start") != 0 {
				t.Fatal("fought intentional stop")
			}
		})
	}
}

type recoveryStore struct {
	ClusterStore
	ops      map[string]*domain.Operation
	enqueues int
}

func (s *recoveryStore) GetOperation(_ context.Context, id string) (*domain.Operation, error) {
	if op := s.ops[id]; op != nil {
		return op, nil
	}
	return nil, ErrNotFound
}
func (s *recoveryStore) EnqueueOperation(_ context.Context, op *domain.Operation) error {
	s.enqueues++
	s.ops[op.ID] = op
	return nil
}
func TestRuntimeRecoveryVolumeReplacementAdmission(t *testing.T) {
	rc, _, m := recoveryFixture(t, true)
	st := &recoveryStore{ops: map[string]*domain.Operation{}}
	rc.Store = st
	if err := RecoverLostVolume(t.Context(), rc, m.Member, m.Volume, m.VolumeID, false); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "restore a backup") {
		t.Fatal("singleton not refused precisely", err)
	}
	if st.enqueues != 0 {
		t.Fatal("singleton replacement queued")
	}
	for range 2 {
		if err := RecoverLostVolume(t.Context(), rc, m.Member, m.Volume, m.VolumeID, true); err != nil {
			t.Fatal(err)
		}
	}
	if st.enqueues != 1 {
		t.Fatal("replacement queue not idempotent")
	}
	for _, op := range st.ops {
		if op.Kind != domain.OperationReplaceMember || !strings.Contains(string(op.Payload), m.Member) {
			t.Fatal("wrong replacement", op)
		}
		op.Phase = domain.PhaseFailed
	}
	if err := RecoverLostVolume(t.Context(), rc, m.Member, m.Volume, m.VolumeID, true); !errors.Is(err, ErrConflict) {
		t.Fatal("failed replacement endlessly requeued", err)
	}
	status := domain.ClusterStatus{Phase: domain.PhaseReady, Members: []domain.MemberStatus{{Name: m.Member, Health: true}}}
	ApplyRuntimeRecoveryStatus(&status, m.Member, m.Volume, RuntimeRecovery{VolumeLost: true}, rc.Clock.Now())
	if status.Phase != domain.PhaseDegraded || status.Members[0].Health || status.Conditions[0].Type != "VolumeLost" {
		t.Fatal("volume loss hidden", status)
	}
}

type recoveryAgentDialer struct {
	url string
	ca  []byte
}

func (d recoveryAgentDialer) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.url, CAPEM: d.ca, Token: "token"})
}
func TestRuntimeRecoveryPhysicalDataLoss(t *testing.T) {
	for _, verifiedID := range []bool{false, true} {
		t.Run(map[bool]string{false: "foreign_physical_loss", true: "owned_physical_loss"}[verifiedID], func(t *testing.T) {
			rc, b, m := recoveryFixture(t, true)
			id := m.VolumeID
			if !verifiedID {
				id = "foreign"
			}
			b.FailNext("GetService", &infra.VolumeLostError{Volume: m.Volume, ServiceID: id, Message: "original physical data is absent"})
			out, err := ReconcileRuntime(t.Context(), rc, m)
			if verifiedID {
				if err != nil || !out.VolumeLost || !m.Recovery.VolumeLost {
					t.Fatal("owned physical loss not routed to replacement", out, err)
				}
			} else if !errors.Is(err, infra.ErrConflict) || out.VolumeLost {
				t.Fatal("foreign loss authorized replacement", out, err)
			}
		})
	}
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "initialized_receipt", true: "agent_sentinel_lost"}[lost], func(t *testing.T) {
			rc, b, m := recoveryFixture(t, true)
			status := agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}}, Volume: &agentapi.VolumeStatus{Identity: agentapi.VolumeIdentity{ClusterUID: "uid", Member: m.Member, Volume: m.Volume, VolumeID: m.VolumeID}, Verified: !lost, Lost: lost}}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(status) }))
			defer server.Close()
			rc.Agents = recoveryAgentDialer{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
			out, err := ReconcileRuntime(t.Context(), rc, m)
			if err != nil || out.VolumeLost != lost {
				t.Fatal(out, err)
			}
			if !lost && !m.Recovery.Initialized {
				t.Fatal("successful native start did not record initialization")
			}
			if countRecoveryCalls(b.Calls(), "Start") != 0 || countRecoveryCalls(b.Calls(), "ImportServices") != 1 {
				t.Fatal("physical loss triggered empty-runtime bootstrap")
			}
		})
	}
}

type recoveryObservedBackend struct {
	infra.Backend
	service *infra.Service
	err     error
}

func (b recoveryObservedBackend) GetService(context.Context, string) (*infra.Service, error) {
	return b.service, b.err
}
func TestRuntimeRecoveryRetirementObservation(t *testing.T) {
	for _, scenario := range []string{"original_runtime", "original_volume", "foreign_id", "wrong_kind", "wrong_error_identity", "unknown_api", "not_classified", "nil_service"} {
		t.Run(scenario, func(t *testing.T) {
			name, id, kind := "member", "runtime-id", infra.KindRuntime
			if scenario == "original_volume" {
				name, id, kind = "volume", "volume-id", infra.KindVolume
			}
			service := &infra.Service{Hostname: name, ID: id, Kind: kind, Status: infra.StatusFailed}
			loss := &infra.VolumeLostError{Volume: "volume", ServiceID: "volume-id", Message: "physical data is missing"}
			var observedErr error = loss
			classified := scenario != "not_classified"
			switch scenario {
			case "foreign_id":
				service.ID = "foreign"
			case "wrong_kind":
				service.Kind = infra.KindVolume
			case "wrong_error_identity":
				loss.ServiceID = "foreign-volume"
			case "unknown_api":
				observedErr = infra.ErrUnavailable
			case "nil_service":
				service = nil
			}
			b := recoveryObservedBackend{service: service, err: observedErr}
			got, err := ObserveRetiringService(t.Context(), b, name, id, "volume", "volume-id", classified)
			allowed := scenario == "original_runtime" || scenario == "original_volume"
			if (err == nil) != allowed {
				t.Fatal("unsafe retirement observation classification", got, err)
			}
			if allowed && got.Status != infra.StatusFailed {
				t.Fatal("storage loss was treated as stopped proof")
			}
		})
	}
}

func TestRuntimeRecoveryBoundsReadOnlyStatusProbe(t *testing.T) {
	rc, b, m := recoveryFixture(t, true)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	rc.Agents = recoveryAgentDialer{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := time.Now()
	out, err := ReconcileRuntime(ctx, rc, m)
	if elapsed := time.Since(started); elapsed >= 3*time.Second {
		t.Fatalf("read-only status probe exceeded its independent two-second deadline: %v", elapsed)
	}
	if err != nil || out.VolumeLost || out.Busy {
		t.Fatal("unresponsive status authorized recovery", out, err)
	}
	if ctx.Err() != nil {
		t.Fatal("read-only deadline cancelled caller mutation context")
	}
	if countRecoveryCalls(b.Calls(), "Start") != 0 || countRecoveryCalls(b.Calls(), "ImportServices") != 1 {
		t.Fatal("read-only timeout mutated runtime")
	}
}

func TestRuntimeRecoveryUnknownNativeDataNeverAuthorizesReplacement(t *testing.T) {
	for _, scenario := range []string{"mongodb-relations", "postgres-partial", "postgres-old-retired", "elasticsearch-indices", "rabbitmq-messages"} {
		t.Run(scenario, func(t *testing.T) {
			rc, b, m := recoveryFixture(t, true)
			m.Recovery.Initialized = true
			st := &recoveryStore{ops: map[string]*domain.Operation{}}
			rc.Store = st
			status := agentapi.Status{
				Processes: map[string]agentapi.ProcessStatus{"engine": {State: "stopped"}},
				Volume: &agentapi.VolumeStatus{
					Identity: agentapi.VolumeIdentity{Member: m.Member, Volume: m.Volume, VolumeID: m.VolumeID, Initialized: true},
					Message:  "VolumeIdentityUnavailable: retained " + scenario + " data requires original-volume adoption",
				},
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(status)
			}))
			defer server.Close()
			rc.Agents = recoveryAgentDialer{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
			before := b.Calls()
			for range 2 {
				out, err := ReconcileRuntime(t.Context(), rc, m)
				if err != nil || out.VolumeLost || m.Recovery.VolumeLost || out.Imported || out.Busy || m.Recovery.Op != nil {
					t.Fatal("unknown retained data authorized runtime replacement", out, m.Recovery, err)
				}
			}
			for _, mutation := range []string{"Start", "Stop", "DeleteService", "ImportServices"} {
				if countRecoveryCalls(b.Calls(), mutation) != countRecoveryCalls(before, mutation) {
					t.Fatal("unknown native data mutated runtime/storage", mutation)
				}
			}
			volume, err := b.GetService(t.Context(), m.Volume)
			if err != nil || volume == nil || volume.ID != m.VolumeID || st.enqueues != 0 {
				t.Fatal("unknown native data retired its original volume or queued replacement", volume, st.enqueues, err)
			}
		})
	}
}
