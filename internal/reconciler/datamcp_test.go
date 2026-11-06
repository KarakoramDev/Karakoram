package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type dataProvider struct {
	*fakeProvider
	kind engine.DataMcpKind
}

func (p *dataProvider) DataMcp(*domain.Cluster) (engine.DataMcpBackend, error) {
	return engine.DataMcpBackend{Kind: p.kind, Upstream: companionspec.UpstreamMongoDB}, nil
}
func (*dataProvider) DataAccessCredentials(context.Context, *engine.Context, engine.DataAccessLevel) (engine.DataAccessCredentials, error) {
	return engine.DataAccessCredentials{}, nil
}

func TestDataMcpStatusSurvivesProviderReplacement(t *testing.T) {
	p := &dataProvider{fakeProvider: &fakeProvider{calls: map[string]int{}, active: map[string]bool{}}, kind: engine.DataMcpNative}
	m, st := newTestManager(t, p)
	backend := fake.New(fake.Options{})
	m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: backend}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", DataMcp: &domain.DataMcpSpec{Enabled: true}}, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.DataMcp == nil || got.Status.DataMcp.Kind != "native" || got.Status.DataMcp.Phase != "Ready" || len(got.Status.Members) != 0 {
		t.Fatal(got.Status)
	}
	if err = m.observeDuringBackoff(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	got, err = st.GetCluster(t.Context(), c.Ref())
	if err != nil || got.Status.DataMcp == nil || got.Status.DataMcp.Phase != "Ready" {
		t.Fatal(got, err)
	}
}

func TestClusterDeleteWaitsForCompanionWithAndWithoutVolumes(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "keep-volumes"}[keep], func(t *testing.T) {
			p := &dataProvider{fakeProvider: &fakeProvider{cleanup: 2}, kind: engine.DataMcpCompanion}
			m, st := newTestManager(t, p)
			backend := fake.New(fake.Options{Polls: 2})
			m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: backend}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if err := st.SetAgentToken(t.Context(), &domain.AgentToken{Namespace: c.Namespace, Cluster: c.Name, Member: "demomcp", Token: "token"}); err != nil {
				t.Fatal(err)
			}
			op, err := backend.ImportServices(t.Context(), []infra.ServiceSpec{engine.RuntimeServiceSpec(backend.Layout(), engine.MemberService{Cluster: "demo", Member: "demomcp", Token: "token", Containers: 1})})
			if err != nil {
				t.Fatal(err)
			}
			for range 4 {
				if _, err = backend.OpStatus(t.Context(), op); err != nil {
					t.Fatal(err)
				}
			}
			service, err := backend.GetService(t.Context(), "demomcp")
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(map[string]string{"id": service.ID, "stage": "ready"})
			if err = st.SetClusterSecret(t.Context(), c.Ref(), "datamcp/state", raw); err != nil {
				t.Fatal(err)
			}
			if err = st.MarkClusterDeleting(t.Context(), c.Ref(), domain.DeleteOptions{KeepVolumes: keep}); err != nil {
				t.Fatal(err)
			}
			if _, err = m.reconcile(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			got, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil || got.Status.Phase == domain.PhaseSuspended {
				t.Fatal("cluster finalized before companion", got, err)
			}
			for range 12 {
				_, err = m.reconcile(t.Context(), c.Ref())
				if errors.Is(err, errClusterGone) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				got, err = st.GetCluster(t.Context(), c.Ref())
				if keep && err == nil && got.Status.Phase == domain.PhaseSuspended {
					break
				}
			}
			if keep {
				got, err = st.GetCluster(t.Context(), c.Ref())
				if err != nil || got.Status.Phase != domain.PhaseSuspended {
					t.Fatal(got, err)
				}
			} else if _, err = st.GetCluster(t.Context(), c.Ref()); !errors.Is(err, store.ErrNotFound) {
				t.Fatal("cluster not deleted", err)
			}
			service, err = backend.GetService(t.Context(), "demomcp")
			if err != nil || service != nil {
				t.Fatal("companion retained", service, err)
			}
		})
	}
}

type companionObservationProvider struct {
	*dataProvider
	unavailable bool
}

func (p *companionObservationProvider) Status(_ context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	if p.unavailable {
		return domain.ClusterStatus{}, errors.New("native agent unavailable")
	}
	return domain.ClusterStatus{
		Phase:      domain.PhaseReady,
		Conditions: []domain.Condition{{Type: "Ready", Status: "True", Reason: "MembersReady"}},
		Members:    []domain.MemberStatus{{Name: "member", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: rc.Clock.Now()}}},
	}, nil
}

type slowCompanionSecrets struct {
	engine.SecretStore
	key string
}

func (s slowCompanionSecrets) GetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string) ([]byte, error) {
	if key == s.key {
		return nil, waitForSlowCompanion(ctx)
	}
	return s.SecretStore.GetClusterSecret(ctx, ref, key)
}

type slowCompanionDialer struct{}

func (slowCompanionDialer) Dial(ctx context.Context, _ domain.ClusterRef, _ string) (*agentapi.Client, error) {
	return nil, waitForSlowCompanion(ctx)
}

func waitForSlowCompanion(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(20 * time.Second):
		return errors.New("companion still rolling")
	}
}

func TestSlowCompanionDoesNotStarveNativePublication(t *testing.T) {
	for _, path := range []string{"ordinary", "backoff", "renewal"} {
		for _, stage := range []string{"state", "config", "dial"} {
			t.Run(path+"/"+stage, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					p := &companionObservationProvider{dataProvider: &dataProvider{fakeProvider: &fakeProvider{}, kind: engine.DataMcpCompanion}}
					m, st := newTestManager(t, p)
					m.opts.Context.ObservationEpoch = "current-process"
					m.opts.Context.Agents = slowCompanionDialer{}
					m.opts.Context.Secrets = slowCompanionSecrets{SecretStore: st, key: "datamcp/" + stage}
					m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: fake.New(fake.Options{})}
					c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", DataMcp: &domain.DataMcpSpec{Enabled: true}},
						Status: domain.ClusterStatus{Phase: domain.PhaseReady, DataMcp: &domain.DataMcpStatus{Phase: domain.DataMcpPhaseReady}}}
					if err := st.CreateCluster(t.Context(), c); err != nil {
						t.Fatal(err)
					}
					for key, raw := range map[string]string{"datamcp/state": `{"stage":"ready"}`, "datamcp/config": `{"configVersion":1}`} {
						if err := st.SetClusterSecret(t.Context(), c.Ref(), key, []byte(raw)); err != nil {
							t.Fatal(err)
						}
					}
					gate := make(chan struct{}, 1)
					publish := func() error {
						ctx := withObservationPublication(t.Context(), gate)
						switch path {
						case "backoff":
							return m.observeDuringBackoff(ctx, c.Ref())
						case "renewal":
							return m.renewAvailability(ctx, c.Ref())
						default:
							// The ordinary lifecycle path owns the gate through
							// its state write, just like the renewal path.
							if err := holdObservation(ctx); err != nil {
								return err
							}
							defer releaseObservation(ctx)
							current, err := st.GetCluster(ctx, c.Ref())
							if err != nil {
								return err
							}
							rc := m.opts.Context
							rc.Cluster = current
							current.Status, _ = m.observeStatus(ctx, p, &rc)
							return m.state(ctx, current, current.Status.Phase)
						}
					}
					var previous time.Time
					// Repeated blocked companion reads span more than the native
					// lease, but each new native sample must reach persistence.
					for range 5 {
						start := time.Now()
						if err := publish(); err != nil {
							t.Fatal(err)
						}
						if elapsed := time.Since(start); elapsed > engine.ObservationTimeout {
							t.Fatalf("companion delayed native publication for %s", elapsed)
						}
						stored, err := st.GetCluster(t.Context(), c.Ref())
						if err != nil {
							t.Fatal(err)
						}
						fresh := engine.FreshStatus(stored.Status, m.opts.Context.Clock.Now())
						if fresh.Phase != domain.PhaseReady || len(fresh.Members) != 1 || !fresh.Members[0].Health ||
							!fresh.Members[0].Agent.Healthy || !fresh.ObservedAt.After(previous) {
							t.Fatalf("native health was not freshly published: %+v", fresh)
						}
						if fresh.DataMcp == nil || fresh.DataMcp.Phase != domain.DataMcpPhaseDegraded ||
							fresh.DataMcp.Message != "Companion readiness unavailable" {
							t.Fatalf("blocked companion retained positive readiness: %+v", fresh.DataMcp)
						}
						previous = fresh.ObservedAt
					}
					// A real native failure must still withdraw positive health.
					p.unavailable = true
					if err := publish(); err != nil {
						t.Fatal(err)
					}
					stored, err := st.GetCluster(t.Context(), c.Ref())
					if err != nil {
						t.Fatal(err)
					}
					failed := engine.FreshStatus(stored.Status, m.opts.Context.Clock.Now())
					if failed.Phase == domain.PhaseReady {
						t.Fatal("companion timeout concealed native failure", failed)
					}
					for _, member := range failed.Members {
						if member.Health || member.Agent.Healthy {
							t.Fatal("native failure retained positive member health", failed)
						}
					}
				})
			})
		}
	}
}
