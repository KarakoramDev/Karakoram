package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

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
