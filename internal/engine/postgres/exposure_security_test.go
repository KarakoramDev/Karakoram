// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type exposureBackend struct {
	*fakeBackend
	routes map[string]infra.PublicPort
}

func (b *exposureBackend) Capabilities() infra.Capabilities {
	c := b.fakeBackend.Capabilities()
	c.TCPAllowlist = true
	return c
}
func (b *exposureBackend) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{PublicIPv4: "192.0.2.1"}, nil
}
func (b *exposureBackend) EnsurePublicPort(_ context.Context, name string, port infra.PublicPort) error {
	b.routes[name] = port
	return nil
}
func (b *exposureBackend) DeletePublicPorts(_ context.Context, name string) error {
	delete(b.routes, name)
	return nil
}
func (b *exposureBackend) ListPublicPorts(context.Context) ([]infra.PublicPortRoute, error) {
	result := []infra.PublicPortRoute{}
	for name, port := range b.routes {
		result = append(result, infra.PublicPortRoute{Hostname: name, PublicPort: port})
	}
	return result, nil
}

func TestExposureRevocationDoesNotWaitForHealth(t *testing.T) {
	for _, change := range []string{"disable", "tighten", "mixed", "disjoint", "expand", "enable"} {
		t.Run(change, func(t *testing.T) {
			p, rc, base := setup(t, "ha")
			b := &exposureBackend{fakeBackend: base, routes: map[string]infra.PublicPort{}}
			rc.Backend = b
			rc.Cluster.Spec.Exposure = domain.Exposure{Public: change != "enable", IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
			converge(t, p, rc)
			base.statusOverrides = map[string]pgspec.StatusResponse{"demop1": {Running: false}}
			switch change {
			case "disable":
				rc.Cluster.Spec.Exposure.Public = false
			case "tighten":
				rc.Cluster.Spec.Exposure.Allowlist = []string{"192.0.2.0/25"}
			case "mixed":
				rc.Cluster.Spec.Exposure.Allowlist = []string{"192.0.2.0/25", "198.51.100.0/24"}
			case "disjoint":
				rc.Cluster.Spec.Exposure.Allowlist = []string{"198.51.100.0/24"}
			case "expand":
				rc.Cluster.Spec.Exposure.Allowlist = []string{"0.0.0.0/0"}
			case "enable":
				rc.Cluster.Spec.Exposure.Public = true
			}
			rc.Cluster.Generation++
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			route, exists := b.routes["demopx"]
			switch change {
			case "disable", "disjoint", "enable":
				if exists {
					t.Fatal("revocation blocked or addition allowed while degraded")
				}
			case "tighten", "mixed":
				if !exists || !reflect.DeepEqual(route.Allowlist, []string{"192.0.2.0/25"}) {
					t.Fatalf("old access retained or new access added: %+v", route)
				}
			case "expand":
				if !exists || !reflect.DeepEqual(route.Allowlist, []string{"192.0.2.0/24"}) {
					t.Fatalf("allowlist expanded before readiness: %+v", route)
				}
			}
		})
	}
}

func TestAllowlistIntersection(t *testing.T) {
	for _, tc := range []struct{ current, desired, want []string }{
		{nil, []string{"192.0.2.0/24"}, []string{"192.0.2.0/24"}},
		{[]string{"192.0.2.0/24"}, nil, []string{"192.0.2.0/24"}},
		{[]string{"2001:db8::/32"}, []string{"2001:db8:1::/48"}, []string{"2001:db8:1::/48"}},
		{[]string{"192.0.2.0/24"}, []string{"2001:db8::/32"}, []string{}},
	} {
		got, err := intersectAllowlists(tc.current, tc.desired)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("intersection=%v want=%v err=%v", got, tc.want, err)
		}
	}
}

type conflictingExposureBackend struct {
	*exposureBackend
	released bool
}

func (b *conflictingExposureBackend) EnsurePublicPort(ctx context.Context, name string, port infra.PublicPort) error {
	if !b.released {
		return &infra.PublicPortConflictError{Port: 5432}
	}
	return b.exposureBackend.EnsurePublicPort(ctx, name, port)
}

func TestPublicPortConflictPreservesPrivatePostgresIntent(t *testing.T) {
	p, rc, base := setup(t, "single")
	b := &conflictingExposureBackend{exposureBackend: &exposureBackend{fakeBackend: base, routes: map[string]infra.PublicPort{}}}
	rc.Backend = b
	s := converge(t, p, rc)
	target := connectionMember(rc, s)
	rc.Cluster.Spec.Exposure = domain.Exposure{Public: true, IPType: "IPV4"}
	_, err := p.reconcileExposure(t.Context(), rc, s)
	var conflict *infra.PublicPortConflictError
	if !errors.As(err, &conflict) || conflict.Port != 5432 {
		t.Fatalf("publication conflict lost: %v", err)
	}
	if !rc.Cluster.Spec.Exposure.Public || connectionMember(rc, s) != target || len(b.routes) != 0 {
		t.Fatal("refused publication changed requested exposure or private client service")
	}
	for _, m := range s.Members {
		if m.Public {
			t.Fatalf("refused route acknowledged as public: %s", m.Name)
		}
	}
}

func TestPublicPortRefusalFullSyncPrivateCorrectionAndReenable(t *testing.T) {
	p, rc, base := setup(t, "single")
	b := &conflictingExposureBackend{exposureBackend: &exposureBackend{fakeBackend: base, routes: map[string]infra.PublicPort{}}}
	rc.Backend = b
	converge(t, p, rc)
	rc.Cluster.Spec.Exposure = domain.Exposure{Public: true, IPType: "IPV4"}
	rc.Cluster.Generation++
	var conflict *infra.PublicPortConflictError
	for range 100 {
		_, err := p.Sync(t.Context(), rc)
		if errors.As(err, &conflict) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if conflict == nil {
		t.Fatal("full Sync did not expose the refusal")
	}
	for _, public := range []bool{false, true} {
		b.released = true
		rc.Cluster.Spec.Exposure.Public = public
		rc.Cluster.Generation++
		for range 100 {
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
				break
			}
		}
		if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
			t.Fatalf("full Sync stuck after correcting exposure to %v: %s", public, rc.SyncWait)
		}
		s, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range s.Members {
			if m.Name == connectionMember(rc, s) && m.Public != public {
				t.Fatalf("public acknowledgement=%v want %v", m.Public, public)
			}
		}
	}
}
