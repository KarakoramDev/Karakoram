// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type uncertainPublicationBackend struct {
	*fake.Backend
	failed       bool
	publications int
}

func (b *uncertainPublicationBackend) EnsurePublicPort(ctx context.Context, name string, port infra.PublicPort) error {
	b.publications++
	if err := b.Backend.EnsurePublicPort(ctx, name, port); err != nil {
		return err
	}
	if !b.failed {
		b.failed = true
		return errors.New("publication reply lost")
	}
	return nil
}
func TestExposureUncertainReplyRestartReplay(t *testing.T) {
	ctx := context.Background()
	f := coreSetup(t, 3)
	s := coreConverge(t, f)
	f.rc.Cluster.Spec.Exposure.Public = true
	if err := f.rc.Store.(*store.Store).UpdateClusterSpec(ctx, f.rc.Cluster.Ref(), f.rc.Cluster.Spec); err != nil {
		t.Fatal(err)
	}
	b := &uncertainPublicationBackend{Backend: f.backend}
	f.rc.Backend = b
	busy, err := f.p.reconcileExposure(ctx, f.rc, s)
	if !busy || err == nil {
		t.Fatal("missing uncertain reply")
	}
	// Save/load through the same store used by a restarted provider, not just
	// a retained Go pointer. Publication must have its allocation checkpoint.
	if err = f.rc.Store.(*store.Store).UpdateClusterState(ctx, f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	restored, err := f.rc.Store.GetCluster(ctx, f.rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	f.rc.Cluster = restored
	s, err = load(restored)
	if err != nil {
		t.Fatal(err)
	}
	allocated := s.Members[0].PublicHTTPPort
	if allocated == 0 {
		t.Fatal("uncertain publication lost durable allocation")
	}
	f.p = New(Options{Store: f.rc.Store})
	for range 20 {
		busy, err = f.p.reconcileExposure(ctx, f.rc, s)
		if err != nil {
			t.Fatal(err)
		}
		if !busy {
			break
		}
	}
	routes, err := b.ListPublicPorts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 6 || b.publications != 6 || s.Members[0].PublicHTTPPort != allocated {
		t.Fatal("publication replay duplicated routes or reallocated", len(routes), b.publications)
	}
}
func TestExposureCrossClusterCollisionAndReclamation(t *testing.T) {
	ctx := context.Background()
	f := coreSetup(t, 3)
	s := coreConverge(t, f)
	// Another engine's persisted single publicPort shares the namespace pool.
	other := &domain.Cluster{Name: "other", Internal: json.RawMessage(`{"members":[{"name":"other0","publicPort":8080}]}`)}
	if err := f.rc.Store.(*store.Store).CreateCluster(ctx, other); err != nil {
		t.Fatal(err)
	}
	port, err := f.p.allocatePublicPort(ctx, f.rc, s, s.Members[0], 8080)
	if err != nil || port != 8081 {
		t.Fatal("cross-engine persisted reservation ignored", port, err)
	}
	s.Members[0].PublicHTTPPort = port
	// A route can appear after allocation. Never overwrite the competing owner.
	s.Members[0].PublicHTTPPort = 8080
	f.rc.Cluster.Spec.Exposure.Public = true
	if err = f.backend.EnsurePublicPort(ctx, s.Members[1].Name, infra.PublicPort{Port: 8080, TargetPort: 8080, Protocol: "tcp"}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.p.reconcileExposure(ctx, f.rc, s); err == nil || !strings.Contains(err.Error(), "owned by another") {
		t.Fatal("route collision did not fail closed", err)
	}
	if err = f.backend.DeletePublicPorts(ctx, s.Members[1].Name); err != nil {
		t.Fatal(err)
	}
	s.Members[0].PublicHTTPPort = 8081
	if err = s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if err = f.rc.Store.(*store.Store).UpdateClusterState(ctx, f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	// Retirement removes the member's durable allocation. The same live provider
	// must reclaim the port, despite its previous in-memory reservation.
	retired := s.Members[0]
	s.Members = s.Members[1:]
	port, err = f.p.allocatePublicPort(ctx, f.rc, s, s.Members[0], 8080)
	if err != nil || port != 8081 {
		t.Fatal("retired port not reclaimed", port, err)
	}
	// Allocation now belongs to the surviving member. Checkpoint it before
	// disabling exposure; an unassigned in-flight reservation must stay excluded.
	s.Members[0].PublicHTTPPort = port
	retired.PublicHTTPPort = 0
	retired.PublicGRPCPort = 0
	s.Members = append([]*member{retired}, s.Members...)
	if err = s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	f.rc.Cluster.Spec.Exposure.Public = false
	for range 10 {
		busy, e := f.p.reconcileExposure(ctx, f.rc, s)
		if e != nil {
			t.Fatal(e)
		}
		if !busy {
			break
		}
	}
	port, err = f.p.allocatePublicPort(ctx, f.rc, s, s.Members[0], 8080)
	if err != nil || port != 8081 {
		t.Fatal("disabled exposure did not reclaim port", port, err)
	}
}

func TestExposureConcurrentClusterReservationsSurviveBeforePersistence(t *testing.T) {
	ctx := context.Background()
	f := coreSetup(t, 3)
	s := coreConverge(t, f)
	other := &domain.Cluster{Name: "other"}
	if err := f.rc.Store.(*store.Store).CreateCluster(ctx, other); err != nil {
		t.Fatal(err)
	}
	otherContext := *f.rc
	otherContext.Cluster = other
	otherMember := &member{Name: "other0"}
	otherState := &state{Members: []*member{otherMember}}
	type allocation struct {
		port int
		err  error
	}
	reserved := make(chan allocation)
	release := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		port, err := f.p.allocatePublicPort(ctx, f.rc, s, s.Members[0], 8080)
		reserved <- allocation{port, err}
		// The first worker is suspended before assigning/persisting the port or
		// publishing a backend route while the other cluster allocates.
		<-release
	}()
	first := <-reserved
	second, secondErr := f.p.allocatePublicPort(ctx, &otherContext, otherState, otherMember, 8080)
	close(release)
	<-finished
	if first.err != nil || secondErr != nil {
		t.Fatal(first.err, secondErr)
	}
	if first.port == second {
		t.Fatalf("concurrent clusters shared unpublished port %d", first.port)
	}
	third, err := f.p.allocatePublicPort(ctx, f.rc, s, s.Members[0], 8080)
	if err != nil || third == first.port || third == second {
		t.Fatal("subsequent allocation lost outstanding reservation", third, err)
	}
}

func TestCleanupReclaimsOnlyCompletedClusterReservations(t *testing.T) {
	for _, keepVolumes := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "suspend"}[keepVolumes], func(t *testing.T) {
			ctx := t.Context()
			f := coreSetup(t, 1)
			s := coreConverge(t, f)
			other := &domain.Cluster{Name: "other"}
			if err := f.rc.Store.(*store.Store).CreateCluster(ctx, other); err != nil {
				t.Fatal(err)
			}
			otherContext := *f.rc
			otherContext.Cluster = other
			otherMember := &member{Name: "other0"}
			otherState := &state{Members: []*member{otherMember}}
			type allocation struct {
				port int
				err  error
			}
			a, b := make(chan allocation, 1), make(chan allocation, 1)
			go func() {
				port, err := f.p.allocatePublicPort(ctx, f.rc, s, s.Members[0], 8080)
				a <- allocation{port, err}
			}()
			go func() {
				port, err := f.p.allocatePublicPort(ctx, &otherContext, otherState, otherMember, 8080)
				b <- allocation{port, err}
			}()
			first, second := <-a, <-b
			if first.err != nil || second.err != nil || first.port == second.port {
				t.Fatal("concurrent unpublished allocations", first, second)
			}
			s.Members[0].PublicHTTPPort = first.port
			if err := s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			done := false
			for range 150 {
				var err error
				done, err = f.p.Cleanup(ctx, f.rc, domain.DeleteOptions{KeepVolumes: keepVolumes})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done {
				t.Fatal("cleanup did not complete")
			}
			cleaned, err := load(f.rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if cleaned.Members[0].PublicHTTPPort != 0 || cleaned.Members[0].PublicGRPCPort != 0 || cleaned.Suspended != keepVolumes {
				t.Fatal("cleanup retained listener assignments or lost suspension")
			}
			if err := f.rc.Store.(*store.Store).UpdateClusterState(ctx, f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			thirdContext := *f.rc
			thirdContext.Cluster = &domain.Cluster{Name: "third"}
			thirdMember := &member{Name: "third0"}
			thirdState := &state{Members: []*member{thirdMember}}
			port, err := f.p.allocatePublicPort(ctx, &thirdContext, thirdState, thirdMember, first.port)
			if err != nil || port != first.port {
				t.Fatal("completed cleanup did not reclaim A's port", port, err)
			}
			if f.p.portReservations[other.Ref().Namespace][second.port] != "other/other0" {
				t.Fatal("cleanup released B's unpublished reservation")
			}
		})
	}
}

type failedRouteDeletionBackend struct {
	*fake.Backend
	routes []infra.PublicPortRoute
}

func (b *failedRouteDeletionBackend) ListPublicPorts(ctx context.Context) ([]infra.PublicPortRoute, error) {
	if b.routes != nil {
		return b.routes, nil
	}
	return b.Backend.ListPublicPorts(ctx)
}

func (b *failedRouteDeletionBackend) DeletePublicPorts(context.Context, string) error {
	return errors.New("route deletion unavailable")
}

func TestReservationReleaseWaitsForRouteDeletion(t *testing.T) {
	f := coreSetup(t, 1)
	s := coreConverge(t, f)
	m := s.Members[0]
	port, err := f.p.allocatePublicPort(t.Context(), f.rc, s, m, 8080)
	if err != nil {
		t.Fatal(err)
	}
	m.PublicHTTPPort = port
	m.CleanupStep = "stopped"
	if err := f.backend.EnsurePublicPort(t.Context(), m.Name, infra.PublicPort{Port: port, TargetPort: 8080}); err != nil {
		t.Fatal(err)
	}
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	b := &failedRouteDeletionBackend{Backend: f.backend}
	f.rc.Backend = b
	if _, err := f.p.Cleanup(t.Context(), f.rc, domain.DeleteOptions{}); err == nil {
		t.Fatal("cleanup ignored failed route deletion")
	}
	if _, err := f.p.reconcileExposure(t.Context(), f.rc, s); err == nil {
		t.Fatal("disable exposure ignored failed route deletion")
	}
	if f.p.portReservations[f.rc.Cluster.Ref().Namespace][port] != f.rc.Cluster.Name+"/"+m.Name {
		t.Fatal("failed backend mutation released reservation")
	}
	// A runtime may already be absent while its public route survived. Cleanup
	// must still retry that route, not declare the listener pool released.
	// The fake stores routes inside the runtime, so deleting the service also
	// erases them. Preserve the observed route in this backend wrapper to model
	// a platform whose independent public route outlives its runtime.
	b.routes, err = f.backend.ListPublicPorts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.DeleteService(t.Context(), m.Name); err != nil {
		t.Fatal(err)
	}
	if _, err := f.p.Cleanup(t.Context(), f.rc, domain.DeleteOptions{}); err == nil {
		t.Fatal("cleanup skipped route deletion for an absent runtime")
	}
	if f.p.portReservations[f.rc.Cluster.Ref().Namespace][port] == "" {
		t.Fatal("orphan route lost its reservation")
	}
}

func TestReservationReleaseWithStaleCheckpoint(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(map[bool]string{false: "disable", true: "cleanup-secrets"}[replay], func(t *testing.T) {
			f := coreSetup(t, 1)
			s := coreConverge(t, f)
			m := s.Members[0]
			ns := f.rc.Cluster.Ref().Namespace
			// The old checkpoint still names a port now held by another owner.
			m.PublicHTTPPort = 8080
			f.p.portReservations[ns] = map[int]string{
				8080: "other/other0",
				8081: f.rc.Cluster.Name + "/" + m.Name,
			}
			// Identically named clusters in another namespace are independent.
			f.p.portReservations["another"] = map[int]string{8081: f.rc.Cluster.Name + "/" + m.Name}
			if replay {
				s.Phase = "cleanup-secrets"
				s.CleanupSecrets = true
				if err := s.save(f.rc.Cluster); err != nil {
					t.Fatal(err)
				}
				before := len(f.backend.Calls())
				if done, err := f.p.Cleanup(t.Context(), f.rc, domain.DeleteOptions{}); err != nil || !done {
					t.Fatal("cleanup-secrets replay", done, err)
				}
				if len(f.backend.Calls()) != before {
					t.Fatal("cleanup-secrets replay revisited backend")
				}
			} else {
				if busy, err := f.p.reconcileExposure(t.Context(), f.rc, s); err != nil || !busy {
					t.Fatal("disable exposure", busy, err)
				}
			}
			if f.p.portReservations[ns][8080] != "other/other0" || f.p.portReservations[ns][8081] != "" || f.p.portReservations["another"][8081] == "" {
				t.Fatal("release crossed reservation ownership or namespace")
			}
		})
	}
}

func TestReleasedReservationIsNotResurrectedFromStaleStore(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(map[bool]string{false: "disable", true: "cleanup-secrets"}[replay], func(t *testing.T) {
			ctx := t.Context()
			f := coreSetup(t, 1)
			s := coreConverge(t, f)
			m := s.Members[0]
			portA, err := f.p.allocatePublicPort(ctx, f.rc, s, m, 8080)
			if err != nil {
				t.Fatal(err)
			}
			m.PublicHTTPPort = portA
			st := f.rc.Store.(*store.Store)
			if err := s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if err := st.UpdateClusterState(ctx, f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if replay {
				// Model the durable secret-only checkpoint written after runtime
				// teardown by a predecessor that retained listener assignments.
				s.Phase = "cleanup-secrets"
				s.CleanupSecrets = true
				if err := s.save(f.rc.Cluster); err != nil {
					t.Fatal(err)
				}
				if done, err := f.p.Cleanup(ctx, f.rc, domain.DeleteOptions{}); err != nil || !done {
					t.Fatal("secret-only replay", done, err)
				}
			} else {
				if busy, err := f.p.reconcileExposure(ctx, f.rc, s); err != nil || !busy {
					t.Fatal("disable exposure", busy, err)
				}
			}
			// A has released locally, but its cleared state has not yet been
			// persisted. B must exclude A's stored port only for this allocation.
			clusterB := &domain.Cluster{Name: "other"}
			if err := st.CreateCluster(ctx, clusterB); err != nil {
				t.Fatal(err)
			}
			contextB := *f.rc
			contextB.Cluster = clusterB
			memberB := &member{Name: "other0"}
			stateB := &state{Members: []*member{memberB}}
			portB, err := f.p.allocatePublicPort(ctx, &contextB, stateB, memberB, portA)
			if err != nil || portB != portA+1 {
				t.Fatal("B ignored A's still-stored assignment", portB, err)
			}
			// B remains unpublished and has not saved its assignment.
			if err := st.UpdateClusterState(ctx, f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			contextC := *f.rc
			contextC.Cluster = &domain.Cluster{Name: "third"}
			memberC := &member{Name: "third0"}
			stateC := &state{Members: []*member{memberC}}
			portC, err := f.p.allocatePublicPort(ctx, &contextC, stateC, memberC, portA)
			if err != nil || portC != portA {
				t.Fatal("stale store resurrected A's released reservation", portC, err)
			}
			next, err := f.p.allocatePublicPort(ctx, &contextC, stateC, memberC, portA)
			if err != nil || next == portA || next == portB {
				t.Fatal("lost C or unpublished B exclusion", next, err)
			}
		})
	}
}
