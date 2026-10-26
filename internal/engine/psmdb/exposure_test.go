// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	infrafake "github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSyncRevokesPublicExposureBeforeNativeHealth(t *testing.T) {
	for _, scenario := range []string{"NoWritablePrimary", "replication lag"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, server := writableReadinessFixture(t)
			f := rc.Backend.(*fakeInfra)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			public := rc.Cluster.Spec.Exposure
			public.Public = true
			rs := mongoadmin.Status{Set: "rs0"}
			for i, m := range s.Members {
				m.PublicPort, m.PublicHash = 27017+i, hash(public)
				state, optime := "SECONDARY", time.Now()
				if scenario == "replication lag" {
					if i == 1 {
						state = "PRIMARY"
					} else {
						optime = optime.Add(-time.Minute)
					}
				}
				rs.Members = append(rs.Members, mongoadmin.Member{Name: m.Name + ":27017", State: state, Health: 1, OptimeDate: optime})
			}
			server.observed = &rs
			if scenario == "NoWritablePrimary" {
				server.hello = bson.M{"ok": 1, "isWritablePrimary": false}
			}
			rc.Cluster.Spec.Exposure.Public = false
			rc.Cluster.Generation = 2
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Status.Phase = domain.PhaseUpdating
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			for range len(s.Members) + 2 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			for _, m := range s.Members {
				if !slices.Contains(f.unexposed, m.Name) {
					t.Errorf("owned route %s retained while public:false and %s", m.Name, scenario)
				}
			}
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range s.Members {
				if m.PublicPort != 0 || m.PublicHash != hash(rc.Cluster.Spec.Exposure) {
					t.Errorf("route removal not checkpointed for %s: port=%d", m.Name, m.PublicPort)
				}
			}
			if len(f.exposed) != 0 || rc.Cluster.Status.Phase == domain.PhaseReady || rc.Cluster.Status.ObservedGeneration != 1 {
				t.Fatalf("revocation published access or acknowledged readiness: routes=%v status=%+v", f.exposed, rc.Cluster.Status)
			}
			wantMessage := "NoWritablePrimary"
			if scenario == "replication lag" {
				wantMessage = "complete replication"
			}
			if !strings.Contains(rc.Cluster.Status.Message, wantMessage) {
				t.Fatalf("native health gate not reached: %+v", rc.Cluster.Status)
			}
		})
	}
}

type failedExposureRemoval struct {
	infra.Backend
}

func (failedExposureRemoval) DeletePublicPorts(context.Context, string) error {
	return errors.New("route removal failed")
}

func TestSyncExposureRevocationPreservesSafetyFences(t *testing.T) {
	for _, fence := range []string{"pending operation", "ownership conflict", "route deletion failure"} {
		t.Run(fence, func(t *testing.T) {
			p, rc, _ := writableReadinessFixture(t)
			f := rc.Backend.(*fakeInfra)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members[0]
			public := rc.Cluster.Spec.Exposure
			public.Public = true
			m.PublicPort, m.PublicHash = 27017, hash(public)
			switch fence {
			case "pending operation":
				s.Pending = &pending{Op: infra.Op{ID: "resources"}, Action: "resources", Member: m.Name}
				f.done = false
			case "ownership conflict":
				f.services[m.Name].ID = "foreign-runtime"
				f.services[m.Name].Ownership = nil
			case "route deletion failure":
				rc.Backend = failedExposureRemoval{rc.Backend}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			_, err = p.Sync(t.Context(), rc)
			if fence == "pending operation" && err != nil || fence != "pending operation" && err == nil {
				t.Fatalf("unexpected safety fence result: %v", err)
			}
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if len(f.unexposed) != 0 || s.Members[0].PublicPort != 27017 || s.Members[0].PublicHash != hash(public) {
				t.Fatalf("revocation bypassed fence or checkpointed failed removal: deleted=%v member=%+v", f.unexposed, s.Members[0])
			}
		})
	}
}

func TestSyncPrivateProvisioningSkipsAbsentRouteRemoval(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Backend = infrafake.New(infrafake.Options{})
	for range 10 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatalf("fresh private provisioning blocked: %v", err)
		}
		s, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if len(s.Members) > 0 && s.Members[0].Stage == "deployed" {
			return
		}
	}
	t.Fatal("fresh private member did not complete import and deployment")
}

type interruptedExposureRemoval struct {
	infra.Backend
	remaining int
}

func (b *interruptedExposureRemoval) DeletePublicPorts(ctx context.Context, name string) error {
	if b.remaining == 0 {
		return errors.New("interrupted route removal")
	}
	b.remaining--
	return b.Backend.DeletePublicPorts(ctx, name)
}

func TestSyncRevocationResumesAfterPartialRemoval(t *testing.T) {
	p, rc, _ := writableReadinessFixture(t)
	f := rc.Backend.(*fakeInfra)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range s.Members {
		m.PublicPort, m.PublicHash = 27017+i, "previous-public-exposure"
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Backend = &interruptedExposureRemoval{Backend: f, remaining: 1}
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("expected interruption after first removal")
	}
	if len(f.unexposed) != 1 {
		t.Fatalf("expected one durable removal before restart, got %v", f.unexposed)
	}
	p = New(Options{Store: rc.Store, InternalCAPEM: "control-plane-ca"})
	rc.Backend = f
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if len(f.unexposed) != len(s.Members) {
		t.Fatalf("restart repeated or lost removals: %v", f.unexposed)
	}
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		if m.PublicPort != 0 || m.PublicHash != hash(rc.Cluster.Spec.Exposure) || !slices.Contains(f.unexposed, m.Name) {
			t.Fatalf("restart did not checkpoint removal for %s", m.Name)
		}
	}
}

func TestSyncShardedRevocationPreservesPublicRouter(t *testing.T) {
	p, rc, f, _ := setup(t)
	rc.Cluster.Spec.Topology = domain.Topology{Type: "sharded", Shards: 1}
	p.ApplyDefaults(&rc.Cluster.Spec)
	rc.Cluster.Spec.Exposure.Public = true
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.UID = "exposure"
	plan(rc.Cluster, s)
	for _, m := range s.Members {
		m.Stage = "active"
		m.Version = rc.Cluster.Spec.Version
		m.Resources = runtimeResources(component(rc.Cluster.Spec, m).Resources)
		m.PublicHash = hash(rc.Cluster.Spec.Exposure)
		if m.Role == agentapi.RoleRouter {
			m.PublicPort = 28000
		}
	}
	for _, set := range s.Sets {
		set.Stage, set.Added = "ready", true
	}
	seedFormedServices(t, p, rc, f, s)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		if m.Role == agentapi.RoleRouter {
			if m.PublicPort != 28000 || slices.Contains(f.unexposed, m.Name) {
				t.Fatalf("public router was revoked: %+v", m)
			}
		} else if !slices.Contains(f.unexposed, m.Name) {
			t.Fatalf("internal route was not revoked: %s", m.Name)
		}
	}
}
