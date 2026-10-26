// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type stalledMongoDialer struct{}

func (stalledMongoDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func assertObservationBudget(t *testing.T, start time.Time, err error, diagnostic string) {
	t.Helper()
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("read took %s, want <=5s", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), diagnostic) || !strings.Contains(err.Error(), "4s budget") || !strings.Contains(err.Error(), "inspect") {
		t.Errorf("missing wrapped deadline/actionable read diagnostic: %v", err)
	}
}

func TestReplicationObservationReadBudget(t *testing.T) {
	for _, read := range []string{"dial", "ping", "hello", "replSetGetStatus"} {
		t.Run(read, func(t *testing.T) {
			p, rc := healingCluster(t, 3)
			s, _ := load(rc.Cluster)
			s.TLSMode = "disabled"
			server := nativeHealingFixture(t, rc, s)
			release := make(chan struct{})
			defer close(release)
			server.beforeCommand = func(command bson.Raw) {
				elements, _ := command.Elements()
				if elements[0].Key() == read {
					<-release
				}
			}
			if read == "dial" {
				rc.Dialer = stalledMongoDialer{}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
			defer cancel()
			start := time.Now()
			status, err := p.replication(ctx, rc, s, s.Members[0])
			assertObservationBudget(t, start, err, "replication observation")
			if status.HasWritablePrimary() {
				t.Fatal("timed-out observation established primary")
			}
		})
	}
}

func TestManagedUserObservationReadBudget(t *testing.T) {
	for _, read := range []string{"dial", "rolesInfo", "usersInfo", "usersInfo fallback"} {
		t.Run(read, func(t *testing.T) {
			p, rc := healingCluster(t, 3)
			s, _ := load(rc.Cluster)
			s.TLSMode = "disabled"
			server := nativeHealingFixture(t, rc, s)
			release := make(chan struct{})
			defer close(release)
			var mutations atomic.Int32
			server.command = func(command bson.Raw) bson.M {
				if command.Lookup("rolesInfo").Type != 0 {
					if read == "rolesInfo" {
						<-release
					}
					return bson.M{"ok": 1, "roles": bson.A{bson.M{"role": "pbmAnyAction", "db": "admin", "roles": bson.A{}, "privileges": bson.A{bson.M{"resource": bson.M{"anyResource": true}, "actions": bson.A{"anyAction"}}}}}}
				}
				if command.Lookup("usersInfo").Type != 0 {
					if read == "usersInfo fallback" && command.Lookup("showCredentials").Boolean() {
						return bson.M{"ok": 0, "code": 13, "errmsg": "unauthorized"}
					}
					<-release
					return bson.M{"ok": 1, "users": bson.A{}}
				}
				mutations.Add(1)
				return bson.M{"ok": 1}
			}
			if read == "dial" {
				rc.Dialer = stalledMongoDialer{}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
			defer cancel()
			start := time.Now()
			err := p.reconcileManagedUsers(ctx, rc, s)
			diagnostic := "usersInfo"
			if read == "dial" {
				diagnostic = "managed-user connection"
			}
			if read == "rolesInfo" {
				diagnostic = "rolesInfo"
			}
			assertObservationBudget(t, start, err, diagnostic)
			if mutations.Load() != 0 {
				t.Fatal("missing observation caused mutation")
			}
		})
	}
}

func TestManagedUserMutationRetainsAdministrationBudget(t *testing.T) {
	for _, mutation := range []string{"createRole", "updateUser"} {
		t.Run(mutation, func(t *testing.T) {
			p, rc := healingCluster(t, 3)
			s, _ := load(rc.Cluster)
			s.TLSMode = "disabled"
			server := nativeHealingFixture(t, rc, s)
			var writes atomic.Int32
			server.command = func(command bson.Raw) bson.M {
				if command.Lookup("rolesInfo").Type != 0 {
					if mutation == "createRole" {
						return bson.M{"ok": 1, "roles": bson.A{}}
					}
					return bson.M{"ok": 1, "roles": bson.A{bson.M{"role": "pbmAnyAction", "db": "admin", "roles": bson.A{}, "privileges": bson.A{bson.M{"resource": bson.M{"anyResource": true}, "actions": bson.A{"anyAction"}}}}}}
				}
				if command.Lookup("usersInfo").Type != 0 {
					return bson.M{"ok": 1, "users": bson.A{bson.M{"roles": bson.A{}}}}
				}
				if command.Lookup(mutation).Type == 0 {
					t.Error("unexpected mutation")
				}
				writes.Add(1)
				time.Sleep(4200 * time.Millisecond)
				return bson.M{"ok": 0, "code": 13, "errmsg": "mutation denied"}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 9*time.Second)
			defer cancel()
			err := p.reconcileManagedUsers(ctx, rc, s)
			if err == nil || errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "code 13") || writes.Load() != 1 {
				t.Fatalf("mutation shortened or replayed: writes=%d err=%v", writes.Load(), err)
			}
		})
	}
}

func TestRouterStatusObservationReadBudget(t *testing.T) {
	_, rc, f, _ := setup(t)
	release := make(chan struct{})
	defer close(release)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.CloseClientConnections()
	t.Cleanup(server.Close)
	rc.Backend = &routerInventory{fakeInfra: f, instances: []infra.Container{{ID: "instance-a", Hostname: "router-a", Status: infra.ContainerRunning}}}
	rc.Agents = routerInstanceDialer{servers: map[string]*httptest.Server{"router-a": server}, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancel()
	start := time.Now()
	ready, observations, err := routerInstancesHealthy(ctx, rc, &member{Name: "demor", Role: agentapi.RoleRouter}, 7)
	assertObservationBudget(t, start, err, "router status")
	if ready || len(observations) != 0 {
		t.Fatal("missing status established router health")
	}
}
