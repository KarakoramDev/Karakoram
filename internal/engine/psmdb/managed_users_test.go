// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestReadyReconcilesManagedNativeUsersAndRole(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, _ := load(rc.Cluster)
	s.TLSMode = "disabled"
	rc.Cluster.Spec.TLS.Mode = "disabled"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	server := nativeHealingFixture(t, rc, s)
	var mu sync.Mutex
	repaired := map[string]int{}
	server.command = func(command bson.Raw) bson.M {
		mu.Lock()
		defer mu.Unlock()
		elements, _ := command.Elements()
		name := elements[0].Key()
		switch name {
		case "rolesInfo":
			return bson.M{"ok": 1, "roles": bson.A{}}
		case "createRole":
			repaired["pbmAnyAction"]++
			return bson.M{"ok": 1}
		case "usersInfo":
			selector := command.Lookup("usersInfo").Document()
			account := selector.Lookup("user").StringValue()
			if account == "backup" {
				return bson.M{"ok": 1, "users": bson.A{}}
			}
			return bson.M{"ok": 1, "users": bson.A{bson.M{"user": account, "db": "admin", "roles": bson.A{bson.M{"role": "read", "db": "admin"}}}}}
		case "createUser", "updateUser":
			account := command.Lookup(name).StringValue()
			if !users.IsSystemUser(account) {
				t.Errorf("modified customer account %q", account)
			}
			if command.Lookup("pwd").StringValue() == "" {
				t.Error("drift repair omitted desired credential")
			}
			repaired[account]++
			return bson.M{"ok": 1}
		default:
			t.Errorf("unexpected command %s", name)
			return bson.M{"ok": 0, "errmsg": "unexpected command"}
		}
	}
	rc.Dialer = server
	// Exercise the ready-state hook twice, with unchanged secrets and desired hash.
	for range 2 {
		if err := p.reconcileManagedUsers(t.Context(), rc, s); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, def := range users.SystemUserDefs() {
		if repaired[def.Name] != 2 {
			t.Errorf("%s repairs=%d", def.Name, repaired[def.Name])
		}
	}
	if repaired["pbmAnyAction"] != 2 {
		t.Fatal("PBM role not repaired")
	}
}

func nativeHealingFixture(t *testing.T, rc *engine.Context, s *internalState) *replacementMongo {
	t.Helper()
	credentials := map[string]scram.StoredCredentials{}
	for _, def := range users.SystemUserDefs() {
		pw, err := secret(t.Context(), rc, "users/"+def.Name)
		if err != nil {
			t.Fatal(err)
		}
		client, err := scram.SHA256.NewClient(def.Name, string(pw), "")
		if err != nil {
			t.Fatal(err)
		}
		credentials[def.Name] = client.GetStoredCredentials(scram.KeyFactors{Salt: "managed-users-test", Iters: 4096})
	}
	server := &replacementMongo{t: t, config: mongoadmin.Config{ID: "rs0", Version: 1}}
	for _, m := range s.Members {
		server.config.Members = append(server.config.Members, mongoadmin.ConfigMember{ID: m.Index, Host: m.Name + ":27017", Votes: 1, Priority: 1})
	}
	var err error
	server.auth, err = scram.SHA256.NewServer(func(name string) (scram.StoredCredentials, error) {
		c, ok := credentials[name]
		if !ok {
			return c, fmt.Errorf("unexpected user %q", name)
		}
		return c, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rc.Dialer = server
	return server
}

func TestNativeVolumeRecoveryChecksSurvivingPrimary(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, _ := load(rc.Cluster)
	s.TLSMode = "disabled"
	server := nativeHealingFixture(t, rc, s)
	if err := p.volumeRecoverySafe(t.Context(), rc, s, s.Members[0]); err != nil {
		t.Fatalf("retained primary could not reseed secondary: %v", err)
	}
	if err := p.volumeRecoverySafe(t.Context(), rc, s, s.Members[1]); err == nil {
		t.Fatal("lost member counted as its own retained primary")
	}
	server.mu.Lock()
	server.config.Members = nil
	server.mu.Unlock()
	if err := p.volumeRecoverySafe(t.Context(), rc, s, s.Members[0]); err == nil {
		t.Fatal("desired replicas counted as surviving data copies")
	}
}

func TestStatusRefusesSecondaryOnlyReady(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, _ := load(rc.Cluster)
	s.TLSMode, s.Phase = "disabled", "ready"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	server := nativeHealingFixture(t, rc, s)
	rs := mongoadmin.Status{Set: "rs0"}
	for _, m := range s.Members {
		rs.Members = append(rs.Members, mongoadmin.Member{Name: m.Name + ":27017", State: "SECONDARY", Health: 1})
	}
	server.observed = &rs
	server.command = func(command bson.Raw) bson.M {
		if command.Lookup("getParameter").Type != 0 {
			return bson.M{"ok": 1, "featureCompatibilityVersion": bson.M{"version": "8.0"}}
		}
		t.Error("unexpected status command")
		return bson.M{"ok": 0}
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != domain.PhaseDegraded {
		t.Fatalf("secondary-only phase=%s", status.Phase)
	}
	found := false
	for _, condition := range status.Conditions {
		if condition.Type == "Replication/rs0" && condition.Status == "False" && condition.Reason == "NoWritablePrimary" {
			found = true
		}
		if condition.Type == "Healthy" && condition.Status == "True" {
			t.Fatal("secondary-only cluster healthy")
		}
	}
	if !found {
		t.Fatal("missing actionable primary condition")
	}
}

func TestManagedUserRepairRecoversAfterElection(t *testing.T) {
	p, rc := healingCluster(t, 3)
	failure := mongo.WriteException{WriteConcernError: &mongo.WriteConcernError{
		Code: 11602, Name: "InterruptedDueToReplStateChange", Message: "operation was interrupted",
	}}
	attempts := 0
	p.ensureManagedUsers = func(context.Context, *engine.Context, *internalState) error {
		attempts++
		if attempts == 1 {
			return fmt.Errorf("managed repair: %w", failure)
		}
		return nil
	}
	probe := func(_ context.Context, _ *engine.Context, s *internalState, m *member) (mongoadmin.Status, error) {
		rs := mongoadmin.Status{Set: m.Set}
		for i, n := range s.setMembers(m.Set) {
			state := "SECONDARY"
			if i == 0 {
				state = "PRIMARY"
			}
			rs.Members = append(rs.Members, mongoadmin.Member{Name: n.Name + ":27017", State: state, Health: 1, OptimeDate: time.Now()})
		}
		return rs, nil
	}
	step := func() error {
		_, err := p.sync(t.Context(), rc, func(context.Context, *engine.Context, *internalState, *member) (bool, error) {
			return false, nil
		}, probe)
		return err
	}
	if err := step(); !errors.As(err, &failure) {
		t.Fatalf("repair did not report election interruption: %v", err)
	}
	s, _ := load(rc.Cluster)
	if s.ManagedUsersError == "" {
		t.Fatal("failed repair was not persisted")
	}
	// No desired-secret change, restart, scale, or provider reconstruction.
	if err := step(); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.ManagedUsersError != "" {
		t.Fatalf("successful repair retained durable error: %s", s.ManagedUsersError)
	}
	if attempts != 2 {
		t.Fatalf("repair attempts=%d, want 2", attempts)
	}
	s.TLSMode = "disabled"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	server := nativeHealingFixture(t, rc, s)
	server.command = func(command bson.Raw) bson.M {
		return bson.M{"ok": 1, "featureCompatibilityVersion": bson.M{"version": "8.0"}}
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range status.Conditions {
		if c.Type == "ManagedSystemUsers" && c.Status != "True" {
			t.Fatalf("successful repair still actionable: %+v", c)
		}
	}
}

func TestManagedUserFailureConditions(t *testing.T) {
	for _, tc := range []struct {
		name           string
		err            error
		status, reason string
	}{
		{"election", mongo.WriteException{WriteConcernError: &mongo.WriteConcernError{Code: 11602, Name: "InterruptedDueToReplStateChange"}}, "Unknown", "RepairRetrying"},
		{"not-primary", mongo.CommandError{Code: 10107, Name: "NotWritablePrimary"}, "Unknown", "RepairRetrying"},
		{"network", mongo.CommandError{Code: 89, Name: "NetworkTimeout"}, "Unknown", "RepairRetrying"},
		{"privileges", mongo.CommandError{Code: 13, Name: "Unauthorized"}, "False", "CredentialRepairRequired"},
		{"authentication", mongo.CommandError{Code: 18, Name: "AuthenticationFailed"}, "False", "CredentialRepairRequired"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc := healingCluster(t, 3)
			p.ensureManagedUsers = func(context.Context, *engine.Context, *internalState) error {
				return fmt.Errorf("native repair failed: %w", tc.err)
			}
			// A genuine auth failure remains actionable across repeated repairs.
			for range 2 {
				if _, err := p.Sync(t.Context(), rc); err == nil {
					t.Fatal("failed repair returned success")
				}
			}
			s, _ := load(rc.Cluster)
			s.TLSMode = "disabled"
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			server := nativeHealingFixture(t, rc, s)
			server.command = func(bson.Raw) bson.M {
				return bson.M{"ok": 1, "featureCompatibilityVersion": bson.M{"version": "8.0"}}
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range status.Conditions {
				if c.Type == "ManagedSystemUsers" {
					if c.Status != tc.status || c.Reason != tc.reason {
						t.Fatalf("condition=%+v", c)
					}
					return
				}
			}
			t.Fatal("missing managed-user condition")
		})
	}
}
