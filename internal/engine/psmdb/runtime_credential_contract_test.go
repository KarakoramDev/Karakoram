// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestGeneratedApplicationCredentialContract(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.TLSMode = "disabled"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	server := nativeHealingFixture(t, rc, s)
	var mu sync.Mutex
	var nativePassword string
	server.command = func(cmd bson.Raw) bson.M {
		if cmd.Lookup("usersInfo").Type != 0 {
			return bson.M{"ok": 1, "users": bson.A{}}
		}
		if cmd.Lookup("createUser").Type != 0 {
			mu.Lock()
			nativePassword = cmd.Lookup("pwd").StringValue()
			mu.Unlock()
		}
		return bson.M{"ok": 1}
	}
	password, err := p.CreateDatabaseUser(t.Context(), rc, domain.DatabaseUser{Name: "app", Database: "app"}, "")
	if err != nil {
		t.Fatal(err)
	}
	generatedPasswordLimit.Assert(t, "returned application password", password)
	mu.Lock()
	defer mu.Unlock()
	generatedPasswordLimit.Assert(t, "native createUser password", nativePassword)
	if password != nativePassword {
		t.Fatal("returned password differs from native createUser request")
	}
}

func TestGeneratedRotationCredentialContracts(t *testing.T) {
	for _, user := range []string{"clusterAdmin", "userAdmin", "databaseAdmin", "backup", "clusterMonitor", engine.DataAccessReadUser, engine.DataAccessReadWriteUser} {
		t.Run(user, func(t *testing.T) {
			p, rc := healingCluster(t, 3)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.TLSMode = "disabled"
			rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}
			s.DataAccessUsers = map[string][]domain.RoleRef{
				engine.DataAccessReadUser:      {{Role: "read", DB: "app"}},
				engine.DataAccessReadWriteUser: {{Role: "readWrite", DB: "app"}},
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			server := nativeHealingFixture(t, rc, s)
			var mu sync.Mutex
			var nativePassword string
			server.command = func(cmd bson.Raw) bson.M {
				if cmd.Lookup("updateUser").Type != 0 {
					mu.Lock()
					nativePassword = cmd.Lookup("pwd").StringValue()
					mu.Unlock()
				}
				return bson.M{"ok": 1}
			}
			recorded := enginetest.RecordCredentials(rc)
			op := &domain.Operation{ID: "credential-contract", Kind: domain.OperationRotateSystemUser, Payload: []byte(`{"user":"` + user + `"}`)}
			done := false
			for range 8 {
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = result.Checkpoint
				// Sidecar users continue with a rolling reload after the native
				// password has changed and the generated value is committed.
				if (user == "backup" || user == "clusterMonitor") && result.Step == "password-persisted" {
					done = true
					break
				}
				if result.Done {
					done = true
					break
				}
			}
			if !done {
				t.Fatal("rotation did not complete")
			}
			recorded.AssertValues(t, map[string]enginetest.CredentialLimit{
				"rotations/" + op.ID: generatedPasswordLimit,
				"users/" + user:      generatedPasswordLimit,
			}, 0)
			persisted, err := secret(t.Context(), rc, "users/"+user)
			if err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			generatedPasswordLimit.Assert(t, "native updateUser password", nativePassword)
			if nativePassword != string(persisted) || nativePassword != string(recorded.Values["rotations/"+op.ID]) {
				t.Fatal("native, staged and persisted rotation passwords differ")
			}
			if user == "databaseAdmin" {
				out, err := p.Connection(t.Context(), rc)
				if err != nil || len(out.Credentials) != 1 {
					t.Fatal("missing returned databaseAdmin credential", err)
				}
				generatedPasswordLimit.Assert(t, "returned rotated databaseAdmin password", out.Credentials[0].Password)
				if out.Credentials[0].Password != nativePassword {
					t.Fatal("returned databaseAdmin password differs from native rotation")
				}
			}
			if isDataAccessUser(user) {
				level := engine.DataAccessRead
				if user == engine.DataAccessReadWriteUser {
					level = engine.DataAccessReadWrite
				}
				creds, err := p.DataAccessCredentials(t.Context(), rc, level)
				if err != nil {
					t.Fatal(err)
				}
				generatedPasswordLimit.Assert(t, "returned rotated data password", creds.Password)
				if creds.Password != nativePassword {
					t.Fatal("returned rotation password differs from native password")
				}
			}
		})
	}
}

func TestRegeneratedKeyfileCredentialContract(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Secrets.DeleteClusterSecret(t.Context(), rc.Cluster.Ref(), "keyfile"); err != nil {
		t.Fatal(err)
	}
	recorded := enginetest.RecordCredentials(rc)
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	recorded.AssertValues(t, map[string]enginetest.CredentialLimit{"keyfile": generatedKeyfileLimit}, 0)
	cfg, err := p.MemberConfig(t.Context(), rc, s.Members[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	generatedKeyfileLimit.Assert(t, "agent keyfile", cfg.Keyfile)
	if cfg.Keyfile != string(recorded.Values["keyfile"]) {
		t.Fatal("agent keyfile differs from regenerated persisted keyfile")
	}
}
