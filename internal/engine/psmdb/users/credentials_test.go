// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package users

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/xoptions"
)

func fixtureCredential(t *testing.T, user, password, mechanism string) scramCredential {
	t.Helper()
	var client *scram.Client
	var err error
	if mechanism == "SCRAM-SHA-1" {
		digest := md5.Sum([]byte(user + ":mongo:" + password))
		client, err = scram.SHA1.NewClientUnprepped(user, hex.EncodeToString(digest[:]), "")
	} else {
		client, err = scram.SHA256.NewClient(user, password, "")
	}
	if err != nil {
		t.Fatal("fixture preparation failed")
	}
	keys := client.GetStoredCredentials(scram.KeyFactors{Salt: "fixture-salt", Iters: 4096})
	return scramCredential{IterationCount: 4096, Salt: base64.StdEncoding.EncodeToString([]byte("fixture-salt")), StoredKey: base64.StdEncoding.EncodeToString(keys.StoredKey), ServerKey: base64.StdEncoding.EncodeToString(keys.ServerKey)}
}

func mockUsersClient(t *testing.T, responses []bson.D) (*mongo.Client, map[string]int) {
	t.Helper()
	counts := make(map[string]int)
	opts := options.Client().SetMonitor(&event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) { counts[e.CommandName]++ }})
	if err := xoptions.SetInternalClientOptions(opts, "deployment", drivertest.NewMockDeployment(responses...)); err != nil {
		t.Fatal("mock deployment option failed")
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal("mock connection failed")
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client, counts
}

func TestEnsureSystemUsersCredentialReconciliation(t *testing.T) {
	for _, scenario := range []string{"unchanged", "password drift", "missing SHA256", "missing configured SHA1", "roles drift", "missing user", "unauthorized fallback"} {
		t.Run(scenario, func(t *testing.T) {
			role := bson.M{
				"role": "pbmAnyAction", "db": "admin", "roles": bson.A{},
				"privileges": bson.A{bson.M{"resource": bson.M{"anyResource": true}, "actions": bson.A{"anyAction"}}},
			}
			responses := []bson.D{{{Key: "ok", Value: 1}, {Key: "roles", Value: bson.A{role}}}}
			passwords := map[string]string{}
			// Match reconciliation's authenticating-user-last ordering.
			defs := SystemUserDefs()
			defs = append(append(defs[:1:1], defs[2:]...), defs[1])
			for i, def := range defs {
				passwords[def.Name] = "expected password"
				password := "expected password"
				if i == 0 && scenario == "password drift" {
					password = "external password"
				}
				credentials := map[string]scramCredential{
					"SCRAM-SHA-256": fixtureCredential(t, def.Name, password, "SCRAM-SHA-256"),
					"SCRAM-SHA-1":   fixtureCredential(t, def.Name, password, "SCRAM-SHA-1"),
				}
				roles := encodeRoles(def.Roles)
				if i == 0 && scenario == "roles drift" {
					roles = nil
				}
				if i == 0 && scenario == "missing SHA256" {
					delete(credentials, "SCRAM-SHA-256")
				}
				if i == 0 && scenario == "missing configured SHA1" {
					delete(credentials, "SCRAM-SHA-1")
				}
				users := bson.A{bson.D{{Key: "user", Value: def.Name}, {Key: "db", Value: "admin"}, {Key: "roles", Value: roles}, {Key: "credentials", Value: credentials}, {Key: "mechanisms", Value: bson.A{"SCRAM-SHA-256", "SCRAM-SHA-1"}}}}
				if i == 0 && scenario == "missing user" {
					users = bson.A{}
				}
				if scenario == "unauthorized fallback" {
					responses = append(responses, bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 13}, {Key: "errmsg", Value: "not authorized"}})
				}
				responses = append(responses, bson.D{{Key: "ok", Value: 1}, {Key: "users", Value: users}})
				if scenario == "unauthorized fallback" || (i == 0 && scenario != "unchanged") {
					responses = append(responses, bson.D{{Key: "ok", Value: 1}})
				}
			}
			client, counts := mockUsersClient(t, responses)
			if err := EnsureSystemUsers(context.Background(), client, passwords); err != nil {
				t.Fatalf("reconcile failed: %v", err)
			}
			updates, creates := 1, 0
			switch scenario {
			case "unchanged":
				updates = 0
			case "missing user":
				updates, creates = 0, 1
			case "unauthorized fallback":
				updates = 5
			}
			if counts["updateUser"] != updates || counts["createUser"] != creates {
				t.Fatalf("writes: update=%d create=%d", counts["updateUser"], counts["createUser"])
			}
		})
	}
}

func TestStoredCredentialsComparison(t *testing.T) {
	for _, mechanism := range []string{"SCRAM-SHA-256", "SCRAM-SHA-1"} {
		t.Run(mechanism, func(t *testing.T) {
			credential := fixtureCredential(t, "backup", "pencil", mechanism)
			if !credential.matches("backup", "pencil", mechanism) || credential.matches("backup", "changed", mechanism) {
				t.Fatal("password comparison failed")
			}
			credential.ServerKey = base64.StdEncoding.EncodeToString(make([]byte, 32))
			if credential.matches("backup", "pencil", mechanism) {
				t.Fatal("server-key mismatch ignored")
			}
			credential = fixtureCredential(t, "backup", "pencil", mechanism)
			credential.StoredKey = "invalid"
			if credential.matches("backup", "pencil", mechanism) {
				t.Fatal("invalid stored key accepted")
			}
		})
	}
	credential := fixtureCredential(t, "backup", "I\u00ADX", "SCRAM-SHA-256")
	if !credential.matches("backup", "IX", "SCRAM-SHA-256") {
		t.Fatal("SASLprep normalization ignored")
	}
}

func TestCredentialReadErrorsDoNotLeak(t *testing.T) {
	for _, response := range []bson.D{
		{{Key: "ok", Value: 0}, {Key: "code", Value: 999}, {Key: "codeName", Value: "secret-marker"}, {Key: "errmsg", Value: "credentials salt storedKey serverKey secret-marker"}},
		{{Key: "ok", Value: 0}, {Key: "code", Value: 18}, {Key: "errmsg", Value: "secret-marker"}},
		{{Key: "ok", Value: 1}, {Key: "users", Value: bson.A{bson.D{{Key: "credentials", Value: "secret-marker"}}}}},
	} {
		client, counts := mockUsersClient(t, []bson.D{response})
		_, _, err := getSystemUser(context.Background(), client, "backup")
		if err == nil {
			t.Fatal("expected read failure")
		}
		for _, secret := range []string{"secret-marker", "credentials", "salt", "storedKey", "serverKey"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatal("credential information leaked")
			}
		}
		if counts["usersInfo"] != 1 {
			t.Fatal("non-authorization error triggered fallback")
		}
	}
}
