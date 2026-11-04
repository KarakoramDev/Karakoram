// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
)

type connectionMongo struct {
	engine.Provider
	engine.DataPlane
	user     domain.DatabaseUser
	password string
	deleted  bool
}

func (p *connectionMongo) CreateDatabaseUser(_ context.Context, _ *engine.Context, u domain.DatabaseUser, pw string) (string, error) {
	p.user = u
	p.password = pw
	return pw, nil
}
func (p *connectionMongo) DeleteDatabaseUser(_ context.Context, _ *engine.Context, db, name string) error {
	p.deleted = db == p.user.Database && name == p.user.Name
	return nil
}

type connectionRabbit struct {
	engine.Provider
	rabbitmq.Access
	user        rabbitmq.UserWrite
	permissions []rabbitmq.Permission
	deleted     bool
}

func (p *connectionRabbit) CreateUser(_ context.Context, _ *engine.Context, u rabbitmq.UserWrite) (string, error) {
	p.user = u
	return "", nil
}
func (p *connectionRabbit) CreatePermission(_ context.Context, _ *engine.Context, v rabbitmq.Permission) error {
	p.permissions = append(p.permissions, v)
	return nil
}
func (p *connectionRabbit) DeleteUser(_ context.Context, _ *engine.Context, name string) error {
	p.deleted = name == p.user.Name
	return nil
}

func TestConnectionTargetIdentityGrants(t *testing.T) {
	cred := connectionCredential{Username: "karakoram_openbao_test", Password: "private-bootstrap-password"}
	mongo := &connectionMongo{}
	target := Target{Provider: mongo, Context: &engine.Context{}}
	c := DatabaseConnection{TargetProvider: "psmdb"}
	if e := createTargetIdentity(t.Context(), target, c, cred); e != nil {
		t.Fatal(e)
	}
	if mongo.user.Database != "admin" || len(mongo.user.Roles) != 1 || mongo.user.Roles[0] != (domain.RoleRef{Role: "userAdminAnyDatabase", DB: "admin"}) || mongo.password != cred.Password {
		t.Fatalf("MongoDB grants: %+v", mongo.user)
	}
	if e := deleteTargetIdentity(t.Context(), target, c, cred); e != nil || !mongo.deleted {
		t.Fatal("MongoDB identity not removed", e)
	}
	rabbit := &connectionRabbit{}
	target.Provider = rabbit
	c.TargetProvider = "rabbitmq"
	c.Roles = []DatabaseRole{{DatabaseRoleWrite: DatabaseRoleWrite{Database: "/"}}, {DatabaseRoleWrite: DatabaseRoleWrite{Database: "orders"}}}
	if e := createTargetIdentity(t.Context(), target, c, cred); e != nil {
		t.Fatal(e)
	}
	if len(rabbit.user.Tags) != 1 || rabbit.user.Tags[0] != "administrator" || len(rabbit.permissions) != 2 {
		t.Fatalf("RabbitMQ grants: %+v", rabbit)
	}
	for _, v := range rabbit.permissions {
		if v.Read != ".*" || v.Write != ".*" || v.Configure != ".*" {
			t.Fatalf("missing administration permission: %+v", v)
		}
	}
	if e := deleteTargetIdentity(t.Context(), target, c, cred); e != nil || !rabbit.deleted {
		t.Fatal("RabbitMQ identity not removed", e)
	}
}

func TestConnectionRabbitSharedLeaseTTL(t *testing.T) {
	_, rc, _, _ := setup(t)
	target, _ := connectionTargetFixture(rc)
	target.Context.Cluster.Spec.Provider = "rabbitmq"
	req := connectionWrite()
	req.Roles = append(req.Roles, DatabaseRoleWrite{Name: "writer", Access: AccessReadWrite, DefaultTTLSeconds: 7200})
	if _, err := validateConnection(rc, target, req); err == nil {
		t.Fatal("accepted differing per-role RabbitMQ TTLs")
	}
	req.Roles[1].DefaultTTLSeconds = 3600
	if _, err := validateConnection(rc, target, req); err != nil {
		t.Fatal(err)
	}
	payload := rolePayload(DatabaseConnection{TargetProvider: "rabbitmq"}, DatabaseRole{DatabaseRoleWrite: req.Roles[0]})
	if _, ok := payload["default_ttl"]; ok {
		t.Fatal("unsupported per-role TTL emitted")
	}
}

func TestConnectionAcceptsCanonicalPSMDBSpec(t *testing.T) {
	_, rc, _, _ := setup(t)
	target, _ := connectionTargetFixture(rc)
	provider := psmdb.New(psmdb.Options{})
	provider.ApplyDefaults(&target.Context.Cluster.Spec)
	target.Context.Cluster.Spec.Provider = provider.Name()
	target.Provider = provider
	if target.Context.Cluster.Spec.Topology.Type != "replicaSet" {
		t.Fatal("fixture no longer uses the canonical PSMDB replica set topology")
	}
	if _, err := validateConnection(rc, target, connectionWrite()); err != nil {
		t.Fatal("rejected real PSMDB replica set spec:", err)
	}
	target.Context.Cluster.Spec.Topology.Type = "sharded"
	if _, err := validateConnection(rc, target, connectionWrite()); err == nil {
		t.Fatal("accepted sharded MongoDB connection")
	}
}
