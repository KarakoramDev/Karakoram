// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package users

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// TestAuthenticatedUserAndRoleLifecycle starts a disposable PSMDB 8.0 with
// authentication enabled and uses its image entrypoint to create the first user.
// It deliberately fails, rather than skips, when Docker/image setup is unavailable.
func TestAuthenticatedUserAndRoleLifecycle(t *testing.T) {
	root, address := startAuthenticatedMongo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	wantError := func(err, want error) {
		t.Helper()
		if !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
	}
	passwords := map[string]string{}
	for _, def := range SystemUserDefs() {
		passwords[def.Name] = "initial-" + def.Name + "-integration-only"
	}
	must(EnsurePBMRole(ctx, root))
	must(EnsurePBMRole(ctx, root))
	must(EnsureSystemUsers(ctx, root, passwords))
	// Exercise every operation with the contractual split user, not the root.
	admin := integrationClient(t, address, "admin", "userAdmin", passwords["userAdmin"])
	must(EnsureSystemUsers(ctx, admin, passwords))
	must(EnsureSystemUsers(ctx, admin, nil))
	for _, def := range SystemUserDefs() {
		got, err := getUser(ctx, admin, "admin", def.Name)
		must(err)
		if !sameRoles(got.Roles, def.Roles) {
			t.Fatalf("system roles %s: %+v", def.Name, got.Roles)
		}
		authenticated := integrationClient(t, address, "admin", def.Name, passwords[def.Name])
		must(authenticated.Ping(ctx, readpref.Primary()))
	}
	pbm, err := GetRole(ctx, admin, "admin", "pbmAnyAction")
	must(err)
	if len(pbm.Privileges) != 1 || !samePrivileges(pbm.Privileges, []domain.Privilege{{Resource: json.RawMessage(`{"anyResource":true}`), Actions: []string{"anyAction"}}}) {
		t.Fatalf("PBM privileges: %+v", pbm.Privileges)
	}
	// Drift both the PBM role and a system user's roles, then reconcile.
	must(UpdateRole(ctx, admin, domain.DatabaseRole{Name: "pbmAnyAction", Database: "admin"}))
	must(EnsurePBMRole(ctx, admin))
	must(run(ctx, admin, "admin", bson.D{{Key: "updateUser", Value: "clusterAdmin"}, {Key: "roles", Value: encodeRoles([]domain.RoleRef{{Role: "read", DB: "admin"}})}}))
	must(EnsureSystemUsers(ctx, admin, nil))
	current, err := getUser(ctx, admin, "admin", "clusterAdmin")
	must(err)
	if !sameRoles(current.Roles, SystemUserDefs()[0].Roles) {
		t.Fatal("system role drift not reconciled")
	}
	must(EnsureSystemUsers(ctx, admin, map[string]string{"backup": "rotated-backup-integration-only"}))
	assertAuthentication(t, address, "admin", "backup", "rotated-backup-integration-only", true)
	assertAuthentication(t, address, "admin", "backup", passwords["backup"], false)
	must(SetPassword(ctx, admin, "admin", "clusterMonitor", "rotated-monitor-integration-only"))
	assertAuthentication(t, address, "admin", "clusterMonitor", "rotated-monitor-integration-only", true)

	// These databases intentionally have no collections, proving global role
	// discovery does not rely on listDatabases (which would miss them).
	role := domain.DatabaseRole{Name: "appReader", Database: "emptyapp", Roles: []domain.RoleRef{{Role: "read", DB: "emptyapp"}}, Privileges: []domain.Privilege{{Resource: json.RawMessage(`{"db":"emptyapp","collection":"items"}`), Actions: []string{"insert"}}}}
	must(CreateRole(ctx, admin, role))
	wantError(CreateRole(ctx, admin, role), engine.ErrConflict)
	gotRole, err := GetRole(ctx, admin, role.Database, role.Name)
	must(err)
	if !sameRoles(gotRole.Roles, role.Roles) || !samePrivileges(gotRole.Privileges, role.Privileges) {
		t.Fatalf("role round trip: %+v", gotRole)
	}
	role.Privileges[0].Actions = []string{"find", "insert"}
	must(UpdateRole(ctx, admin, role))
	gotRole, err = GetRole(ctx, admin, role.Database, role.Name)
	must(err)
	if !samePrivileges(gotRole.Privileges, role.Privileges) {
		t.Fatal("role privileges not replaced")
	}
	must(CreateRole(ctx, admin, domain.DatabaseRole{Name: "anotherRole", Database: "secondempty", Privileges: []domain.Privilege{{Resource: json.RawMessage(`{"db":"secondempty","collection":""}`), Actions: []string{"find"}}}}))
	roles, err := ListRoles(ctx, admin)
	must(err)
	seen := map[string]bool{}
	for _, got := range roles {
		seen[got.Database+"/"+got.Name] = true
		if got.Name == "read" || got.Name == "root" {
			t.Fatal("builtin role listed")
		}
	}
	if !seen["emptyapp/appReader"] || !seen["secondempty/anotherRole"] || !seen["admin/pbmAnyAction"] {
		t.Fatalf("missing roles: %+v", seen)
	}
	wantError(CreateRole(ctx, admin, domain.DatabaseRole{Name: "read", Database: "emptyapp"}), engine.ErrConflict)
	_, err = GetRole(ctx, admin, "emptyapp", "read")
	wantError(err, engine.ErrNotFound)
	wantError(UpdateRole(ctx, admin, domain.DatabaseRole{Name: "absent", Database: "emptyapp"}), engine.ErrNotFound)
	wantError(DeleteRole(ctx, admin, "emptyapp", "absent"), engine.ErrNotFound)
	wantError(CreateRole(ctx, admin, domain.DatabaseRole{Name: "badInheritance", Database: "emptyapp", Roles: []domain.RoleRef{{Role: "absent", DB: "emptyapp"}}}), engine.ErrInvalid)
	wantError(CreateRole(ctx, admin, domain.DatabaseRole{Name: "badAction", Database: "admin", Privileges: []domain.Privilege{{Resource: json.RawMessage(`{"cluster":true}`), Actions: []string{"notARealMongoAction"}}}}), engine.ErrInvalid)

	app := domain.DatabaseUser{Name: "alice", Database: "emptyapp", Roles: []domain.RoleRef{{Role: "appReader", DB: "emptyapp"}}}
	must(CreateUser(ctx, admin, app, "alice-initial-integration-only"))
	wantError(CreateUser(ctx, admin, app, "alice-initial-integration-only"), engine.ErrConflict)
	must(CreateUser(ctx, admin, domain.DatabaseUser{Name: "alice", Database: "secondempty", Roles: []domain.RoleRef{{Role: "read", DB: "secondempty"}}}, "second-alice-integration-only"))
	gotUser, err := GetUser(ctx, admin, app.Database, app.Name)
	must(err)
	if gotUser.Name != app.Name || gotUser.Database != app.Database || gotUser.Password != "" || !sameRoles(gotUser.Roles, app.Roles) {
		t.Fatalf("user round trip: %+v", gotUser)
	}
	listed, err := ListUsers(ctx, admin)
	must(err)
	appCount := 0
	for _, user := range listed {
		if IsSystemUser(user.Name) {
			t.Fatal("system user listed")
		}
		if user.Name == "alice" {
			appCount++
		}
	}
	if appCount != 2 {
		t.Fatalf("cross-db users: %+v", listed)
	}
	app.Roles = []domain.RoleRef{{Role: "readWrite", DB: "emptyapp"}}
	must(UpdateUser(ctx, admin, app, ""))
	assertAuthentication(t, address, app.Database, app.Name, "alice-initial-integration-only", true)
	gotUser, err = GetUser(ctx, admin, app.Database, app.Name)
	must(err)
	if !sameRoles(gotUser.Roles, app.Roles) {
		t.Fatal("user roles not updated")
	}
	must(UpdateUser(ctx, admin, app, "alice-updated-integration-only"))
	assertAuthentication(t, address, app.Database, app.Name, "alice-updated-integration-only", true)
	assertAuthentication(t, address, app.Database, app.Name, "alice-initial-integration-only", false)
	must(SetPassword(ctx, admin, app.Database, app.Name, "alice-final-integration-only"))
	assertAuthentication(t, address, app.Database, app.Name, "alice-final-integration-only", true)
	wantError(CreateUser(ctx, admin, domain.DatabaseUser{Name: "missingRole", Database: "emptyapp", Roles: []domain.RoleRef{{Role: "absent", DB: "emptyapp"}}}, "password"), engine.ErrInvalid)
	wantError(UpdateUser(ctx, admin, domain.DatabaseUser{Name: "absent", Database: "emptyapp"}, ""), engine.ErrNotFound)
	wantError(SetPassword(ctx, admin, "emptyapp", "absent", "password"), engine.ErrNotFound)
	wantError(DeleteUser(ctx, admin, "admin", "backup"), engine.ErrInvalid)
	wantError(DeleteUser(ctx, admin, "emptyapp", "absent"), engine.ErrNotFound)
	must(DeleteUser(ctx, admin, app.Database, app.Name))
	_, err = GetUser(ctx, admin, app.Database, app.Name)
	wantError(err, engine.ErrNotFound)
	assertAuthentication(t, address, app.Database, app.Name, "alice-final-integration-only", false)
	must(DeleteUser(ctx, admin, "secondempty", "alice"))
	must(DeleteRole(ctx, admin, role.Database, role.Name))
	_, err = GetRole(ctx, admin, role.Database, role.Name)
	wantError(err, engine.ErrNotFound)
	must(DeleteRole(ctx, admin, "secondempty", "anotherRole"))
}

func startAuthenticatedMongo(t *testing.T) (*mongo.Client, string) {
	t.Helper()
	name := fmt.Sprintf("karakoram-users-integration-%d", time.Now().UnixNano())
	command := exec.Command("docker", "run", "--detach", "--rm", "--name", name, "--publish", "127.0.0.1::27017",
		"--env", "MONGO_INITDB_ROOT_USERNAME=integrationRoot", "--env", "MONGO_INITDB_ROOT_PASSWORD=integration-root-only",
		"percona/percona-server-mongodb:8.0", "--auth", "--bind_ip_all")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("start Docker PSMDB: %v\n%s", err, output)
	}
	t.Cleanup(func() {
		if output, err := exec.Command("docker", "rm", "--force", name).CombinedOutput(); err != nil {
			t.Errorf("remove Docker PSMDB: %v\n%s", err, output)
		}
	})
	output, err := exec.Command("docker", "port", name, "27017/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("read Docker port: %v\n%s", err, output)
	}
	address := strings.TrimSpace(string(output))
	if _, _, err := net.SplitHostPort(address); err != nil {
		t.Fatalf("invalid Docker address: %q", address)
	}
	client := integrationClient(t, address, "admin", "integrationRoot", "integration-root-only")
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		lastErr = client.Ping(ctx, readpref.Primary())
		cancel()
		if lastErr == nil {
			return client, address
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("PSMDB did not become ready with authentication: %v", lastErr)
	return nil, ""
}

func integrationClient(t *testing.T, address, db, user, password string) *mongo.Client {
	t.Helper()
	client, err := mongo.Connect(options.Client().SetHosts([]string{address}).SetDirect(true).
		SetAuth(options.Credential{AuthSource: db, Username: user, Password: password}).SetServerSelectionTimeout(time.Second).SetConnectTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Disconnect(ctx); err != nil {
			t.Error(err)
		}
	})
	return client
}

func assertAuthentication(t *testing.T, address, db, user, password string, want bool) {
	t.Helper()
	client := integrationClient(t, address, db, user, password)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := client.Ping(ctx, readpref.Primary())
	if (err == nil) != want {
		t.Fatalf("authentication for %s@%s got success=%v, want %v", user, db, err == nil, want)
	}
}

func TestPreservedPBMAccountRestoresCurrentSystemPasswords(t *testing.T) {
	root, address := startAuthenticatedMongo(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	snapshot := map[string]string{}
	current := map[string]string{}
	for _, def := range SystemUserDefs() {
		snapshot[def.Name] = "snapshot-" + def.Name + "-integration-only"
		current[def.Name] = "current-" + def.Name + "-integration-only"
	}
	// Logical restore preserves the target PBM account, but rolls back the
	// remaining users to the source snapshot's passwords.
	snapshot["backup"] = current["backup"]
	if err := EnsureSystemUsers(ctx, root, snapshot); err != nil {
		t.Fatal(err)
	}
	pbm := integrationClient(t, address, "admin", "backup", current["backup"])
	if err := EnsureSystemUsers(ctx, pbm, current); err != nil {
		t.Fatal(err)
	}
	for _, def := range SystemUserDefs() {
		assertAuthentication(t, address, "admin", def.Name, current[def.Name], true)
	}
}
