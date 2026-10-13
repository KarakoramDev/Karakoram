// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package users

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestSystemUserDefinitions(t *testing.T) {
	expected := map[string][]domain.RoleRef{
		"clusterAdmin":   {{Role: "clusterAdmin", DB: "admin"}},
		"userAdmin":      {{Role: "userAdminAnyDatabase", DB: "admin"}},
		"clusterMonitor": {{Role: "clusterMonitor", DB: "admin"}, {Role: "read", DB: "local"}},
		"backup":         {{Role: "backup", DB: "admin"}, {Role: "restore", DB: "admin"}, {Role: "clusterMonitor", DB: "admin"}, {Role: "readWrite", DB: "admin"}, {Role: "pbmAnyAction", DB: "admin"}},
		"databaseAdmin":  {{Role: "readWriteAnyDatabase", DB: "admin"}, {Role: "readAnyDatabase", DB: "admin"}, {Role: "dbAdminAnyDatabase", DB: "admin"}, {Role: "backup", DB: "admin"}, {Role: "restore", DB: "admin"}, {Role: "clusterMonitor", DB: "admin"}},
	}
	defs := SystemUserDefs()
	if len(defs) != len(expected) {
		t.Fatal("wrong system user count")
	}
	for _, def := range defs {
		if !IsSystemUser(def.Name) || !sameRoles(def.Roles, expected[def.Name]) || def.Description == "" {
			t.Fatalf("wrong definition: %+v", def)
		}
	}
	defs[0].Roles[0].Role = "changed"
	if SystemUserDefs()[0].Roles[0].Role == "changed" {
		t.Fatal("definitions share mutable slices")
	}
	if IsSystemUser("app") {
		t.Fatal("application account is reserved")
	}
}

func TestMapping(t *testing.T) {
	roles := []domain.RoleRef{{Role: "readWrite", DB: "app"}, {Role: "read", DB: "local"}}
	if got := decodeRoles(encodeRoles(roles)); !reflect.DeepEqual(got, roles) {
		t.Fatalf("round trip: %+v", got)
	}
	data, err := bson.Marshal(bson.D{{Key: "user", Value: "alice"}, {Key: "db", Value: "app"},
		{Key: "roles", Value: encodeRoles(roles)}, {Key: "credentials", Value: bson.M{"secret": "never returned"}}})
	if err != nil {
		t.Fatal(err)
	}
	var wire wireUser
	if err := bson.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	mapped := wire.domain()
	if mapped.Name != "alice" || mapped.Database != "app" || mapped.Password != "" || !sameRoles(mapped.Roles, roles) {
		t.Fatalf("mapped user: %+v", mapped)
	}
	role := domain.DatabaseRole{Name: "reader", Database: "app", Roles: roles[:1], Privileges: []domain.Privilege{{Resource: json.RawMessage(`{"db":"app","collection":"items"}`), Actions: []string{"find", "insert"}}}}
	// This role's inherited references and privilege resources are local to app.
	encoded, err := validateRole(role)
	if err != nil {
		t.Fatal(err)
	}
	roleWire := wireRole{Name: role.Name, Database: role.Database, Roles: encodeRoles(role.Roles), Privileges: encoded}
	got, err := roleWire.domain()
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != role.Name || got.Database != role.Database || !sameRoles(got.Roles, role.Roles) || !samePrivileges(got.Privileges, role.Privileges) {
		t.Fatalf("mapped role: %+v", got)
	}
	if decodeRoles(nil) == nil || encodeRoles(nil) == nil || wire.domain().Roles == nil {
		t.Fatal("empty arrays serialized as null")
	}
	reordered := []domain.RoleRef{roles[1], roles[0]}
	if !sameRoles(roles, reordered) || sameRoles(roles, roles[:1]) {
		t.Fatal("role comparison depends on order")
	}
	if !samePrivileges(role.Privileges, []domain.Privilege{{Resource: json.RawMessage(`{"collection":"items","db":"app"}`), Actions: []string{"insert", "find"}}}) {
		t.Fatal("privilege comparison depends on order")
	}
}

func TestIdentityValidation(t *testing.T) {
	for _, name := range []string{"", " alice", "alice ", "a\x00b", "a\nb", string([]byte{0xff})} {
		if !errors.Is(validateName(name), engine.ErrInvalid) {
			t.Fatalf("accepted name %q", name)
		}
	}
	for _, db := range []string{"", "a.b", "a/b", "a\\b", "a b", "a$b", "a\"b", "a:b", "a?b", "$external", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {
		if !errors.Is(validateDB(db), engine.ErrInvalid) {
			t.Fatalf("accepted db %q", db)
		}
	}
	for _, db := range []string{"admin", "app-db", "app_db", "应用"} {
		if err := validateDB(db); err != nil {
			t.Fatalf("valid db rejected %s: %v", db, err)
		}
	}
	for _, name := range []string{"alice", "app-user", "应用"} {
		if err := validateName(name); err != nil {
			t.Fatalf("valid name rejected: %v", err)
		}
	}
	for _, name := range []string{"backup", "clusterAdmin", "userAdmin", "clusterMonitor", "databaseAdmin"} {
		if !errors.Is(validateApplicationUser("app", name), engine.ErrInvalid) {
			t.Fatal("system name accepted")
		}
	}
	for _, password := range []string{"", "a\x00b", string([]byte{0xff})} {
		if !errors.Is(validatePassword(password), engine.ErrInvalid) {
			t.Fatal("invalid password accepted")
		}
	}
	if err := validatePassword("a password with punctuation!%"); err != nil {
		t.Fatal(err)
	}
	duplicate := []domain.RoleRef{{Role: "read", DB: "app"}, {Role: "read", DB: "app"}}
	if !errors.Is(validateRoles(duplicate), engine.ErrInvalid) {
		t.Fatal("duplicate roles accepted")
	}
}

func TestPrivilegeValidation(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `{}`, `{"cluster":false}`, `{"anyResource":false}`, `{"cluster":true,"db":"app"}`, `{"db":"app"}`, `{"db":1,"collection":""}`, `{"db":"app","collection":null}`, `{"db":"a.b","collection":""}`, `{"db":"app","collection":"a\u0000b"}`, `{"unexpected":true}`} {
		if _, err := validateResource(json.RawMessage(raw)); !errors.Is(err, engine.ErrInvalid) {
			t.Fatalf("accepted resource %s: %v", raw, err)
		}
	}
	for _, raw := range []string{`{"cluster":true}`, `{"anyResource":true}`, `{"db":"","collection":""}`, `{"db":"app","collection":"items"}`} {
		if _, err := validateResource(json.RawMessage(raw)); err != nil {
			t.Fatalf("rejected resource %s: %v", raw, err)
		}
	}
	base := domain.DatabaseRole{Name: "reader", Database: "app", Privileges: []domain.Privilege{{Resource: json.RawMessage(`{"db":"app","collection":""}`), Actions: []string{"find"}}}}
	if _, err := validateRole(base); err != nil {
		t.Fatal(err)
	}
	cases := []domain.DatabaseRole{
		{Name: "reader", Database: "local"},
		{Name: "reader", Database: "app", Roles: []domain.RoleRef{{Role: "reader", DB: "app"}}},
		{Name: "reader", Database: "app", Roles: []domain.RoleRef{{Role: "read", DB: "other"}}},
		{Name: "reader", Database: "app", Privileges: []domain.Privilege{{Resource: json.RawMessage(`{"cluster":true}`), Actions: []string{"serverStatus"}}}},
		{Name: "reader", Database: "app", Privileges: []domain.Privilege{{Resource: json.RawMessage(`{"db":"app","collection":""}`)}}},
		{Name: "reader", Database: "app", Privileges: []domain.Privilege{{Resource: json.RawMessage(`{"db":"app","collection":""}`), Actions: []string{"find", "find"}}}},
		{Name: "reader", Database: "app", Privileges: []domain.Privilege{{Resource: json.RawMessage(`{"db":"app","collection":""}`), Actions: []string{"not an action"}}}},
	}
	for _, role := range cases {
		if _, err := validateRole(role); !errors.Is(err, engine.ErrInvalid) {
			t.Fatalf("invalid role accepted: %+v, %v", role, err)
		}
	}
}

func TestCommandErrors(t *testing.T) {
	for _, test := range []struct {
		code int32
		name string
		want error
	}{
		{11, "UserNotFound", engine.ErrNotFound}, {31, "RoleNotFound", engine.ErrNotFound}, {11000, "DuplicateKey", engine.ErrConflict}, {51003, "", engine.ErrConflict}, {51002, "", engine.ErrConflict}, {2, "BadValue", engine.ErrInvalid}, {23, "InvalidRoleModification", engine.ErrInvalid}, {999, "UserNotFound", engine.ErrNotFound},
	} {
		err := commandError(mongo.CommandError{Code: test.code, Name: test.name, Message: "secret"})
		if !errors.Is(err, test.want) {
			t.Fatalf("code %d: %v", test.code, err)
		}
		if err.Error() == "secret" {
			t.Fatal("server message leaked")
		}
	}
	if commandError(nil) != nil {
		t.Fatal("nil error changed")
	}
	generic := errors.New("connection failed")
	if commandError(generic) != generic {
		t.Fatal("non-command error changed")
	}
}

func TestInvalidCallsWithoutMongo(t *testing.T) {
	ctx := context.Background()
	for _, err := range []error{
		CreateUser(ctx, nil, domain.DatabaseUser{Name: "backup", Database: "admin"}, "pwd"),
		UpdateUser(ctx, nil, domain.DatabaseUser{Name: "a", Database: "a.b"}, ""),
		DeleteUser(ctx, nil, "admin", "userAdmin"), SetPassword(ctx, nil, "app", "alice", ""),
		EnsurePBMRole(ctx, nil), EnsureSystemUsers(ctx, nil, nil),
		CreateRole(ctx, nil, domain.DatabaseRole{Name: "role", Database: "app", Roles: []domain.RoleRef{{Role: "role", DB: "app"}}}),
	} {
		if !errors.Is(err, engine.ErrInvalid) {
			t.Fatalf("expected invalid: %v", err)
		}
	}
	if _, err := GetUser(ctx, nil, "app", "backup"); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := GetRole(ctx, nil, "a.b", "role"); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := ListUsers(ctx, nil); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := ListRoles(ctx, nil); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal(err)
	}
}
