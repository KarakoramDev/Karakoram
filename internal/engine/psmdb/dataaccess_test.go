// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"net"
	"net/url"
	"reflect"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestDataMcpScope(t *testing.T) {
	p := New(Options{})
	c := &domain.Cluster{}
	backend, err := p.DataMcp(c)
	if err != nil || backend.Kind != engine.DataMcpCompanion || backend.Upstream != "mongodb-mcp-server" {
		t.Fatalf("%+v %v", backend, err)
	}
	for _, scope := range []domain.DataMcpScope{
		{Databases: []string{"admin"}}, {Databases: []string{"local"}}, {Databases: []string{"config"}}, {Databases: []string{"bad/name"}},
		{Schemas: []string{"public"}}, {IndexPatterns: []string{"app-*"}}, {Vhosts: []string{"/"}},
	} {
		c.Spec.DataMcp = &domain.DataMcpSpec{Scope: scope}
		if _, err := p.DataMcp(c); !errors.Is(err, engine.ErrInvalid) {
			t.Fatalf("accepted %+v: %v", scope, err)
		}
	}
	c.Spec.DataMcp = &domain.DataMcpSpec{Scope: domain.DataMcpScope{Databases: []string{"app"}}}
	if _, err := p.DataMcp(c); err != nil {
		t.Fatal(err)
	}
}

func TestDataAccessCredentialsAndListing(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, _ := load(rc.Cluster)
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: "readWrite"}
	s.DataAccessUsers = map[string][]domain.RoleRef{}
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		name := engine.DataAccessUser(level)
		s.DataAccessUsers[name], _ = users.DataAccessRoles(level, []string{"app"})
		if err := putSecret(t.Context(), rc, "users/"+name, []byte("p@ss:/word")); err != nil {
			t.Fatal(err)
		}
	}
	for _, topology := range []string{"replicaSet", "sharded"} {
		rc.Cluster.Spec.Topology.Type = topology
		for _, mode := range []string{"disabled", "allowTLS", "preferTLS", "requireTLS"} {
			s.TLSMode = mode
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
				creds, err := p.DataAccessCredentials(t.Context(), rc, level)
				if err != nil {
					t.Fatal(err)
				}
				uri, err := url.Parse(creds.URI)
				if err != nil {
					t.Fatal(err)
				}
				pw, _ := uri.User.Password()
				if creds.Username != engine.DataAccessUser(level) || pw != "p@ss:/word" || creds.Database != "admin" || uri.Query().Get("authSource") != "admin" || len(creds.CAPEM) == 0 {
					t.Fatalf("incorrect credential metadata for %s", level)
				}
				if creds.TLS != (mode == "preferTLS" || mode == "requireTLS") {
					t.Fatalf("TLS mode %s", mode)
				}
				if (uri.Query().Get("readPreference") == "secondaryPreferred") != (level == engine.DataAccessRead) {
					t.Fatal("read preference")
				}
				if topology == "sharded" {
					if !reflect.DeepEqual(creds.Hosts, []string{rc.Cluster.Name + "r:27017"}) || uri.Query().Get("replicaSet") != "" {
						t.Fatal("router credentials")
					}
				} else if len(creds.Hosts) != 3 || uri.Query().Get("replicaSet") != "rs0" {
					t.Fatal("replica-set credentials")
				}
			}
		}
	}
	listed, err := p.SystemUsers(t.Context(), rc)
	if err != nil || len(listed) != 7 {
		t.Fatalf("listing: %d %v", len(listed), err)
	}
	rc.Cluster.Spec.DataMcp.Access = "read"
	if _, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessReadWrite); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
	delete(s.DataAccessUsers, engine.DataAccessReadUser)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessRead); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := p.DataAccessCredentials(t.Context(), rc, "root"); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestDataAccessReconcileScopeDisableAndRotation(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, _ := load(rc.Cluster)
	s.TLSMode = "disabled"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	server := nativeHealingFixture(t, rc, s)
	var mu sync.Mutex
	grants := map[string]bson.Raw{}
	dropped := map[string]int{}
	server.command = func(cmd bson.Raw) bson.M {
		mu.Lock()
		defer mu.Unlock()
		elements, _ := cmd.Elements()
		switch command := elements[0].Key(); command {
		case "usersInfo":
			return bson.M{"ok": 1, "users": bson.A{}}
		case "createUser", "updateUser":
			name := cmd.Lookup(command).StringValue()
			if roles := cmd.Lookup("roles"); roles.Type != 0 {
				grants[name] = append(bson.Raw(nil), roles.Array()...)
			}
			return bson.M{"ok": 1}
		case "dropUser":
			dropped[cmd.Lookup(command).StringValue()]++
			return bson.M{"ok": 1}
		default:
			t.Errorf("unexpected command %s", command)
			return bson.M{"ok": 0}
		}
	}
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: "readWrite", Scope: domain.DataMcpScope{Databases: []string{"app", "other"}}}
	if err := p.reconcileDataAccessUsers(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.DataAccessUsers) != 2 {
		t.Fatal("missing native provisioning markers")
	}
	rc.Cluster.Spec.DataMcp.Scope.Databases = []string{"other"}
	if err := p.reconcileDataAccessUsers(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	for _, name := range []string{engine.DataAccessReadUser, engine.DataAccessReadWriteUser} {
		values, _ := grants[name].Values()
		if len(values) != 1 || values[0].Document().Lookup("db").StringValue() != "other" {
			t.Errorf("scope not replaced for %s", name)
		}
	}
	mu.Unlock()
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	before, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessRead)
	if err != nil {
		t.Fatal(err)
	}
	configVersion := s.ConfigVersion
	op := &domain.Operation{ID: "rotate-data-user", Kind: domain.OperationRotateSystemUser, Payload: []byte(`{"user":"karakoram-mcp-ro"}`)}
	for range 4 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			break
		}
	}
	s, _ = load(rc.Cluster)
	after, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessRead)
	if err != nil {
		t.Fatal(err)
	}
	if after.Password == before.Password || s.ConfigVersion != configVersion || s.LastRotatedAt[engine.DataAccessReadUser] == nil {
		t.Fatal("rotation did not update credentials independently of members")
	}
	rc.Cluster.Spec.DataMcp.Access = "read"
	if err := p.reconcileDataAccessUsers(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if s.DataAccessUsers[engine.DataAccessReadWriteUser] != nil {
		t.Fatal("write marker retained")
	}
	rc.Cluster.Spec.DataMcp.Enabled = false
	if err := p.reconcileDataAccessUsers(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.DataAccessUsers) != 0 {
		t.Fatal("disabled identities retained")
	}
	for _, name := range []string{engine.DataAccessReadUser, engine.DataAccessReadWriteUser} {
		if _, err := secret(t.Context(), rc, "users/"+name); !missing(err) {
			t.Fatal("retained data secret", err)
		}
		mu.Lock()
		count := dropped[name]
		mu.Unlock()
		if count == 0 {
			t.Fatal("native identity not dropped")
		}
	}
}

type dataAccessRouterDialer struct {
	t        *testing.T
	server   *replacementMongo
	expected string
}

func (d dataAccessRouterDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if address != d.expected {
		d.t.Errorf("data identity connection to %q, want router %q", address, d.expected)
	}
	return d.server.DialContext(ctx, network, address)
}

func TestShardedDataAccessUsesRouter(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, _ := load(rc.Cluster)
	s.TLSMode = "disabled"
	rc.Cluster.Spec.Topology.Type = "sharded"
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	server := nativeHealingFixture(t, rc, s)
	server.handshake = bson.M{"ok": 1, "ismaster": true, "isWritablePrimary": true, "msg": "isdbgrid", "minWireVersion": 0, "maxWireVersion": 25, "maxBsonObjectSize": 16777216, "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000, "saslSupportedMechs": bson.A{"SCRAM-SHA-256"}}
	server.command = func(cmd bson.Raw) bson.M {
		elements, _ := cmd.Elements()
		switch elements[0].Key() {
		case "usersInfo":
			return bson.M{"ok": 1, "users": bson.A{}}
		case "createUser", "updateUser", "dropUser":
			return bson.M{"ok": 1}
		default:
			t.Errorf("unexpected router command %s", elements[0].Key())
			return bson.M{"ok": 0}
		}
	}
	rc.Dialer = dataAccessRouterDialer{t: t, server: server, expected: rc.Cluster.Name + "r:27017"}
	if err := p.reconcileDataAccessUsers(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "rotate-sharded-data", Kind: domain.OperationRotateSystemUser, Payload: []byte(`{"user":"karakoram-mcp-ro"}`)}
	done := false
	for range 4 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("sharded credential rotation did not complete")
	}
	s, _ = load(rc.Cluster)
	rc.Cluster.Spec.DataMcp.Enabled = false
	if err := p.reconcileDataAccessUsers(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
}
