// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type stoppedMongos struct{}

func (stoppedMongos) DialContext(context.Context, string, string) (net.Conn, error) {
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
}

func TestDataPlaneReadsUnavailableMongos(t *testing.T) {
	p, rc := healingCluster(t, 3)
	rc.Cluster.Spec.Topology.Type = "sharded"
	s, _ := load(rc.Cluster)
	s.TLSMode = "disabled"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Dialer = stoppedMongos{}
	reads := map[string]func(context.Context) error{
		"balancer":   func(ctx context.Context) error { _, err := p.Balancer(ctx, rc); return err },
		"list users": func(ctx context.Context) error { _, err := p.ListDatabaseUsers(ctx, rc); return err },
		"get user":   func(ctx context.Context) error { _, err := p.GetDatabaseUser(ctx, rc, "app", "app"); return err },
		"list roles": func(ctx context.Context) error { _, err := p.ListDatabaseRoles(ctx, rc); return err },
		"get role":   func(ctx context.Context) error { _, err := p.GetDatabaseRole(ctx, rc, "app", "custom"); return err },
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if err := read(ctx); !errors.Is(err, engine.ErrUnavailable) {
				t.Fatalf("stopped mongos: %v", err)
			}
		})
	}
}

func TestDataPlaneReadCommandAvailability(t *testing.T) {
	for _, code := range []int{0, 91, 18, 59} {
		t.Run(mongo.CommandError{Code: int32(code)}.Error(), func(t *testing.T) {
			p, rc := healingCluster(t, 3)
			rc.Cluster.Spec.Topology.Type = "sharded"
			s, _ := load(rc.Cluster)
			s.TLSMode = "disabled"
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			server := nativeHealingFixture(t, rc, s)
			server.handshake = bson.M{"ok": 1, "ismaster": true, "isWritablePrimary": true, "msg": "isdbgrid", "minWireVersion": 0, "maxWireVersion": 25, "maxBsonObjectSize": 16777216, "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000, "saslSupportedMechs": bson.A{"SCRAM-SHA-256"}}
			server.command = func(command bson.Raw) bson.M {
				if code != 0 {
					return bson.M{"ok": 0, "code": code, "errmsg": "test command failure"}
				}
				if command.Lookup("balancerStatus").Type != 0 {
					return bson.M{"ok": 1, "mode": "full", "inBalancerRound": true}
				}
				if command.Lookup("find").Type != 0 {
					return bson.M{"ok": 1, "cursor": bson.M{"id": int64(0), "ns": "admin.system.roles", "firstBatch": bson.A{}}}
				}
				return bson.M{"ok": 1, "users": bson.A{}, "roles": bson.A{}}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			status, err := p.Balancer(ctx, rc)
			if code == 0 {
				if err != nil || status != (domain.BalancerStatus{Enabled: true, Running: true, Mode: "full"}) {
					t.Fatalf("success changed: %+v %v", status, err)
				}
			} else if err == nil || errors.Is(err, engine.ErrUnavailable) != (code == 91) {
				t.Fatalf("code %d: %v", code, err)
			}
			_, err = p.ListDatabaseUsers(ctx, rc)
			if (code == 0) != (err == nil) || errors.Is(err, engine.ErrUnavailable) != (code == 91) {
				t.Fatalf("users code %d: %v", code, err)
			}
			_, err = p.ListDatabaseRoles(ctx, rc)
			if (code == 0) != (err == nil) || errors.Is(err, engine.ErrUnavailable) != (code == 91) {
				t.Fatalf("roles code %d: %v", code, err)
			}
		})
	}
}
