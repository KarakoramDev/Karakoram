// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func assertWireBudget(t *testing.T, start time.Time, err error, diagnostic, member string) {
	t.Helper()
	if elapsed := time.Since(start); elapsed > engine.ObservationTimeout+time.Second {
		t.Errorf("read took %s, want at most %s", elapsed, engine.ObservationTimeout+time.Second)
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), diagnostic) || !strings.Contains(err.Error(), "4s budget") || !strings.Contains(err.Error(), member) || !strings.Contains(err.Error(), "inspect") {
		t.Errorf("missing member-named deadline diagnostic: %v", err)
	}
}

func TestWireBudgetSyncPrepareDisruption(t *testing.T) {
	p, rc := healingCluster(t, 1)
	s, _ := load(rc.Cluster)
	s.TLSMode = "disabled"
	server := nativeHealingFixture(t, rc, s)
	release := make(chan struct{})
	defer close(release)
	server.beforeCommand = func(command bson.Raw) {
		if command.Lookup("replSetGetStatus").Type != 0 {
			<-release
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancel()
	start := time.Now()
	changed, err := p.prepareDisruption(ctx, rc, s, s.Members[0])
	assertWireBudget(t, start, err, "replication status", s.Members[0].Name)
	if changed {
		t.Fatal("unanswered status allowed disruption")
	}
}

func TestWireBudgetAdministrationStatus(t *testing.T) {
	for name, operation := range map[string]func(context.Context, *Provider, *engine.Context, *internalState) error{
		"promote": func(ctx context.Context, p *Provider, rc *engine.Context, s *internalState) error {
			c, err := p.dial(ctx, rc, s, "rs0", "clusterAdmin")
			if err != nil {
				return err
			}
			defer c.Disconnect(ctx)
			return mongoadmin.Promote(ctx, c, s.Members[0].Name+":27017")
		},
		"remove": func(ctx context.Context, p *Provider, rc *engine.Context, s *internalState) error {
			c, err := p.dial(ctx, rc, s, "rs0", "clusterAdmin")
			if err != nil {
				return err
			}
			defer c.Disconnect(ctx)
			return mongoadmin.Remove(ctx, c, s.Members[0].Name+":27017")
		},
		"restart": func(ctx context.Context, p *Provider, rc *engine.Context, s *internalState) error {
			c, err := p.dial(ctx, rc, s, "rs0", "clusterAdmin")
			if err != nil {
				return err
			}
			defer c.Disconnect(ctx)
			return mongoadmin.SafeToRestart(ctx, c, s.Members[0].Name+":27017")
		},
	} {
		t.Run(name, func(t *testing.T) {
			p, rc := healingCluster(t, 3)
			s, _ := load(rc.Cluster)
			s.TLSMode = "disabled"
			server := nativeHealingFixture(t, rc, s)
			release := make(chan struct{})
			defer close(release)
			server.beforeCommand = func(command bson.Raw) {
				if command.Lookup("replSetGetStatus").Type != 0 {
					<-release
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
			defer cancel()
			start := time.Now()
			err := operation(ctx, p, rc, s)
			assertWireBudget(t, start, err, "replication status", s.Members[0].Name)
		})
	}
}

func TestWireBudgetRestoreHello(t *testing.T) {
	for _, observer := range []string{"absence", "worker"} {
		t.Run(observer, func(t *testing.T) {
			p, rc := healingCluster(t, 3)
			s, _ := load(rc.Cluster)
			s.TLSMode = "disabled"
			server := nativeHealingFixture(t, rc, s)
			id := bson.NewObjectID()
			server.command = func(command bson.Raw) bson.M {
				collection, _ := command.Lookup("find").StringValueOK()
				var doc bson.M
				if collection == "pbmCmd" {
					doc = bson.M{"_id": id, "ts": int64(1792626275)}
				} else if collection == "pbmRestores" {
					doc = bson.M{"status": "running", "last_transition_ts": int64(1792626275)}
				} else if command.Lookup("aggregate").Type != 0 {
					collection, doc = "pbmCmd", bson.M{"n": int64(1)}
				} else {
					t.Error("unexpected restore command")
				}
				return bson.M{"ok": 1, "cursor": bson.M{"id": int64(0), "ns": "admin." + collection, "firstBatch": bson.A{doc}}}
			}
			release := make(chan struct{})
			defer close(release)
			server.beforeCommand = func(command bson.Raw) {
				if command.Lookup("hello").Type != 0 {
					<-release
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
			defer cancel()
			c, err := p.dial(ctx, rc, s, "rs0", "backup")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Disconnect(ctx)
			start := time.Now()
			if observer == "absence" {
				_, absent, readErr := observeRestoreAbsence(ctx, c, "native")
				err = readErr
				if absent {
					t.Error("unanswered hello established absence")
				}
			} else {
				_, unconfirmed, readErr := observeRestoreWorkerFailure(ctx, c, "native", map[string]pbm.RestoreWorkerIdentity{}, func(context.Context, string) (agentapi.ProcessStatus, error) {
					t.Error("unanswered hello reached process observation")
					return agentapi.ProcessStatus{}, nil
				})
				err = readErr
				if unconfirmed {
					t.Error("unanswered hello established worker staleness")
				}
			}
			assertWireBudget(t, start, err, "restore "+observer+" hello", "demom1:27017")
		})
	}
}

func TestWireBudgetBalancer(t *testing.T) {
	p, rc := healingCluster(t, 3)
	rc.Cluster.Spec.Topology.Type = "sharded"
	s, _ := load(rc.Cluster)
	s.TLSMode = "disabled"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	server := nativeHealingFixture(t, rc, s)
	server.handshake = bson.M{"ok": 1, "ismaster": true, "isWritablePrimary": true, "msg": "isdbgrid", "minWireVersion": 0, "maxWireVersion": 25, "maxBsonObjectSize": 16777216, "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000, "saslSupportedMechs": bson.A{"SCRAM-SHA-256"}}
	release := make(chan struct{})
	defer close(release)
	server.command = func(command bson.Raw) bson.M {
		if command.Lookup("balancerStatus").Type != 0 {
			<-release
		} else {
			t.Error("unexpected balancer command")
		}
		return bson.M{"ok": 1}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancel()
	start := time.Now()
	_, err := p.Balancer(ctx, rc)
	assertWireBudget(t, start, err, "balancer status", rc.Cluster.Name+"r")
}
