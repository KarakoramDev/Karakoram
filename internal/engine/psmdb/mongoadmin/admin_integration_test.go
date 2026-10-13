//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mongoadmin

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// TestReplicaReconfiguration runs real mongod instances on an isolated Docker network.
func TestReplicaReconfiguration(t *testing.T) {
	if _, e := exec.LookPath("docker"); e != nil {
		t.Skip("Docker is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	run := func(args ...string) string {
		t.Helper()
		b, e := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if e != nil {
			t.Fatalf("docker %v: %v: %s", args, e, b)
		}
		return strings.TrimSpace(string(b))
	}
	id := fmt.Sprintf("kkadmin%d", time.Now().UnixNano())
	run("network", "create", id)
	t.Cleanup(func() { _ = exec.Command("docker", "network", "rm", id).Run() })
	names := []string{id + "m0", id + "m1", id + "m2"}
	for _, name := range names {
		run("run", "-d", "--name", name, "--network", id, "-p", "127.0.0.1::27017", "-e", "PERCONA_TELEMETRY_DISABLE=1", "percona/percona-server-mongodb:8.0", "--replSet", "rs0", "--bind_ip_all", "--wiredTigerCacheSizeGB", "0.25")
		t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	}
	address := run("port", names[0], "27017/tcp")
	var client *mongo.Client
	for {
		var e error
		client, e = Dial(ctx, DialOptions{Hosts: []string{address}, Direct: true, Timeout: time.Second})
		if e == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(e)
		}
		time.Sleep(250 * time.Millisecond)
	}
	defer client.Disconnect(context.Background())
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetInitiate", Value: bson.D{{Key: "_id", Value: "rs0"}, {Key: "members", Value: bson.A{bson.D{{Key: "_id", Value: 0}, {Key: "host", Value: names[0] + ":27017"}}}}}}}).Err()
	if err != nil {
		t.Fatal(err)
	}
	eventually := func(fn func() error) {
		t.Helper()
		for {
			e := fn()
			if e == nil {
				return
			}
			if ctx.Err() != nil {
				t.Fatal(e)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}
	eventually(func() error {
		s, e := GetStatus(ctx, client)
		if e != nil {
			return e
		}
		if len(s.Members) != 1 || s.Members[0].State != "PRIMARY" {
			return fmt.Errorf("await primary: %+v", s)
		}
		return nil
	})
	for i := 1; i < 3; i++ {
		host := names[i] + ":27017"
		eventually(func() error { return AddNonVoting(ctx, client, i, host) })
		cfg, e := GetConfig(ctx, client)
		if e != nil {
			t.Fatal(e)
		}
		for _, m := range cfg.Members {
			if m.Host == host && (m.Votes != 0 || m.Priority != 0) {
				t.Fatal("new member voted before sync")
			}
		}
		eventually(func() error { return Promote(ctx, client, host) })
	}
	if e := SafeToRestart(ctx, client, names[0]+":27017"); e != nil {
		t.Fatal(e)
	}
	if e := Remove(ctx, client, names[0]+":27017"); e == nil {
		t.Fatal("primary removal accepted")
	}
	eventually(func() error { return Remove(ctx, client, names[2]+":27017") })
	eventually(func() error { return Remove(ctx, client, names[1]+":27017") })
	cfg, e := GetConfig(ctx, client)
	if e != nil || len(cfg.Members) != 1 {
		t.Fatal(cfg, e)
	}
	if e = AddNonVoting(ctx, client, 0, names[0]+":27017"); e != nil {
		t.Fatal("idempotent add failed", e)
	}
	if e = Promote(ctx, client, names[0]+":27017"); e != nil {
		t.Fatal("resume promotion of singleton primary failed", e)
	}
}
