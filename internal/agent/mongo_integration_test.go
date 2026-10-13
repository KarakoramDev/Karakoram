// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package agent

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func TestBootstrapRedirectsToElectedPrimaryAndVerifiesSecondary(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Docker host networking on Linux")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	run := func(args ...string) {
		t.Helper()
		output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %v: %v\n%s", args, err, output)
		}
	}
	run("info")
	id := fmt.Sprintf("kkbootstrap%d", time.Now().UnixNano())
	agents := make([]*nodeAgent, 3)
	req := agentapi.BootstrapRequest{ReplSetName: "rs0", UserAdmin: agentapi.Credentials{Username: "userAdmin", Password: "bootstrap-integration-only", Database: "admin"}}
	// Reserving all ports together avoids duplicate ports within this test.
	listeners := make([]net.Listener, 3)
	for i := range listeners {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = listener
		defer listener.Close()
		port := listener.Addr().(*net.TCPAddr).Port
		agents[i] = &nodeAgent{c: agentapi.MemberConfig{Member: fmt.Sprintf("member%d", i), Port: port, Role: agentapi.RoleReplicaSet, ReplSetName: "rs0", TLS: agentapi.TLSConfig{Mode: "disabled"}}}
		priority := float64(1)
		if i == 0 {
			priority = 0 // The first contacted member cannot become primary.
		}
		req.Members = append(req.Members, agentapi.BootstrapMember{ID: i, Host: fmt.Sprintf("127.0.0.1:%d", port), Priority: &priority})
	}
	for i, listener := range listeners {
		listener.Close()
		name := fmt.Sprintf("%sm%d", id, i)
		port := agents[i].c.Port
		// Shared keyfile contents authorize inter-member replication. Each
		// container owns its own private file and disposable database volume.
		command := fmt.Sprintf("printf '%%s' '%s' > /tmp/replica-key; chmod 600 /tmp/replica-key; exec mongod --replSet rs0 --port %d --bind_ip_all --keyFile /tmp/replica-key --wiredTigerCacheSizeGB 0.25", strings.Repeat("a", 64), port)
		run("run", "-d", "--name", name, "--network", "host", "--user", "0", "--entrypoint", "/bin/sh", "-e", "PERCONA_TELEMETRY_DISABLE=1", "percona/percona-server-mongodb:8.0", "-c", command)
		t.Cleanup(func() {
			if t.Failed() {
				output, _ := exec.Command("docker", "logs", name).CombinedOutput()
				t.Logf("%s: %s", name, output)
			}
			if output, err := exec.Command("docker", "rm", "-fv", name).CombinedOutput(); err != nil {
				t.Errorf("cleanup %s: %v %s", name, err, output)
			}
		})
	}
	// Replica-set initiation checks every proposed member's connectivity.
	for _, a := range agents {
		client, err := localMongo(a.c, nil)
		if err != nil {
			t.Fatal(err)
		}
		for client.Ping(ctx, readpref.Nearest()) != nil {
			if err := pause(ctx); err != nil {
				client.Disconnect(context.Background())
				t.Fatal(err)
			}
		}
		client.Disconnect(context.Background())
	}
	// MongoDB requires the initiating node to be electable. Initialize through
	// member 1 without creating users, then contact the unelectable member 0.
	initiator, err := localMongo(agents[1].c, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer initiator.Disconnect(context.Background())
	for initiator.Ping(ctx, readpref.Nearest()) != nil {
		if err := pause(ctx); err != nil {
			t.Fatal(err)
		}
	}
	members := bson.A{}
	for _, m := range req.Members {
		members = append(members, bson.M{"_id": m.ID, "host": m.Host, "priority": *m.Priority})
	}
	if err := initiator.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetInitiate", Value: bson.M{"_id": req.ReplSetName, "members": members}}}).Err(); err != nil {
		t.Fatal(err)
	}
	secondary, err := localMongo(agents[0].c, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer secondary.Disconnect(context.Background())
	for {
		var hello bson.M
		err := secondary.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
		if err == nil && hello["setName"] == req.ReplSetName {
			break
		}
		if err := pause(ctx); err != nil {
			t.Fatal(err)
		}
	}
	redirect, err := agents[0].bootstrap(ctx, req)
	if err != nil || !redirect.Initialized || redirect.Primary == "" || redirect.UserCreated {
		t.Fatalf("first member did not redirect: %+v %v", redirect, err)
	}
	primary := -1
	for i, m := range req.Members {
		if m.Host == redirect.Primary {
			primary = i
		}
	}
	if primary <= 0 {
		t.Fatalf("unexpected primary %q", redirect.Primary)
	}
	created, err := agents[primary].bootstrap(ctx, req)
	if err != nil || !created.Initialized || !created.UserCreated || created.Primary != "" {
		t.Fatalf("primary bootstrap: %+v %v", created, err)
	}
	// Wait for user replication, then verify the existing credentials locally
	// on the unelectable secondary. No leadership change should be required.
	for {
		verified, err := agents[0].bootstrap(ctx, req)
		if err == nil && verified.Initialized && !verified.UserCreated && verified.Primary == "" {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("secondary resume: %+v %v", verified, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
