// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package users

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// TestNativeDataAccessLifecycle uses a native mongod, not Docker. Set
// MONGOD_BINARY to its executable or put mongod on PATH. The server is isolated,
// loopback-only and memory-limited; it is stopped before its data is removed.
func TestNativeDataAccessLifecycle(t *testing.T) {
	root, address := startNativeDataAccessMongo(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	password := "native-mcp-integration-password"
	must(EnsureDataAccessUser(ctx, root, engine.DataAccessRead, []string{"allowed", "formerlyAllowed"}, password))
	must(EnsureDataAccessUser(ctx, root, engine.DataAccessReadWrite, []string{"allowed"}, password))
	ro := integrationClient(t, address, "admin", engine.DataAccessReadUser, password)
	rw := integrationClient(t, address, "admin", engine.DataAccessReadWriteUser, password)
	_, err := rw.Database("allowed").Collection("items").InsertOne(ctx, bson.M{"value": 1})
	must(err)
	var item bson.M
	must(ro.Database("allowed").Collection("items").FindOne(ctx, bson.M{}).Decode(&item))
	unauthorized := func(err error) {
		t.Helper()
		var server mongo.ServerError
		if !errors.As(err, &server) || !server.HasErrorCode(13) {
			t.Fatalf("expected Unauthorized, got %v", err)
		}
	}
	_, err = ro.Database("allowed").Collection("items").InsertOne(ctx, bson.M{"value": 2})
	unauthorized(err)
	unauthorized(ro.Database("outside").RunCommand(ctx, bson.D{{Key: "find", Value: "items"}}).Err())
	unauthorized(rw.Database("outside").RunCommand(ctx, bson.D{{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.M{"value": 1}}}}).Err())
	// Scope replacement must remove a previously allowed database, including from
	// already-open authenticated connections once MongoDB invalidates its cache.
	must(EnsureDataAccessUser(ctx, root, engine.DataAccessRead, []string{"allowed"}, password))
	unauthorized(ro.Database("formerlyAllowed").RunCommand(ctx, bson.D{{Key: "find", Value: "items"}}).Err())
	// The disable path drops both native identities, idempotently.
	must(DropDataAccessUser(ctx, root, engine.DataAccessRead))
	must(DropDataAccessUser(ctx, root, engine.DataAccessReadWrite))
	must(DropDataAccessUser(ctx, root, engine.DataAccessRead))
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		if _, err := getUser(ctx, root, "admin", engine.DataAccessUser(level)); !errors.Is(err, engine.ErrNotFound) {
			t.Fatal("disabled identity retained", err)
		}
		assertAuthentication(t, address, "admin", engine.DataAccessUser(level), password, false)
	}
}

func startNativeDataAccessMongo(t *testing.T) (*mongo.Client, string) {
	t.Helper()
	binary := os.Getenv("MONGOD_BINARY")
	if binary == "" {
		var err error
		binary, err = exec.LookPath("mongod")
		if err != nil {
			t.Fatal("native mongod required: set MONGOD_BINARY or put mongod on PATH")
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	process := exec.Command(binary, "--dbpath", dir, "--bind_ip", "127.0.0.1", "--port", strconv.Itoa(port), "--auth", "--wiredTigerCacheSizeGB", "0.25", "--logpath", filepath.Join(dir, "mongod.log"), "--nounixsocket")
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- process.Wait() }()
	t.Cleanup(func() {
		_ = process.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			_ = process.Process.Kill()
			<-exited
		}
	})
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + address).SetServerSelectionTimeout(500 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	for {
		err = client.Ping(ctx, nil)
		if err == nil {
			break
		}
		select {
		case processErr := <-exited:
			exited <- processErr
			log, _ := os.ReadFile(filepath.Join(dir, "mongod.log"))
			t.Fatalf("mongod exited: %v\n%s", processErr, log)
		case <-ctx.Done():
			t.Fatalf("mongod did not start: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	// localhost exception permits only this initial bootstrap account.
	if err := run(ctx, client, "admin", bson.D{{Key: "createUser", Value: "integration-root"}, {Key: "pwd", Value: "native-root-integration-password"}, {Key: "roles", Value: bson.A{bson.M{"role": "root", "db": "admin"}}}}); err != nil {
		t.Fatal(err)
	}
	return integrationClient(t, address, "admin", "integration-root", "native-root-integration-password"), address
}
