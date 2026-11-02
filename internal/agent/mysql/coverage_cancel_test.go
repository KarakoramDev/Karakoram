// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
)

type blockedCoverageRead struct {
	http.RoundTripper
	started, release chan struct{}
	once             sync.Once
}

func (b *blockedCoverageRead) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") != "2" {
		b.once.Do(func() { close(b.started) })
		select {
		case <-b.release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}
	return b.RoundTripper.RoundTrip(r)
}

type coverageWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *coverageWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestCancelledCoveragePollsDoNotQueueBehindRebuild(t *testing.T) {
	objects := map[string][]byte{}
	repo := manifestS3Fixture(t, objects)
	meta := binlogSidecar{ServerUUID: recoverySID, File: "binlog.000001", GTIDSet: recoverySID + ":1", Transactions: []binlogTransaction{{GTID: recoverySID + ":1", OriginalCommitTimestamp: 1100000, EndPosition: 486}}}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	objects[archiveObject(repo, recoverySID, meta.File)+".json"] = raw
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	blocked := &blockedCoverageRead{RoundTripper: transport, started: make(chan struct{}), release: make(chan struct{})}
	client, err := minio.New(strings.TrimPrefix(repo.S3.Endpoint, "http://"), &minio.Options{Region: repo.S3.Region, BucketLookup: minio.BucketLookupPath, Transport: blocked})
	if err != nil {
		t.Fatal(err)
	}
	baseline := myspec.BackupResult{SourceUUID: recoverySID, BinlogFile: meta.File, BinlogPosition: 4, EndTime: time.Unix(1, 0).UTC()}
	e := New()
	active := make(chan error, 1)
	go func() { _, err := e.cachedReplayCoverage(t.Context(), client, repo, baseline); active <- err }()
	defer func() {
		close(blocked.release)
		select {
		case err := <-active:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("active rebuild did not finish")
		}
	}()
	select {
	case <-blocked.started:
	case <-time.After(2 * time.Second):
		t.Fatal("rebuild did not reach sidecar read")
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	goroutines := runtime.NumGoroutine()
	for range 1000 {
		ctx, cancel := context.WithCancel(t.Context())
		waitCtx := &coverageWaitContext{Context: ctx, waiting: make(chan struct{})}
		done := make(chan error, 1)
		go func() { _, err := e.cachedReplayCoverage(waitCtx, client, repo, baseline); done <- err }()
		select {
		case <-waitCtx.waiting:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("poll did not reach cancellable coverage wait")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled poll error=%v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("canceled poll remained behind active rebuild")
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if after.HeapAlloc > before.HeapAlloc+(8<<20) || runtime.NumGoroutine() > goroutines+4 {
		t.Fatalf("canceled polls retained queue: heapBefore=%d heapAfter=%d goroutinesBefore=%d goroutinesAfter=%d", before.HeapAlloc, after.HeapAlloc, goroutines, runtime.NumGoroutine())
	}
	t.Logf("canceledPolls=1000 heldRebuild=1 heapBefore=%d heapAfter=%d goroutinesBefore=%d goroutinesAfter=%d", before.HeapAlloc, after.HeapAlloc, goroutines, runtime.NumGoroutine())
}
