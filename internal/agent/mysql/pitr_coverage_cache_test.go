// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
)

type coverageCountingTransport struct {
	http.RoundTripper
	gets atomic.Int64
}

func (c *coverageCountingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") != "2" {
		c.gets.Add(1)
	}
	return c.RoundTripper.RoundTrip(r)
}

func TestCoverageCacheChecksInventoryAndBaselineWithoutRetainingSidecars(t *testing.T) {
	objects := map[string][]byte{}
	repo := manifestS3Fixture(t, objects)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	counted := &coverageCountingTransport{RoundTripper: transport}
	c, err := minio.New(strings.TrimPrefix(repo.S3.Endpoint, "http://"), &minio.Options{Region: repo.S3.Region, BucketLookup: minio.BucketLookupPath, Transport: counted})
	if err != nil {
		t.Fatal(err)
	}
	baseline := myspec.BackupResult{SourceUUID: recoverySID, BinlogFile: "binlog.000001", BinlogPosition: 4, GTIDSet: recoverySID + ":1-3", EndTime: time.Unix(1, 0).UTC()}
	put := func(file, previous, gtid string, stamp int64) {
		t.Helper()
		m := binlogSidecar{ServerUUID: recoverySID, File: file, PreviousGTIDSet: previous, GTIDSet: gtid, Transactions: []binlogTransaction{{GTID: gtid, OriginalCommitTimestamp: stamp, EndPosition: 486}}}
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		objects[archiveObject(repo, recoverySID, file)+".json"] = b
	}
	put("binlog.000001", recoverySID+":1-3", recoverySID+":4", 1100000)
	e := New()
	poll := func() myspec.PITRCoverageResponse {
		t.Helper()
		out, err := e.cachedReplayCoverage(t.Context(), c, repo, baseline)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	first := poll()
	if len(first.Windows) != 1 || counted.gets.Load() != 1 {
		t.Fatalf("first coverage=%+v gets=%d", first, counted.gets.Load())
	}
	for range 20 {
		out := poll()
		if len(out.Windows) != 1 || !out.Windows[0].End.Equal(first.Windows[0].End) {
			t.Fatal("unchanged coverage changed")
		}
	}
	if counted.gets.Load() != 1 {
		t.Fatalf("unchanged inventory reread sidecars: gets=%d", counted.gets.Load())
	}
	put("binlog.000002", recoverySID+":1-4", recoverySID+":5", 1200000)
	appended := poll()
	if counted.gets.Load() != 3 || !appended.Windows[0].End.Equal(time.UnixMicro(1200000)) {
		t.Fatalf("append not observed: %+v gets=%d", appended, counted.gets.Load())
	}
	put("binlog.000002", recoverySID+":1-4", recoverySID+":5", 1300000)
	replaced := poll()
	if counted.gets.Load() != 5 || !replaced.Windows[0].End.Equal(time.UnixMicro(1300000)) {
		t.Fatalf("ETag replacement not observed: %+v", replaced)
	}
	delete(objects, archiveObject(repo, recoverySID, "binlog.000002")+".json")
	deleted := poll()
	if counted.gets.Load() != 6 || !deleted.Windows[0].End.Equal(first.Windows[0].End) {
		t.Fatal("deletion reused stale coverage")
	}
	baseline.EndTime = baseline.EndTime.Add(time.Microsecond)
	changedBaseline := poll()
	if counted.gets.Load() != 7 || !changedBaseline.Windows[0].Start.Equal(baseline.EndTime) {
		t.Fatal("baseline change reused another baseline")
	}
	objects[archiveObject(repo, recoverySID, "binlog.000001")+".json"] = []byte("invalid metadata")
	if _, err := e.cachedReplayCoverage(t.Context(), c, repo, baseline); err == nil {
		t.Fatal("changed malformed sidecar reused cached coverage")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := e.cachedReplayCoverage(ctx, c, repo, baseline); err == nil {
		t.Fatal("canceled observation reused cached coverage")
	}
}
