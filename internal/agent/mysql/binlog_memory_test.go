// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type archiveMemoryConnector struct {
	groupTestConnector
	dir  string
	pass int
}

func (c *archiveMemoryConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *archiveMemoryConnector) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.HasPrefix(q, "SELECT @@server_uuid") {
		return &groupTestRows{values: []driver.Value{recoverySID, filepath.Join(c.dir, "binlog")}}, nil
	}
	if q == "SHOW BINARY LOGS" {
		var rows [][]driver.Value
		for i := 1; i <= c.pass+1; i++ {
			rows = append(rows, []driver.Value{fmt.Sprintf("binlog.%06d", i), int64(512)})
		}
		return &nativeStatusRows{rows: rows}, nil
	}
	return nil, fmt.Errorf("unexpected SQL %s", q)
}

func archiveMemorySnapshot(t *testing.T, pass int) runtime.MemStats {
	t.Helper()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	rss := uint64(0)
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile("/proc/self/statm")
		if err != nil {
			t.Fatal(err)
		}
		fields := strings.Fields(string(b))
		pages, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		rss = pages * uint64(os.Getpagesize())
	}
	t.Logf("pass=%d heap_alloc=%d heap_inuse=%d rss=%d goroutines=%d gc=%d", pass, m.HeapAlloc, m.HeapInuse, rss, runtime.NumGoroutine(), m.NumGC)
	return m
}

func TestArchivePassMemoryBounded(t *testing.T) {
	// Run the production archive pass with a fake native database/decoder and a
	// real TLS S3 transport. Never retain uploaded objects in the fixture itself.
	// Idle transport expiry (90s) must not be needed to satisfy the bound.
	s := fakeMysqlbinlog(t, "IFS= read -r state < \"$4\"\n[ \"$state\" = active ] && exit 0\nprintf '%s' "+shellQuote(streamTransaction())+"\n")
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(404)
			fmt.Fprint(w, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		if r.Method == "PUT" {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"")
			return
		}
		t.Errorf("unexpected S3 method %s", r.Method)
		w.WriteHeader(405)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	repo := testBackupRepository()
	repo.S3.Endpoint = srv.URL
	repo.S3.VerifyTLS = false
	s.Role = myspec.RoleData
	s.Backup = &myspec.Backup{Repository: repo, Binlog: myspec.Binlog{Enabled: true}}
	dir := t.TempDir()
	p := paths{root: dir}
	c := &archiveMemoryConnector{groupTestConnector: groupTestConnector{t: t}, dir: dir}
	db := sql.OpenDB(c)
	defer db.Close()
	status := func(context.Context) (myspec.StatusResponse, error) {
		return myspec.StatusResponse{}, fmt.Errorf("unexpected rotation")
	}
	writeLog := func(number int, active bool) {
		t.Helper()
		kind := "closed"
		if active {
			kind = "active"
		}
		data := kind + "\n" + strings.Repeat("x", 511-len(kind))
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("binlog.%06d", number)), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var baseline runtime.MemStats
	for pass := 1; pass <= 1200; pass++ {
		c.pass = pass
		writeLog(pass, false)
		writeLog(pass+1, true)
		if err := archiveNativeBinlogs(t.Context(), s, p, db, status); err != nil {
			t.Fatal(err)
		}
		if pass%200 == 0 {
			m := archiveMemorySnapshot(t, pass)
			if pass == 200 {
				baseline = m
			}
			if pass > 200 && m.HeapAlloc > baseline.HeapAlloc+(8<<20) {
				t.Fatalf("live archive heap grew by %d bytes", m.HeapAlloc-baseline.HeapAlloc)
			}
		}
	}
	indexPath := archiveIndexPath(p, repo)
	idx, err := loadArchiveState(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Files) != 0 || len(idx.Pending) != 1 || idx.Status.LastArchivedFile != "binlog.001200" {
		t.Fatalf("unexpected compact state: %+v", idx)
	}
	for i := 1; i <= 1200; i++ {
		recorded, err := archiveRecorded(indexPath, fmt.Sprintf("%s/binlog.%06d", recoverySID, i))
		if err != nil || !recorded {
			t.Fatalf("receipt %d missing: %v", i, err)
		}
	}
	// Keep all native files and immutable receipts while the writer is idle.
	// Every pass still executes inventory checks, cursor decoding and state writes.
	for pass := 1201; pass <= 2400; pass++ {
		if err := archiveNativeBinlogs(t.Context(), s, p, db, status); err != nil {
			t.Fatal(err)
		}
	}
	final := archiveMemorySnapshot(t, 2400)
	if final.HeapAlloc > baseline.HeapAlloc+(8<<20) {
		t.Fatalf("idle live heap grew by %d bytes", final.HeapAlloc-baseline.HeapAlloc)
	}
}
