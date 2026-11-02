// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// The repository is generated lazily. Neither the fixture nor its client stores
// historical payloads, so measured retained memory belongs to coverage/runtime.
func coverageHistoryFixture(t *testing.T, initial int) (myspec.Repository, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	var count, gets atomic.Int64
	count.Store(int64(initial))
	repo := testBackupRepository()
	prefix := repo.Path + "/binlogs/" + recoverySID + "/"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			start := 1
			if cursor := r.URL.Query().Get("continuation-token"); cursor != "" {
				start, _ = strconv.Atoi(cursor)
			}
			total := int(count.Load())
			end := min(start+999, total)
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, "<ListBucketResult><Name>backups</Name><IsTruncated>%t</IsTruncated><NextContinuationToken>%d</NextContinuationToken>", end < total, end+1)
			for i := start; i <= end; i++ {
				fmt.Fprintf(w, "<Contents><Key>%sbinlog.%06d.json</Key><Size>6000</Size><ETag>v%d</ETag><LastModified>2026-11-02T00:00:00Z</LastModified></Contents>", prefix, i, i)
			}
			fmt.Fprint(w, "</ListBucketResult>")
			return
		}
		file := strings.TrimSuffix(r.URL.Path, ".json")
		dot := strings.LastIndexByte(file, '.')
		i, err := strconv.Atoi(file[dot+1:])
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		meta := binlogSidecar{ServerUUID: recoverySID, File: fmt.Sprintf("binlog.%06d", i), GTIDSet: fmt.Sprintf("%s:%d-%d", recoverySID, (i-1)*32+1, i*32)}
		if i > 1 {
			meta.PreviousGTIDSet = fmt.Sprintf("%s:1-%d", recoverySID, (i-1)*32)
		}
		for tx := 1; tx <= 32; tx++ {
			n := (i-1)*32 + tx
			meta.Transactions = append(meta.Transactions, binlogTransaction{GTID: fmt.Sprintf("%s:%d", recoverySID, n), OriginalCommitTimestamp: int64(1000000 + n), EndPosition: uint64(4 + n)})
		}
		data, err := json.Marshal(meta)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("Last-Modified", "Mon, 2 Nov 2026 00:00:00 GMT")
		w.Header().Set("ETag", fmt.Sprintf("\"v%d\"", i))
		if r.Method != "HEAD" {
			gets.Add(1)
			_, _ = w.Write(data)
		}
	}))
	t.Cleanup(server.Close)
	repo.S3.Endpoint = server.URL
	return repo, &count, &gets
}

func updateCoveragePeak(peak *atomic.Uint64, value uint64) {
	for old := peak.Load(); value > old; old = peak.Load() {
		if peak.CompareAndSwap(old, value) {
			return
		}
	}
}

func coverageHistoryRSS(t *testing.T) uint64 {
	t.Helper()
	if runtime.GOOS != "linux" {
		return 0
	}
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(b))
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return pages * uint64(os.Getpagesize())
}

func TestCoverageLongHistoryStreamsAndFetchesOnlyAppends(t *testing.T) {
	for _, history := range []int{10000, 50000} {
		t.Run(strconv.Itoa(history), func(t *testing.T) {
			repo, count, gets := coverageHistoryFixture(t, history)
			client, closeClient, err := s3Client(repo)
			if err != nil {
				t.Fatal(err)
			}
			defer closeClient()
			baseline := myspec.BackupResult{SourceUUID: recoverySID, BinlogFile: "binlog.000001", BinlogPosition: 4, EndTime: time.Unix(1, 0).UTC()}
			e := New()
			before := archiveMemorySnapshot(t, 0)
			var peakAlloc, peakInuse, peakRSS atomic.Uint64
			stop, stopped := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(stopped)
				ticker := time.NewTicker(20 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						var m runtime.MemStats
						runtime.ReadMemStats(&m)
						updateCoveragePeak(&peakAlloc, m.HeapAlloc)
						updateCoveragePeak(&peakInuse, m.HeapInuse)
					}
				}
			}()
			defer func() { close(stop); <-stopped }()
			poll := func(total int) {
				t.Helper()
				out, err := e.cachedReplayCoverage(t.Context(), client, repo, baseline)
				if err != nil {
					t.Fatal(err)
				}
				if len(out.Windows) != 1 || out.Windows[0].End.UnixMicro() != int64(1000000+total*32) {
					t.Fatalf("coverage does not reach file%d: %+v", total, out)
				}
			}
			poll(history)
			if gets.Load() != int64(history) {
				t.Fatalf("initial reads=%d expected=%d", gets.Load(), history)
			}
			warm := archiveMemorySnapshot(t, history)
			warmRSS := coverageHistoryRSS(t)
			updateCoveragePeak(&peakRSS, warmRSS)
			if warm.HeapAlloc > before.HeapAlloc+(8<<20) {
				t.Fatalf("historical sidecars retained: live growth%d", warm.HeapAlloc-before.HeapAlloc)
			}
			for pass := 1; pass <= 100; pass++ {
				count.Store(int64(history + pass))
				poll(history + pass)
				if gets.Load() != int64(history+pass) {
					t.Fatalf("append%d reread old sidecars: GETs=%d expected=%d", pass, gets.Load(), history+pass)
				}
				updateCoveragePeak(&peakRSS, coverageHistoryRSS(t))
			}
			final := archiveMemorySnapshot(t, history+100)
			if final.HeapAlloc > warm.HeapAlloc+(8<<20) {
				t.Fatalf("append cache retained growing metadata: live growth%d", final.HeapAlloc-warm.HeapAlloc)
			}
			if peakInuse.Load() > before.HeapInuse+(64<<20) {
				t.Fatalf("streamed history used materialized-history heap: peakInuse=%d before=%d", peakInuse.Load(), before.HeapInuse)
			}
			if peakRSS.Load() > warmRSS+(48<<20) {
				t.Fatalf("append RSS failed to plateau: warm=%d peak=%d", warmRSS, peakRSS.Load())
			}
			t.Logf("history=%d appended=100 peakHeapAlloc=%d peakHeapInuse=%d warmRSS=%d peakAppendRSS=%d finalRSS=%d GETs=%d", history, peakAlloc.Load(), peakInuse.Load(), warmRSS, peakRSS.Load(), coverageHistoryRSS(t), gets.Load())
		})
	}
}
