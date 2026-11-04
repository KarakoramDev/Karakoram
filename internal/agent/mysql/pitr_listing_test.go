// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

type listingMemory struct {
	HeapAlloc, HeapInuse uint64
	Goroutines           int
}

func listingSnapshot() listingMemory {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return listingMemory{HeapAlloc: stats.HeapAlloc, HeapInuse: stats.HeapInuse, Goroutines: runtime.NumGoroutine()}
}

// The first sidecar read is held while a full listing page is prefetched. Both
// cancellation during that read and a decode error abandon the old channel
// adapter with its one-element output buffer full. Cancelling its context alone
// cannot unblock its subsequent unconditional cancellation-error send.
func replayListingEarlyReturns(t *testing.T, history, calls int, cancelRead bool) (listingMemory, listingMemory) {
	t.Helper()
	repo := testBackupRepository()
	prefix := repo.Path + "/binlogs/" + recoverySID + "/"
	started, respond := make(chan struct{}, 1), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			start := 0
			if value := r.URL.Query().Get("continuation-token"); value != "" {
				start, _ = strconv.Atoi(value)
			}
			end := min(start+1000, history)
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, "<ListBucketResult><Name>backups</Name><IsTruncated>%t</IsTruncated><NextContinuationToken>%d</NextContinuationToken>", end < history, end)
			for i := start; i < end; i++ {
				fmt.Fprintf(w, "<Contents><Key>%sbinlog.%06d.json</Key><Size>7000</Size><ETag>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa</ETag><LastModified>2026-11-02T00:00:00Z</LastModified><StorageClass>STANDARD</StorageClass></Contents>", prefix, i+1)
			}
			fmt.Fprint(w, "</ListBucketResult>")
			return
		}
		select {
		case started <- struct{}{}:
		case <-r.Context().Done():
			return
		}
		select {
		case <-respond:
			fmt.Fprint(w, "{malformed sidecar")
		case <-r.Context().Done():
			return
		}
	}))
	defer server.Close()
	repo.S3.Endpoint = server.URL
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{Region: repo.S3.Region, BucketLookup: minio.BucketLookupPath, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	before := listingSnapshot()
	for range calls {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { _, err := readReplaySidecars(ctx, client, repo); done <- err }()
		select {
		case <-started:
		case err := <-done:
			cancel()
			t.Fatalf("listing returned before the held read: %v", err)
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("listing did not reach first sidecar")
		}
		// Yield while the consumer is blocked in HTTP, allowing the producer to fill
		// its bounded output and stop on the next send before the failure is injected.
		time.Sleep(time.Millisecond)
		if cancelRead {
			cancel()
		} else {
			respond <- struct{}{}
		}
		select {
		case err := <-done:
			if err == nil {
				cancel()
				t.Fatal("injected sidecar failure was ignored")
			}
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("failed sidecar read did not return")
		}
		cancel()
	}
	after := listingSnapshot()
	return before, after
}

func TestReplayListingEarlyReturnsDoNotRetainPages(t *testing.T) {
	for _, history := range []int{923, 10000} {
		for _, cancelRead := range []bool{false, true} {
			t.Run(fmt.Sprintf("history=%d/cancel=%t", history, cancelRead), func(t *testing.T) {
				before, after := replayListingEarlyReturns(t, history, 60, cancelRead)
				t.Logf("calls=60 history=%d cancelled=%t before=%+v after=%+v", history, cancelRead, before, after)
				if after.Goroutines > before.Goroutines+8 || after.HeapAlloc > before.HeapAlloc+(8<<20) {
					t.Fatal("early listing return retained SDK producers/pages")
				}
			})
		}
	}
}
