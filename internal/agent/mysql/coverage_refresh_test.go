// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type delayedCoverageRepo struct {
	repo                                    myspec.Repository
	count, delay, gets, lists, active, peak atomic.Int64
	failed                                  atomic.Bool
	objects                                 map[string][]byte
	request                                 myspec.PITRCoverageRequest
	tools                                   string
}

func delayedCoverageFixture(t *testing.T, history int) *delayedCoverageRepo {
	t.Helper()
	f := &delayedCoverageRepo{repo: testBackupRepository(), objects: map[string][]byte{}}
	f.count.Store(int64(history))
	prefix := f.repo.Path + "/binlogs/" + recoverySID + "/"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active := f.active.Add(1)
		defer f.active.Add(-1)
		for old := f.peak.Load(); active > old; old = f.peak.Load() {
			if f.peak.CompareAndSwap(old, active) {
				break
			}
		}
		if d := f.delay.Load(); d > 0 {
			timer := time.NewTimer(time.Duration(d))
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
		}
		if f.failed.Load() {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(403)
			fmt.Fprint(w, "<Error><Code>AccessDenied</Code></Error>")
			return
		}
		if r.URL.Query().Get("list-type") == "2" {
			f.lists.Add(1)
			start, _ := strconv.Atoi(r.URL.Query().Get("continuation-token"))
			requested := r.URL.Query().Get("prefix")
			w.Header().Set("Content-Type", "application/xml")
			if strings.Contains(requested, "/base/") {
				var keys []string
				for key := range f.objects {
					if strings.HasPrefix(key, requested) {
						keys = append(keys, key)
					}
				}
				sort.Strings(keys)
				fmt.Fprint(w, "<ListBucketResult><Name>backups</Name><IsTruncated>false</IsTruncated>")
				for _, key := range keys {
					fmt.Fprintf(w, "<Contents><Key>%s</Key><Size>%d</Size><ETag>baseline</ETag><LastModified>2026-11-02T00:00:00Z</LastModified></Contents>", key, len(f.objects[key]))
				}
				fmt.Fprint(w, "</ListBucketResult>")
				return
			}
			total := int(f.count.Load()) * 2
			end := min(start+1000, total)
			fmt.Fprintf(w, "<ListBucketResult><Name>backups</Name><IsTruncated>%t</IsTruncated><NextContinuationToken>%d</NextContinuationToken>", end < total, end)
			for i := start; i < end; i++ {
				key := fmt.Sprintf("%sbinlog.%06d", prefix, i/2+1)
				size := 2097152
				if i%2 == 1 {
					key += ".json"
					size = 7000
				}
				fmt.Fprintf(w, "<Contents><Key>%s</Key><Size>%d</Size><ETag>v%d</ETag><LastModified>2026-11-02T00:00:00Z</LastModified></Contents>", key, size, i)
			}
			fmt.Fprint(w, "</ListBucketResult>")
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/backups/")
		data, baseline := f.objects[key]
		if !baseline {
			if !strings.HasSuffix(key, ".json") {
				w.WriteHeader(404)
				return
			}
			ordinal := strings.TrimSuffix(key, ".json")
			dot := strings.LastIndexByte(ordinal, '.')
			file, err := strconv.Atoi(ordinal[dot+1:])
			if err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			first, last := (file-1)*50+1, file*50
			meta := binlogSidecar{ServerUUID: recoverySID, File: fmt.Sprintf("binlog.%06d", file), Size: 2 << 20, SHA256: strings.Repeat("a", 64), GTIDSet: fmt.Sprintf("%s:%d-%d", recoverySID, first, last)}
			if first > 1 {
				meta.PreviousGTIDSet = fmt.Sprintf("%s:1-%d", recoverySID, first-1)
			}
			for tx := first; tx <= last; tx++ {
				meta.Transactions = append(meta.Transactions, binlogTransaction{GTID: fmt.Sprintf("%s:%d", recoverySID, tx), OriginalCommitTimestamp: int64(1793577600000000 + tx), EndPosition: uint64(200000 + tx)})
			}
			data, err = json.Marshal(meta)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("Last-Modified", "Mon, 2 Nov 2026 00:00:00 GMT")
		w.Header().Set("ETag", "\"fixture\"")
		if r.Method != "HEAD" {
			if !baseline {
				f.gets.Add(1)
			}
			_, _ = w.Write(data)
		}
	}))
	t.Cleanup(server.Close)
	f.repo.S3.Endpoint = server.URL
	metadata := filepath.Join(t.TempDir(), "metadata")
	if err := os.MkdirAll(metadata, 0700); err != nil {
		t.Fatal(err)
	}
	scalar := map[string]string{"backup-my.cnf": "[mysqld]\nserver_uuid=" + recoverySID + "\n", "xtrabackup_checkpoints": "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 10\nlast_lsn = 10\n", "xtrabackup_info": "tool_version = 8.4.0-7\nserver_version = 8.4.11-11\nend_time = 2026-11-02 00:00:00\n", "xtrabackup_binlog_info": "binlog.000001\t4\t" + recoverySID + ":1\n"}
	base := f.repo.Path + "/base/baseline"
	manifest := myspec.BackupObjectManifest{Version: 1, ObjectPrefix: base}
	for name, text := range scalar {
		if err := os.WriteFile(filepath.Join(metadata, name), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		data := append(nativeXBFrame('P', name, []byte(text), 0, nil), nativeXBFrame('E', name, nil, 0, nil)...)
		sum := sha256.Sum256(data)
		key := base + "/" + name + ".00000000000000000000"
		f.objects[key] = data
		manifest.Objects = append(manifest.Objects, myspec.BackupObject{Key: key, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])})
	}
	sort.Slice(manifest.Objects, func(i, j int) bool { return manifest.Objects[i].Key < manifest.Objects[j].Key })
	data, digest, err := manifest.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	f.objects[metadataObject(base, physicalManifestName)] = data
	f.request = myspec.PITRCoverageRequest{Source: f.repo, ObjectPaths: []string{base}, ObjectManifests: []myspec.BackupManifestRef{{ObjectPrefix: base, Digest: digest, Size: int64(len(data))}}}
	f.tools = t.TempDir()
	if err = os.MkdirAll(filepath.Join(f.tools, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\ncat >/dev/null\ncp " + shellQuote(metadata) + "/* \"$3/\"\n"
	if err = os.WriteFile(filepath.Join(f.tools, "bin", "xbstream"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return f
}
func latencyBaseline() myspec.BackupResult {
	return myspec.BackupResult{SourceUUID: recoverySID, BinlogFile: "binlog.000001", BinlogPosition: 4, EndTime: time.Unix(1793577600, 0).UTC()}
}

type refreshTestAgent struct {
	runner  *Runner
	request myspec.PITRCoverageRequest
	server  *httptest.Server
}

func (f *delayedCoverageRepo) agent(t *testing.T, dir string) *refreshTestAgent {
	t.Helper()
	runner := New()
	runner.dir = dir
	runner.current = coreTestSpec()
	runner.current.XtraBackupDir = f.tools
	mux := http.NewServeMux()
	runner.backupRoutes(mux, nil)
	server := httptest.NewServer(mux)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		runner.stopCoverageRefresh(ctx)
		server.Close()
	})
	return &refreshTestAgent{runner: runner, request: f.request, server: server}
}
func (a *refreshTestAgent) poll(ctx context.Context, fresh bool) (myspec.PITRCoverageResponse, error) {
	request := a.request
	request.RequireFresh = fresh
	request.AcceptPending = true
	return a.pollRequest(ctx, request)
}
func (a *refreshTestAgent) pollRequest(ctx context.Context, request myspec.PITRCoverageRequest) (myspec.PITRCoverageResponse, error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", a.server.URL+"/v1/mysql/pitr-coverage", bytes.NewReader(raw))
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	response, err := a.server.Client().Do(req)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
		a.runner.coverageServiceMu.Lock()
		service := a.runner.coverageRefresh
		a.runner.coverageServiceMu.Unlock()
		var workerErr error
		if service != nil {
			service.mu.Lock()
			if entry := service.entries[coverageRequestKey(a.request)]; entry != nil {
				workerErr = entry.err
			}
			service.mu.Unlock()
		}
		return myspec.PITRCoverageResponse{}, fmt.Errorf("coverage status%d %s (worker: %v)", response.StatusCode, body, workerErr)
	}
	var out myspec.PITRCoverageResponse
	err = json.NewDecoder(response.Body).Decode(&out)
	return out, err
}
func waitRefresh(t *testing.T, agent *refreshTestAgent, total int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	polls := 0
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		out, err := agent.poll(ctx, false)
		cancel()
		polls++
		if err != nil {
			t.Fatal(err)
		}
		if !out.Pending && len(out.Windows) == 1 && out.Windows[0].End.UnixMicro() == int64(1793577600000000+total*50) {
			return polls
		}
		if out.Pending && out.Progress == nil {
			t.Fatal("pending omitted progress")
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("coverage did not converge to%d sidecars", total)
	return 0
}

func TestBackgroundCoverageSurvivesPollsRestartFailoverAndAppend(t *testing.T) {
	fixture := delayedCoverageFixture(t, 10000)
	fixture.delay.Store(int64(20 * time.Millisecond))
	dir := t.TempDir()
	first := fixture.agent(t, dir)
	started := time.Now()
	saved := 0
	for saved < 256 {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		before := time.Now()
		out, err := first.poll(ctx, false)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(before) > time.Second {
			t.Fatal("cold poll blocked on S3")
		}
		if !out.Pending {
			t.Fatal("unexpected cold completion")
		}
		if out.Progress != nil && out.Progress.Stage == "sidecars" {
			saved = out.Progress.Processed
		}
		time.Sleep(25 * time.Millisecond)
	}
	stop, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	first.runner.stopCoverageRefresh(stop)
	cancel()
	first.server.Close()
	checkpoint, err := loadCoverageWork(coverageStatePrefix(dataPaths(dir).root, coverageRequestKey(fixture.request)))
	if err != nil || checkpoint == nil || checkpoint.Position < 256 {
		t.Fatalf("restart checkpoint: %+v %v", checkpoint, err)
	}
	saved = checkpoint.Position
	checkpoint = nil
	resumed := fixture.agent(t, dir)
	polls := waitRefresh(t, resumed, 10000, 90*time.Second)
	if got := fixture.gets.Load(); got > 10000+coverageBatchSize {
		t.Fatalf("restart refetched history from zero: GETs=%d checkpoint=%d", got, saved)
	}
	t.Logf("cold10k latency20ms convergence=%s polls=%d restartCheckpoint=%d GETs=%d", time.Since(started), polls, saved, fixture.gets.Load())
	before := listingSnapshot()
	gets, lists := fixture.gets.Load(), fixture.lists.Load()
	slowest := time.Duration(0)
	for poll := range 500 {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		if poll%5 == 0 {
			cancel()
		}
		before := time.Now()
		out, err := resumed.poll(ctx, false)
		elapsed := time.Since(before)
		cancel()
		if poll%5 == 0 {
			if err == nil {
				t.Fatal("cancelled poll succeeded")
			}
			continue
		}
		if err != nil || out.Pending {
			t.Fatalf("steady poll%d %+v %v", poll, out, err)
		}
		slowest = max(slowest, elapsed)
	}
	after := listingSnapshot()
	if slowest >= time.Second || fixture.gets.Load() != gets {
		t.Fatalf("steady poll performed history work: max=%s GETdelta=%d", slowest, fixture.gets.Load()-gets)
	}
	if after.HeapAlloc > before.HeapAlloc+(16<<20) || after.Goroutines > before.Goroutines+12 {
		t.Fatalf("poll resources grew: before=%+v after=%+v", before, after)
	}
	t.Logf("steady500 max=%s sidecarGETdelta=%d LISTdelta=%d heap=%d->%d goroutines=%d->%d", slowest, fixture.gets.Load()-gets, fixture.lists.Load()-lists, before.HeapAlloc, after.HeapAlloc, before.Goroutines, after.Goroutines)
	fixture.count.Store(12000)
	gets = fixture.gets.Load()
	appended := time.Now()
	polls = waitRefresh(t, resumed, 12000, 50*time.Second)
	if got := fixture.gets.Load() - gets; got != 2000 {
		t.Fatalf("append reread old sidecars: GETdelta=%d", got)
	}
	t.Logf("append2000 convergence=%s polls=%d GETdelta=%d", time.Since(appended), polls, fixture.gets.Load()-gets)
	failover := fixture.agent(t, t.TempDir())
	began := time.Now()
	polls = waitRefresh(t, failover, 12000, 90*time.Second)
	t.Logf("new-primary12000 convergence=%s polls=%d", time.Since(began), polls)
	freshCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	admitted := time.Now()
	fresh, err := failover.poll(freshCtx, true)
	cancel()
	if err != nil || !fresh.Fresh || !fresh.VerificationStartedAt.After(admitted) {
		t.Fatalf("admission used older status verification: %+v %v", fresh, err)
	}
	if fixture.peak.Load() > 2*coverageReadConcurrency {
		t.Fatalf("unbounded S3 fanout: %d", fixture.peak.Load())
	}
}

func TestBackgroundCoverageFailureNeverFallsBackToSuccess(t *testing.T) {
	fixture := delayedCoverageFixture(t, 20)
	fixture.delay.Store(int64(time.Millisecond))
	agent := fixture.agent(t, t.TempDir())
	waitRefresh(t, agent, 20, 10*time.Second)
	fixture.failed.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	_, err := agent.poll(ctx, true)
	cancel()
	if err == nil {
		t.Fatal("fresh verification ignored storage failure")
	}
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	out, err := agent.poll(ctx, false)
	cancel()
	if err == nil || len(out.Windows) != 0 {
		t.Fatalf("failed refresh returned older success: %+v %v", out, err)
	}
}
