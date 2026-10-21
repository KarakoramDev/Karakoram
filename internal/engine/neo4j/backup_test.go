// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type dumpStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	deleted []string
}

func (f *dumpStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/dumps/")
	switch {
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		type entry struct {
			Key  string
			Size int
		}
		result := struct {
			XMLName     xml.Name `xml:"ListBucketResult"`
			Name        string
			Prefix      string
			IsTruncated bool
			Contents    []entry
		}{Name: "dumps", Prefix: r.URL.Query().Get("prefix")}
		for k, v := range f.objects {
			if strings.HasPrefix(k, result.Prefix) {
				result.Contents = append(result.Contents, entry{k, len(v)})
			}
		}
		w.Header().Set("Content-Type", "application/xml")
		xml.NewEncoder(w).Encode(result)
	case r.Method == http.MethodGet:
		data, ok := f.objects[key]
		if !ok {
			w.WriteHeader(404)
			fmt.Fprint(w, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		w.Header().Set("ETag", `"test"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Write(data)
	case r.Method == http.MethodDelete:
		delete(f.objects, key)
		f.deleted = append(f.deleted, key)
		w.WriteHeader(204)
	default:
		http.Error(w, "unexpected S3 request", 400)
	}
}
func setupDumpStore(t *testing.T, rc *engine.Context) (domain.BackupStorage, *dumpStore) {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	host := ""
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && n.IP.IsPrivate() && n.IP.To4() != nil {
			host = n.IP.String()
			break
		}
	}
	if host == "" {
		t.Fatal("S3 endpoint policy tests require a non-loopback IPv4 interface")
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		t.Fatal(err)
	}
	f := &dumpStore{objects: map[string][]byte{}}
	server := httptest.NewUnstartedServer(f)
	server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	t.Setenv("KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST", listener.Addr().String())
	storage := domain.BackupStorage{Namespace: rc.Cluster.Ref().Namespace, Name: "dumps", Type: "s3", EndpointURL: server.URL, Region: "us-east-1", Bucket: "dumps", Prefix: "production", ForcePathStyle: true, Credentials: domain.StorageCredentials{AccessKeyID: "test-key", SecretAccessKey: "test-secret"}}
	if err = rc.Store.(*store.Store).CreateBackupStorage(t.Context(), &storage); err != nil {
		t.Fatal(err)
	}
	return storage, f
}
func manifestFor(prefix string) neo4jspec.Manifest {
	m := neo4jspec.Manifest{Format: neo4jspec.ManifestFormat, Neo4jVersion: defaultVersion, Edition: "community", CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	for _, db := range []string{"system", "neo4j"} {
		m.Databases = append(m.Databases, neo4jspec.DumpObject{Database: db, Key: path.Join(prefix, db+".dump"), Size: 5, SHA256: strings.Repeat("ab", 32)})
	}
	return m
}
func installJobAgent(t *testing.T, rc *engine.Context, handler func(http.ResponseWriter, *http.Request)) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/neo4j/readiness" {
			json.NewEncoder(w).Encode(neo4jspec.Readiness{Ready: true, Version: rc.Cluster.Spec.Version})
			return
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	rc.Agents = testAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
}
func runDumpOperation(t *testing.T, p *Provider, rc *engine.Context, op *domain.Operation) {
	t.Helper()
	for range 20 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			return
		}
	}
	t.Fatal("operation did not finish")
}
func newDumpBackup(t *testing.T, rc *engine.Context, storage domain.BackupStorage, name string) *domain.Backup {
	t.Helper()
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Name: name, Cluster: rc.Cluster.Name, Storage: storage.Name, Type: "dump", Provider: "neo4j", Phase: domain.PhasePending, PBMPrefix: backupPrefix(storage, rc.Cluster, s, name)}
	if err = rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	return b
}
func TestBackupReceiptReplay(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	storage, s3 := setupDumpStore(t, rc)
	b := newDumpBackup(t, rc, storage, "snapshot")
	manifest := manifestFor(b.PBMPrefix)
	calls := 0
	var first neo4jspec.BackupRequest
	installJobAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		var q neo4jspec.BackupRequest
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			t.Error(err)
		}
		calls++
		if calls == 1 {
			first = q
			http.Error(w, "password=must-not-leak", http.StatusServiceUnavailable)
			return
		}
		if !reflect.DeepEqual(q, first) {
			t.Error("receipt replay changed request")
		}
		if calls == 2 {
			json.NewEncoder(w).Encode(neo4jspec.JobResponse{State: neo4jspec.JobRunning})
			return
		}
		raw, _ := json.Marshal(manifest)
		s3.mu.Lock()
		s3.objects[path.Join(b.PBMPrefix, "manifest.json")] = raw
		s3.mu.Unlock()
		json.NewEncoder(w).Encode(neo4jspec.JobResponse{State: neo4jspec.JobDone, Manifest: &manifest})
	})
	op := &domain.Operation{ID: "backup-op", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snapshot"}`)}
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	if _, err = p.RunOperation(t.Context(), rc, op); err == nil || strings.Contains(err.Error(), "must-not-leak") {
		t.Fatal(err)
	}
	// Simulate losing the queue checkpoint after a control-plane interruption.
	op.Checkpoint = nil
	p = New(Options{})
	runDumpOperation(t, p, rc, op)
	got, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name)
	if err != nil || got.Phase != domain.PhaseSucceeded || got.Size != 10 || got.SourceVersion != defaultVersion || got.CompletedAt == nil || got.Location != "s3://dumps/"+b.PBMPrefix {
		t.Fatal(got, err)
	}
	if first.Receipt != op.ID || first.Target.EndpointAllowlist == "" || first.Target.AccessKey != "test-key" {
		t.Fatal(first.Receipt, first.Target.Prefix)
	}
}
func TestBackupAgentFailureCleansPrefix(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	storage, s3 := setupDumpStore(t, rc)
	b := newDumpBackup(t, rc, storage, "snapshot")
	s3.objects[b.PBMPrefix+"/system.dump"] = []byte("partial")
	s3.objects[b.PBMPrefix+"-other/keep"] = []byte("keep")
	installJobAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(neo4jspec.JobResponse{State: neo4jspec.JobFailed, Message: "secret"})
	})
	op := &domain.Operation{ID: "failed-backup", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snapshot"}`)}
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	if _, err = p.RunOperation(t.Context(), rc, op); err == nil {
		t.Fatal("failed receipt accepted")
	}
	got, _ := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name)
	if got.Phase != domain.PhaseFailed || got.Error != "Neo4j offline backup failed" {
		t.Fatal(got)
	}
	s3.mu.Lock()
	defer s3.mu.Unlock()
	if len(s3.objects) != 1 || len(s3.deleted) != 1 {
		t.Fatal(s3.objects, s3.deleted)
	}
}
func TestDeleteBackupAndRetainedDeletion(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	storage, s3 := setupDumpStore(t, rc)
	b := newDumpBackup(t, rc, storage, "snapshot")
	s3.objects[b.PBMPrefix+"/system.dump"] = []byte("dump")
	op := &domain.Operation{ID: "delete-backup", Kind: domain.OperationDeleteBackup, Payload: json.RawMessage(`{"backup":"snapshot"}`)}
	runDumpOperation(t, p, rc, op)
	op.Checkpoint = nil
	runDumpOperation(t, p, rc, op)
	p = New(Options{Store: rc.Store})
	s3.mu.Lock()
	s3.objects[b.PBMPrefix+"/manifest.json"] = []byte("manifest")
	s3.mu.Unlock()
	if err := p.DeleteRetainedBackup(t.Context(), domain.BackupStorage{}, *b); err != nil {
		t.Fatal(err)
	}
	if err := p.DeleteRetainedBackup(t.Context(), storage, *b); err != nil {
		t.Fatal(err)
	}
	s3.mu.Lock()
	defer s3.mu.Unlock()
	if len(s3.objects) != 0 {
		t.Fatal(s3.objects)
	}
}
func TestScheduleDowntimeAcceptance(t *testing.T) {
	p := New(Options{})
	c := &domain.Cluster{Name: "demo"}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Backup.Schedules = []domain.BackupSchedule{{Name: "nightly", Enabled: true, Type: "dump", Cron: "0 0 * * *"}}
	if err := p.Validate(t.Context(), nil, c); err == nil {
		t.Fatal("outage acknowledgement not required")
	}
	c.Spec.EngineOptions = json.RawMessage(`{"acceptBackupDowntime":true}`)
	if err := p.Validate(t.Context(), nil, c); err != nil {
		t.Fatal(err)
	}
}
