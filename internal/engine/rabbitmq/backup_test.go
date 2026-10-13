// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const definitionsFixture = `{"rabbit_version":"4.3.6","users":[{"name":"karakoram","password_hash":"source-secret"},{"name":"admin","password_hash":"admin-secret"},{"name":"monitor","password_hash":"monitor-secret"},{"name":"guest","password_hash":"guest-secret"},{"name":"customer","password_hash":"customer-secret"}],"permissions":[{"user":"karakoram","vhost":"/"},{"user":"customer","vhost":"/"}],"topic_permissions":[{"user":"admin","vhost":"/"},{"user":"customer","vhost":"/"}],"vhosts":[{"name":"/"}],"queues":[{"name":"orders","vhost":"/","durable":true}]}`

type definitionsTestStore struct {
	engine.ClusterStore
	storage           domain.BackupStorage
	backup            *domain.Backup
	restore           *domain.Restore
	backupLookups     []domain.ClusterRef
	storageLookups    []string
	failUpdate        bool
	failRestoreUpdate bool
}

func (s *definitionsTestStore) GetBackupStorage(_ context.Context, ns, name string) (*domain.BackupStorage, error) {
	s.storageLookups = append(s.storageLookups, ns+"/"+name)
	if ns != domain.NormalizedNamespace(s.storage.Namespace) || name != s.storage.Name {
		return nil, engine.ErrNotFound
	}
	v := s.storage
	return &v, nil
}
func (s *definitionsTestStore) GetBackup(_ context.Context, ref domain.ClusterRef, name string) (*domain.Backup, error) {
	s.backupLookups = append(s.backupLookups, ref)
	if s.backup == nil || name != s.backup.Name || ref.Name != s.backup.Cluster || ref.Namespace != domain.NormalizedNamespace(s.backup.Namespace) {
		return nil, engine.ErrNotFound
	}
	v := *s.backup
	return &v, nil
}
func (s *definitionsTestStore) UpdateBackup(_ context.Context, b *domain.Backup) error {
	if s.failUpdate {
		s.failUpdate = false
		return errors.New("interrupted metadata write")
	}
	v := *b
	s.backup = &v
	return nil
}
func (s *definitionsTestStore) DeleteBackup(context.Context, domain.ClusterRef, string) error {
	s.backup = nil
	return nil
}
func (s *definitionsTestStore) GetRestore(_ context.Context, _ domain.ClusterRef, name string) (*domain.Restore, error) {
	if s.restore == nil || s.restore.Name != name {
		return nil, engine.ErrNotFound
	}
	v := *s.restore
	return &v, nil
}
func (s *definitionsTestStore) UpdateRestore(_ context.Context, r *domain.Restore) error {
	if s.failRestoreUpdate {
		s.failRestoreUpdate = false
		return errors.New("interrupted restore metadata write")
	}
	v := *r
	s.restore = &v
	return nil
}

type definitionsTestSecrets struct{ engine.SecretStore }

func (definitionsTestSecrets) GetClusterSecret(context.Context, domain.ClusterRef, string) ([]byte, error) {
	return []byte("target-secret"), nil
}

type definitionsTestDialer struct{ address string }

func (d definitionsTestDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

type definitionsTestEvents struct{ events []domain.Event }

func (e *definitionsTestEvents) Record(_ context.Context, event domain.Event) error {
	e.events = append(e.events, event)
	return nil
}

type definitionsFakeS3 struct {
	mu            sync.Mutex
	objects       map[string][]byte
	puts, deletes int
}

func (s *definitionsFakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !strings.Contains(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
		http.Error(w, "unsigned request", 403)
		return
	}
	body, exists := s.objects[r.URL.Path]
	switch r.Method {
	case "PUT":
		value, err := definitionsReadUpload(r)
		if err != nil {
			http.Error(w, "bad upload", 400)
			return
		}
		s.objects[r.URL.Path] = value
		s.puts++
		w.Header().Set("ETag", `"definitions"`)
	case "GET", "HEAD":
		if !exists {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(404)
			if r.Method != "HEAD" {
				_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
			}
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("ETag", `"definitions"`)
		if r.Method == "GET" {
			_, _ = w.Write(body)
		}
	case "DELETE":
		delete(s.objects, r.URL.Path)
		s.deletes++
		if !exists {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(404)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
		} else {
			w.WriteHeader(204)
		}
	default:
		http.Error(w, "unexpected request", 400)
	}
}

// Decode the same AWS streaming framing used by backupstorage's httptest storage.
func definitionsReadUpload(r *http.Request) ([]byte, error) {
	if !strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") && !strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		return io.ReadAll(r.Body)
	}
	reader := bufio.NewReader(r.Body)
	var body bytes.Buffer
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.ParseInt(strings.Split(strings.TrimSpace(line), ";")[0], 16, 64)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			return body.Bytes(), nil
		}
		if _, err = io.CopyN(&body, reader, size); err != nil {
			return nil, err
		}
		if _, err = reader.ReadString('\n'); err != nil {
			return nil, err
		}
	}
}

type definitionsFixtureEnv struct {
	p                *Provider
	rc               *engine.Context
	store            *definitionsTestStore
	s3               *definitionsFakeS3
	events           *definitionsTestEvents
	exports, imports int
	imported         []byte
	exportData       string
	importStatus     int
}

func newDefinitionsFixture(t *testing.T) *definitionsFixtureEnv {
	t.Helper()
	f := &definitionsFixtureEnv{s3: &definitionsFakeS3{objects: map[string][]byte{}}, events: &definitionsTestEvents{}, exportData: definitionsFixture}
	s3Server := httptest.NewServer(f.s3)
	t.Cleanup(s3Server.Close)
	original := definitionsS3Client
	definitionsS3Client = func(s domain.BackupStorage, c domain.StorageCredentials) (*minio.Client, func(), error) {
		u, err := url.Parse(s.EndpointURL)
		if err != nil {
			return nil, nil, err
		}
		client, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKey, ""), Secure: false, Region: "us-east-1", BucketLookup: minio.BucketLookupPath, Transport: s3Server.Client().Transport})
		return client, func() {}, err
	}
	t.Cleanup(func() { definitionsS3Client = original })
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "karakoram" || password != "target-secret" {
			http.Error(w, "bad auth", 401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/overview":
			_, _ = io.WriteString(w, `{"rabbitmq_version":"4.3.6"}`)
		case r.URL.Path == "/api/definitions" && r.Method == "GET":
			f.exports++
			_, _ = io.WriteString(w, f.exportData)
		case r.URL.Path == "/api/definitions" && r.Method == "POST":
			f.imports++
			f.imported, _ = io.ReadAll(r.Body)
			if f.importStatus != 0 {
				w.WriteHeader(f.importStatus)
			} else {
				w.WriteHeader(204)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)
	u, _ := url.Parse(api.URL)
	cluster := &domain.Cluster{Name: "target", Namespace: "default", Spec: domain.ClusterSpec{Provider: "rabbitmq", Version: "4.3.6", Topology: domain.Topology{Type: "cluster"}, TLS: domain.TLSConfig{Mode: "disabled"}}}
	state := &internalState{UID: "cluster-uid", Phase: "ready", Members: []*member{{Name: "targetm0", Stage: "ready", TLSMode: "disabled"}}}
	if err := state.save(cluster); err != nil {
		t.Fatal(err)
	}
	f.store = &definitionsTestStore{storage: domain.BackupStorage{Namespace: "default", Name: "store", Type: "s3", EndpointURL: s3Server.URL, Bucket: "test-bucket", Prefix: "nested/prefix", ForcePathStyle: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "storage-secret"}}, backup: &domain.Backup{Namespace: "default", Name: "daily", Cluster: "target", Storage: "store", Type: "definitions", Phase: domain.PhasePending}}
	f.rc = &engine.Context{Cluster: cluster, Store: f.store, Secrets: definitionsTestSecrets{}, Dialer: definitionsTestDialer{address: u.Host}, Events: f.events}
	f.p = New(Options{})
	return f
}
func definitionsOperation(kind, name string) *domain.Operation {
	return &domain.Operation{ID: "operation-id", Payload: json.RawMessage(fmt.Sprintf(`{"%s":%q}`, kind, name))}
}
func advanceDefinitions(t *testing.T, f *definitionsFixtureEnv, op *domain.Operation, fn func(context.Context, *engine.Context, *domain.Operation) (engine.OpResult, error)) engine.OpResult {
	t.Helper()
	out, err := fn(context.Background(), f.rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = out.Checkpoint
	return out
}
func finishDefinitions(t *testing.T, f *definitionsFixtureEnv, op *domain.Operation, fn func(context.Context, *engine.Context, *domain.Operation) (engine.OpResult, error)) {
	t.Helper()
	for range 10 {
		if advanceDefinitions(t, f, op, fn).Done {
			return
		}
	}
	t.Fatal("workflow did not complete")
}
func definitionsGzip(t *testing.T, data string) []byte {
	t.Helper()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := io.WriteString(w, data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func seedDefinitions(t *testing.T, f *definitionsFixtureEnv, data []byte) {
	t.Helper()
	b := f.store.backup
	b.Provider = "rabbitmq"
	b.Location = "nested/prefix/cluster-uid/definitions/daily.json.gz"
	b.Phase = domain.PhaseSucceeded
	f.s3.objects["/test-bucket/"+b.Location] = data
}

func TestDefinitionsBackupSuccess(t *testing.T) {
	f := newDefinitionsFixture(t)
	op := definitionsOperation("backup", "daily")
	finishDefinitions(t, f, op, f.p.backupOp)
	b := f.store.backup
	if b.Phase != domain.PhaseSucceeded || b.Provider != "rabbitmq" || b.Location != "nested/prefix/cluster-uid/definitions/daily.json.gz" || b.SourceVersion != "4.3.6" || b.SourceTopology.Type != "cluster" || b.Size <= 0 || b.StartedAt == nil || b.CompletedAt == nil {
		t.Fatalf("backup metadata: %+v", b)
	}
	reader, err := gzip.NewReader(bytes.NewReader(f.s3.objects["/test-bucket/"+b.Location]))
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	expected, err := targetDefinitions([]byte(definitionsFixture))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, expected) {
		t.Fatal("backup does not contain filtered definitions")
	}
	if f.exports != 1 || f.s3.puts != 1 {
		t.Fatalf("exports=%d uploads=%d", f.exports, f.s3.puts)
	}
	if len(f.events.events) != 2 || f.events.events[1].Reason != "BackupCompleted" {
		t.Fatal(f.events.events)
	}
	for _, event := range f.events.events {
		if strings.Contains(event.Message, "secret") {
			t.Fatal("event leaked definitions")
		}
	}
	if strings.Contains(string(op.Checkpoint), "secret") {
		t.Fatal("checkpoint leaked definitions")
	}
}
func TestDefinitionsBackupInterruptedCheckpoint(t *testing.T) {
	f := newDefinitionsFixture(t)
	op := definitionsOperation("backup", "daily")
	advanceDefinitions(t, f, op, f.p.backupOp)
	uploadCheckpoint := append(json.RawMessage(nil), op.Checkpoint...)
	advanceDefinitions(t, f, op, f.p.backupOp)
	op.Checkpoint = uploadCheckpoint // PUT succeeded but its checkpoint was not committed.
	f.store.failUpdate = true
	out, err := f.p.backupOp(context.Background(), f.rc, op)
	if err == nil || out.Done {
		t.Fatalf("expected interrupted metadata write: %+v %v", out, err)
	}
	finishDefinitions(t, f, op, f.p.backupOp)
	if f.exports != 1 || f.s3.puts != 1 {
		t.Fatalf("retry repeated export/upload: %d/%d", f.exports, f.s3.puts)
	}
}
func TestDefinitionsDeleteMissingObject(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	delete(f.s3.objects, "/test-bucket/"+f.store.backup.Location)
	op := definitionsOperation("backup", "daily")
	finishDefinitions(t, f, op, f.p.deleteBackupOp)
	if f.store.backup != nil || f.s3.deletes != 1 {
		t.Fatalf("backup=%+v deletes=%d", f.store.backup, f.s3.deletes)
	}
	finishDefinitions(t, f, op, f.p.deleteBackupOp)
	if f.s3.deletes != 1 {
		t.Fatal("terminal delete repeated")
	}
}
func TestDefinitionsRetainedDelete(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	backup := *f.store.backup
	f.rc.Cluster = nil // No live/source cluster state is needed.
	if err := f.p.DeleteRetainedBackup(context.Background(), f.store.storage, backup); err != nil {
		t.Fatal(err)
	}
	if err := f.p.DeleteRetainedBackup(context.Background(), f.store.storage, backup); err != nil {
		t.Fatal(err)
	}
	if len(f.s3.objects) != 0 {
		t.Fatal("retained object remains")
	}
	backup.Location = "nested/prefix/cluster-uid/definitions/../daily.json.gz"
	if err := f.p.DeleteRetainedBackup(context.Background(), f.store.storage, backup); err == nil {
		t.Fatal("unsafe location accepted")
	}
}
func TestDefinitionsBackupInvalidExport(t *testing.T) {
	f := newDefinitionsFixture(t)
	f.exportData = "not-json source-secret"
	op := definitionsOperation("backup", "daily")
	advanceDefinitions(t, f, op, f.p.backupOp)
	advanceDefinitions(t, f, op, f.p.backupOp)
	out, err := f.p.backupOp(context.Background(), f.rc, op)
	if err == nil || !out.Done || f.store.backup.Phase != domain.PhaseFailed || f.s3.puts != 0 {
		t.Fatalf("out=%+v err=%v backup=%+v", out, err, f.store.backup)
	}
	if strings.Contains(err.Error(), "source-secret") {
		t.Fatal("invalid export leaked")
	}
}

func TestDefinitionsDeleteUnstartedRecord(t *testing.T) {
	f := newDefinitionsFixture(t)
	op := definitionsOperation("backup", "daily")
	finishDefinitions(t, f, op, f.p.deleteBackupOp)
	if f.store.backup != nil || f.s3.deletes != 0 {
		t.Fatalf("backup=%+v deletes=%d", f.store.backup, f.s3.deletes)
	}
}

func TestDefinitionsRetainedDeleteUsesRecordedLocation(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	f.s3.objects["/test-bucket/nested/prefix/other/definitions/daily.json.gz"] = []byte("unrelated")
	f.store.storage.Prefix = "new/prefix"
	if err := f.p.DeleteRetainedBackup(context.Background(), f.store.storage, *f.store.backup); err != nil {
		t.Fatal(err)
	}
	if len(f.s3.objects) != 1 {
		t.Fatal("deleted objects outside the recorded backup")
	}
	if _, exists := f.s3.objects["/test-bucket/nested/prefix/other/definitions/daily.json.gz"]; !exists {
		t.Fatal("unrelated backup deleted")
	}
}
