// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type snapshotTestStore struct {
	engine.ClusterStore
	storage           domain.BackupStorage
	backup            *domain.Backup
	restore           *domain.Restore
	backupLookups     []domain.ClusterRef
	storageLookups    []string
	failUpdate        bool
	failRestoreUpdate bool
}

func (s *snapshotTestStore) GetBackupStorage(_ context.Context, ns, name string) (*domain.BackupStorage, error) {
	s.storageLookups = append(s.storageLookups, ns+"/"+name)
	if ns != domain.NormalizedNamespace(s.storage.Namespace) || name != s.storage.Name {
		return nil, engine.ErrNotFound
	}
	v := s.storage
	return &v, nil
}
func (s *snapshotTestStore) GetBackup(_ context.Context, ref domain.ClusterRef, name string) (*domain.Backup, error) {
	s.backupLookups = append(s.backupLookups, ref)
	if s.backup == nil || name != s.backup.Name || ref.Name != s.backup.Cluster || ref.Namespace != domain.NormalizedNamespace(s.backup.Namespace) {
		return nil, engine.ErrNotFound
	}
	v := *s.backup
	return &v, nil
}
func (s *snapshotTestStore) UpdateBackup(_ context.Context, b *domain.Backup) error {
	if s.failUpdate {
		s.failUpdate = false
		return errors.New("interrupted metadata write")
	}
	v := *b
	s.backup = &v
	return nil
}
func (s *snapshotTestStore) DeleteBackup(context.Context, domain.ClusterRef, string) error {
	s.backup = nil
	return nil
}
func (s *snapshotTestStore) GetRestore(_ context.Context, _ domain.ClusterRef, name string) (*domain.Restore, error) {
	if s.restore == nil || s.restore.Name != name {
		return nil, engine.ErrNotFound
	}
	v := *s.restore
	return &v, nil
}
func (s *snapshotTestStore) UpdateRestore(_ context.Context, r *domain.Restore) error {
	if s.failRestoreUpdate {
		s.failRestoreUpdate = false
		return errors.New("interrupted restore metadata write")
	}
	v := *r
	s.restore = &v
	return nil
}

type snapshotFakeS3 struct {
	mu            sync.Mutex
	objects       map[string][]byte
	puts, deletes int
}

func (s *snapshotFakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !strings.Contains(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
		http.Error(w, "unsigned request", http.StatusForbidden)
		return
	}
	body, exists := s.objects[r.URL.Path]
	switch r.Method {
	case "POST":
		if r.URL.Query().Has("uploads") {
			_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>`+strings.TrimPrefix(r.URL.Path, "/bucket/")+`</Key><UploadId>upload</UploadId></InitiateMultipartUploadResult>`)
			return
		}
		_, _ = io.WriteString(w, `<CompleteMultipartUploadResult><Location>location</Location><Bucket>bucket</Bucket><Key>snapshot</Key><ETag>"snapshot"</ETag></CompleteMultipartUploadResult>`)
	case "PUT":
		value, err := snapshotReadUpload(r)
		if err != nil {
			http.Error(w, "bad upload", 400)
			return
		}
		s.objects[r.URL.Path] = value
		s.puts++
		w.Header().Set("ETag", `"snapshot"`)
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
		w.Header().Set("ETag", `"snapshot"`)
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
func snapshotReadUpload(r *http.Request) ([]byte, error) {
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

type snapshotFixture struct {
	p                *Provider
	rc               *engine.Context
	agents           *fakeAgents
	store            *snapshotTestStore
	s3               *snapshotFakeS3
	exports, imports int
	force            bool
	sourceShares     []string
	restorePolicies  map[string]bool
	loseImportReply  bool
	loseMarkerReply  bool
	policyListStatus int
}

func newSnapshotFixture(t *testing.T) *snapshotFixture {
	t.Helper()
	p, rc, _, agents := setup(t)
	ready(t, p, rc)
	f := &snapshotFixture{p: p, rc: rc, agents: agents, s3: &snapshotFakeS3{objects: map[string][]byte{}}, restorePolicies: map[string]bool{}}
	s3Server := httptest.NewServer(f.s3)
	t.Cleanup(s3Server.Close)
	original := snapshotS3Client
	snapshotS3Client = func(s domain.BackupStorage, c domain.StorageCredentials) (*minio.Client, func(), error) {
		u, err := url.Parse(s.EndpointURL)
		if err != nil {
			return nil, nil, err
		}
		client, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKey, ""), Region: "us-east-1", BucketLookup: minio.BucketLookupPath, Transport: s3Server.Client().Transport})
		return client, func() {}, err
	}
	t.Cleanup(func() { snapshotS3Client = original })
	f.store = &snapshotTestStore{ClusterStore: rc.Store, storage: domain.BackupStorage{Namespace: rc.Cluster.Ref().Namespace, Name: "s3", Bucket: "bucket", Prefix: "prefix", EndpointURL: s3Server.URL, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}, backup: &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: "snap", Storage: "s3", Type: "raft-snapshot", Provider: "openbao"}}
	rc.Store = f.store
	for _, m := range ordered(state(t, rc)) {
		name := m.Name
		cert, err := memberCertificate(t.Context(), rc, "tls/members/"+name+".pem")
		if err != nil {
			t.Fatal(err)
		}
		pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			agents.requestMu.Lock()
			defer agents.requestMu.Unlock()
			if r.URL.Path == "/v1/sys/policies/acl" || strings.HasPrefix(r.URL.Path, "/v1/sys/policies/acl/karakoram-restore-") {
				if r.Header.Get("X-Vault-Token") != agents.management || !agents.tokenValid {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				if r.Method == "LIST" {
					if f.policyListStatus != 0 {
						w.WriteHeader(f.policyListStatus)
						return
					}
					keys := []string{"default", "root", "karakoram-manager"}
					for name := range f.restorePolicies {
						keys = append(keys, name)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"keys": keys}})
					return
				}
				policy := strings.TrimPrefix(r.URL.Path, "/v1/sys/policies/acl/")
				switch r.Method {
				case http.MethodPut:
					f.restorePolicies[policy] = true
					if f.loseMarkerReply {
						f.loseMarkerReply = false
						panic(http.ErrAbortHandler)
					}
				case http.MethodDelete:
					delete(f.restorePolicies, policy)
				default:
					t.Errorf("unexpected marker request %s %s", r.Method, r.URL.Path)
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if r.URL.Path == "/v1/sys/storage/raft/snapshot" || r.URL.Path == "/v1/sys/storage/raft/snapshot-force" {
				if r.Header.Get("X-Vault-Token") != agents.management || !agents.tokenValid {
					w.WriteHeader(403)
					return
				}
				if r.Method == http.MethodGet {
					f.exports++
					_, _ = io.WriteString(w, "raft-snapshot-bytes")
					return
				}
				f.imports++
				f.force = r.URL.Path == "/v1/sys/storage/raft/snapshot-force"
				data, _ := io.ReadAll(r.Body)
				if string(data) != "raft-snapshot-bytes" {
					t.Errorf("unexpected imported bytes %q", data)
				}
				// Policies are part of the restored barrier state. This snapshot
				// predates every randomly generated restore marker.
				clear(f.restorePolicies)
				agents.tokenValid = false
				if f.force {
					agents.shares = f.sourceShares
					for _, b := range agents.members {
						b.sealed = true
						b.active = false
					}
				}
				if f.loseImportReply {
					f.loseImportReply = false
					panic(http.ErrAbortHandler)
				}
				w.WriteHeader(204)
				return
			}
			agents.serveNative(name, w, r)
		}))
		server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
		server.StartTLS()
		t.Cleanup(server.Close)
		agents.nativeServers[name] = server
	}
	return f
}

func snapshotRun(t *testing.T, f *snapshotFixture, op *domain.Operation) {
	t.Helper()
	for range 20 {
		result, handled, err := f.p.runBackupOperation(t.Context(), f.rc, op)
		if err != nil {
			t.Fatal(err)
		}
		if !handled {
			t.Fatal("not handled")
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			return
		}
	}
	t.Fatal("operation did not complete")
}

func TestSnapshotBackupStreamingAndHeadRecovery(t *testing.T) {
	f := newSnapshotFixture(t)
	op := &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snap"}`)}
	result, _, err := f.p.runBackupOperation(t.Context(), f.rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	result, _, err = f.p.runBackupOperation(t.Context(), f.rc, op)
	if err != nil {
		t.Fatal(err)
	}
	if result.Step != "complete" {
		t.Fatalf("step %s", result.Step)
	}
	// Drop the returned upload checkpoint, reconstruct the provider, recover via HEAD.
	f.p = New(ProviderOptions{})
	snapshotRun(t, f, op)
	if f.exports != 1 || f.store.backup.Size != int64(len("raft-snapshot-bytes")) || f.store.backup.Phase != domain.PhaseSucceeded {
		t.Fatalf("exports=%d backup=%+v", f.exports, f.store.backup)
	}
	location := f.store.backup.Location
	if !strings.HasPrefix(location, "prefix/") || !strings.HasSuffix(location, "/raft/snap.snap") {
		t.Fatal(location)
	}
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationDeleteBackup, Payload: json.RawMessage(`{"backup":"snap"}`)})
	if f.store.backup != nil || len(f.s3.objects) != 0 {
		t.Fatal("backup object/record remained")
	}
}

func TestSnapshotRetainedDeleteAndLocationSafety(t *testing.T) {
	f := newSnapshotFixture(t)
	b := domain.Backup{Namespace: f.rc.Cluster.Ref().Namespace, Name: "snap", Storage: "s3", Provider: "openbao", Type: "raft-snapshot", Location: "old-prefix/source-uid/raft/snap.snap"}
	f.s3.objects["/bucket/"+b.Location] = []byte("snapshot")
	if err := f.p.DeleteRetainedBackup(t.Context(), f.store.storage, b); err != nil {
		t.Fatal(err)
	}
	if len(f.s3.objects) != 0 {
		t.Fatal("object remains")
	}
	b.Location = "../source-uid/raft/snap.snap"
	if err := f.p.DeleteRetainedBackup(t.Context(), f.store.storage, b); err == nil {
		t.Fatal("unsafe object accepted")
	}
}

func TestSnapshotSameClusterRestoreRecoversManagementToken(t *testing.T) {
	f := newSnapshotFixture(t)
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snap"}`)})
	f.store.restore = &domain.Restore{Name: "restore", Source: domain.RestoreSource{Backup: "snap"}, Phase: domain.PhasePending}
	before := f.agents.generated
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)})
	if f.imports != 1 || f.force || f.agents.generated != before+1 || f.store.restore.Phase != domain.PhaseSucceeded {
		t.Fatalf("imports=%d force=%v generated=%d restore=%+v", f.imports, f.force, f.agents.generated, f.store.restore)
	}
}

func TestSnapshotCrossClusterCopiesSharesAndUnsealsEveryMember(t *testing.T) {
	f := newSnapshotFixture(t)
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snap"}`)})
	source := domain.ClusterRef{Namespace: f.rc.Cluster.Ref().Namespace, Name: "source"}
	f.store.backup.Cluster = source.Name
	material := storedUnseal{Shares: []string{"source-share-one", "source-share-two", "source-share-three"}, Threshold: 3, SealType: "shamir"}
	f.sourceShares = material.Shares
	raw, _ := json.Marshal(material)
	if err := f.rc.Secrets.SetClusterSecret(t.Context(), source, SecretUnseal, raw); err != nil {
		t.Fatal(err)
	}
	before := map[string]int{}
	for name, b := range f.agents.members {
		before[name] = b.unseals
	}
	f.store.restore = &domain.Restore{Name: "restore", SourceNamespace: source.Namespace, Source: domain.RestoreSource{Backup: "snap", SourceCluster: source.Name}}
	op := &domain.Operation{Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
	for range 2 {
		result, _, err := f.p.runBackupOperation(t.Context(), f.rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
	}
	// The foreign POST succeeds but its returned checkpoint is lost.
	if result, _, err := f.p.runBackupOperation(t.Context(), f.rc, op); err != nil || result.Step != "unseal" {
		t.Fatalf("post: %+v %v", result, err)
	}
	f.p = New(ProviderOptions{})
	snapshotRun(t, f, op)
	if !f.force || f.imports != 1 {
		t.Fatal("cross cluster restore did not force snapshot")
	}
	for name, b := range f.agents.members {
		if b.sealed || b.unseals != before[name]+3 {
			t.Fatalf("%s sealed=%v unseals=%d", name, b.sealed, b.unseals)
		}
	}
	target, err := readUnseal(t.Context(), f.rc)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(target.Shares, ",") != strings.Join(material.Shares, ",") {
		t.Fatal("source material not copied")
	}
	for _, secret := range material.Shares {
		if bytes.Contains(op.Checkpoint, []byte(secret)) || bytes.Contains(f.rc.Cluster.Internal, []byte(secret)) {
			t.Fatal("share persisted outside secret store")
		}
	}
}

func TestSnapshotDeletedSourceRequiresSuppliedShares(t *testing.T) {
	f := newSnapshotFixture(t)
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snap"}`)})
	source := domain.ClusterRef{Namespace: f.rc.Cluster.Ref().Namespace, Name: "deleted"}
	if err := f.p.prepareSnapshotMaterial(t.Context(), f.rc, source, "restore", true, f.store.backup); err == nil {
		t.Fatal("missing shares accepted")
	}
	v := RestoreUnsealShares{Shares: []string{"supplied-key-one", "supplied-key-two", "supplied-key-three"}, Threshold: 3}
	raw, _ := json.Marshal(v)
	if err := putSecret(t.Context(), f.rc, RestoreUnsealSecret("restore"), raw); err != nil {
		t.Fatal(err)
	}
	f.sourceShares = v.Shares
	f.store.backup.Cluster = source.Name
	f.store.restore = &domain.Restore{Name: "restore", SourceNamespace: source.Namespace, Source: domain.RestoreSource{SourceCluster: source.Name, Backup: "snap"}}
	op := &domain.Operation{Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
	snapshotRun(t, f, op)
	got, err := readUnseal(t.Context(), f.rc)
	if err != nil || got.Shares[0] != v.Shares[0] {
		t.Fatalf("material recovery failed: %v", err)
	}
	if _, err = secret(t.Context(), f.rc, RestoreUnsealSecret("restore")); !missing(err) {
		t.Fatalf("staged shares not consumed: %v", err)
	}
	clusterJSON, _ := json.Marshal(f.rc.Cluster)
	restoreJSON, _ := json.Marshal(f.store.restore)
	for _, share := range v.Shares {
		if bytes.Contains(clusterJSON, []byte(share)) || bytes.Contains(restoreJSON, []byte(share)) || bytes.Contains(op.Checkpoint, []byte(share)) || bytes.Contains(op.Payload, []byte(share)) {
			t.Fatal("write-only share leaked outside SecretStore")
		}
	}
}

func TestSnapshotSizeBound(t *testing.T) {
	r := &snapshotBoundedReader{r: strings.NewReader("oversize"), remaining: 3}
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("oversized stream silently truncated")
	}
	r = &snapshotBoundedReader{r: strings.NewReader("fit"), remaining: 3}
	if got, err := io.ReadAll(r); err != nil || string(got) != "fit" {
		t.Fatalf("%q %v", got, err)
	}
}

func TestSnapshotScheduleValidation(t *testing.T) {
	f := newSnapshotFixture(t)
	f.p.options.Store = f.store
	schedule := domain.BackupSchedule{Name: "daily", Cron: "0 0 * * *", Type: "raft-snapshot", Storage: "s3", RetainCount: 3}
	for _, tc := range []struct {
		name   string
		change func(*domain.BackupSchedule)
		valid  bool
	}{
		{"valid", func(*domain.BackupSchedule) {}, true},
		{"cron", func(s *domain.BackupSchedule) { s.Cron = "not cron" }, false},
		{"type", func(s *domain.BackupSchedule) { s.Type = "full" }, false},
		{"retention", func(s *domain.BackupSchedule) { s.RetainCount = 0 }, false},
		{"storage", func(s *domain.BackupSchedule) { s.Storage = "" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := *f.rc.Cluster
			s := schedule
			tc.change(&s)
			next.Spec.Backup.Schedules = []domain.BackupSchedule{s}
			err := f.p.Validate(t.Context(), nil, &next)
			if (err == nil) != tc.valid {
				t.Fatalf("validation=%v want valid=%v", err, tc.valid)
			}
		})
	}
}

func TestSnapshotCloneResumesAfterProviderReconstruction(t *testing.T) {
	f := newSnapshotFixture(t)
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snap"}`)})
	source := domain.ClusterRef{Namespace: f.rc.Cluster.Ref().Namespace, Name: "deleted"}
	f.store.backup.Cluster = source.Name
	f.rc.Cluster.Spec.DataSource = &domain.DataSource{Namespace: source.Namespace, Cluster: source.Name, Backup: "snap"}
	v := RestoreUnsealShares{Shares: []string{"clone-one", "clone-two", "clone-three"}, Threshold: 3}
	f.sourceShares = v.Shares
	raw, _ := json.Marshal(v)
	if err := putSecret(t.Context(), f.rc, RestoreUnsealSecret("clone"), raw); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		s := state(t, f.rc)
		pending, err := f.p.restoreDataSource(t.Context(), f.rc, s)
		if err != nil {
			t.Fatal(err)
		}
		f.p = New(ProviderOptions{})
		if !pending {
			if f.imports != 1 || !f.force || s.CloneRestoreStep != "done" {
				t.Fatal("clone did not restore exactly once")
			}
			return
		}
	}
	t.Fatal("clone did not converge")
}

func TestSnapshotProvenanceRejectsReusedClusterName(t *testing.T) {
	f := newSnapshotFixture(t)
	snapshotRun(t, f, &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snap"}`)})
	if !SnapshotBelongsToCluster(f.rc.Cluster, f.store.backup) {
		t.Fatal("own snapshot did not match")
	}
	s := state(t, f.rc)
	s.UID = "new-incarnation"
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if SnapshotBelongsToCluster(f.rc.Cluster, f.store.backup) {
		t.Fatal("former incarnation accepted")
	}
	if err := f.p.prepareSnapshotMaterial(t.Context(), f.rc, f.rc.Cluster.Ref(), "restore", true, f.store.backup); err == nil {
		t.Fatal("fresh target keys accepted for former incarnation")
	}
}
