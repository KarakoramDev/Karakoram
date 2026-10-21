// SPDX-License-Identifier: Apache-2.0
package cpbackup

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/minio/minio-go/v7"
	"io"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time { return f.now }

type memoryObjects struct {
	objects map[string][]byte
	fail    bool
}

func (m *memoryObjects) PutObject(_ context.Context, bucket, key string, r io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	if m.fail {
		return minio.UploadInfo{}, errors.New("credentials https://user:secret@example.invalid")
	}
	raw, err := io.ReadAll(r)
	m.objects[bucket+"/"+key] = raw
	return minio.UploadInfo{}, err
}
func (m *memoryObjects) RemoveObject(_ context.Context, bucket, key string, _ minio.RemoveObjectOptions) error {
	delete(m.objects, bucket+"/"+key)
	return nil
}

type eventLog struct{ items []domain.Event }

func (e *eventLog) Record(_ context.Context, v domain.Event) error {
	e.items = append(e.items, v)
	return nil
}
func fixture(t *testing.T) (context.Context, *Controller, *memoryObjects, *fakeClock, *eventLog) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	s, _ := secrets.New(strings.Repeat("s", 32))
	st, err := store.Open(filepath.Join(dir, "karakoram.db"), s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	b := domain.BackupStorage{Namespace: "default", Name: "state", Type: "s3", Bucket: "backups", Prefix: "state", Credentials: domain.StorageCredentials{AccessKeyID: "key", SecretAccessKey: "secret"}}
	if err = st.CreateBackupStorage(ctx, &b); err != nil {
		t.Fatal(err)
	}
	p := Defaults(domain.ControlPlaneBackupPolicy{Enabled: true, Storage: domain.ControlPlaneBackupStorage{Namespace: "default", Name: "state"}, Schedule: "0 3 * * *", RetainCount: 1})
	raw, _ := json.Marshal(p)
	if err = st.SetSetting(ctx, PolicyKey, raw); err != nil {
		t.Fatal(err)
	}
	objects := &memoryObjects{objects: map[string][]byte{}}
	clock := &fakeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	events := &eventLog{}
	c := New(Options{Store: st, Sealer: s, DataDir: dir, Clock: clock, Events: events, Version: "test", Commit: "commit", NewClient: func(domain.BackupStorage) (ObjectClient, func(), error) { return objects, func() {}, nil }})
	return ctx, c, objects, clock, events
}
func enqueue(t *testing.T, ctx context.Context, c *Controller, id string, phase domain.Phase) {
	t.Helper()
	b := domain.ControlPlaneBackup{ID: id, Trigger: "manual", Phase: phase, Storage: domain.ControlPlaneBackupStorage{Namespace: "default", Name: "state"}, CreatedAt: c.opts.Clock.Now()}
	if err := c.opts.Store.EnqueueControlPlaneBackup(ctx, &b, nil); err != nil {
		t.Fatal(err)
	}
}
func TestControllerScheduleCatchupAndRetention(t *testing.T) {
	ctx, c, objects, clock, events := fixture(t)
	if err := c.Step(ctx); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(72 * time.Hour)
	if err := c.Step(ctx); err != nil {
		t.Fatal(err)
	}
	runs, err := c.opts.Store.ListControlPlaneBackups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Phase != domain.PhaseSucceeded || runs[0].Trigger != "schedule" {
		t.Fatalf("runs=%+v", runs)
	}
	first := runs[0]
	if len(objects.objects) != 2 {
		t.Fatalf("objects=%d", len(objects.objects))
	}
	var m Manifest
	if err = json.Unmarshal(objects.objects["backups/"+manifestKey(first.ObjectKey)], &m); err != nil {
		t.Fatal(err)
	}
	if m.KeyFingerprint != c.opts.Sealer.Fingerprint() || m.SchemaVersion != store.LatestSchemaVersion() {
		t.Fatalf("manifest=%+v", m)
	}
	if err = c.Step(ctx); err != nil {
		t.Fatal(err)
	}
	runs, _ = c.opts.Store.ListControlPlaneBackups(ctx)
	if len(runs) != 1 {
		t.Fatal("multiple catchup runs")
	}
	// A successful foreign installation record and object must never be retained/deleted by this installation.
	foreign := domain.ControlPlaneBackup{ID: "foreign", Phase: domain.PhaseSucceeded, Storage: first.Storage, ObjectKey: path.Join("state", "karakoram-control-plane", "other-installation", "foreign.db.gz"), CreatedAt: clock.now.Add(-time.Hour)}
	if err = c.opts.Store.EnqueueControlPlaneBackup(ctx, &foreign, nil); err != nil {
		t.Fatal(err)
	}
	objects.objects["backups/"+foreign.ObjectKey] = []byte("foreign")
	clock.now = clock.now.Add(time.Second)
	enqueue(t, ctx, c, "manual", domain.PhasePending)
	if err = c.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := objects.objects["backups/"+first.ObjectKey]; ok {
		t.Fatal("old archive retained")
	}
	if _, ok := objects.objects["backups/"+manifestKey(first.ObjectKey)]; ok {
		t.Fatal("old manifest retained")
	}
	if _, ok := objects.objects["backups/"+foreign.ObjectKey]; !ok {
		t.Fatal("foreign installation deleted")
	}
	if len(events.items) != 2 {
		t.Fatalf("events=%+v", events.items)
	}
	success, _, err := c.opts.Store.LatestControlPlaneBackupTimes(ctx)
	if err != nil || !success.Equal(clock.now) {
		t.Fatalf("latest success=%v err=%v", success, err)
	}
}
func TestControllerCrashRecoveryAndFailureSanitization(t *testing.T) {
	ctx, c, objects, clock, events := fixture(t)
	enqueue(t, ctx, c, "interrupted", domain.PhaseRunning)
	if err := c.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	runs, _ := c.opts.Store.ListControlPlaneBackups(ctx)
	if runs[0].Phase != domain.PhaseFailed || !strings.Contains(runs[0].Error, "restart") {
		t.Fatalf("run=%+v", runs[0])
	}
	objects.fail = true
	clock.now = clock.now.Add(time.Second)
	enqueue(t, ctx, c, "failure", domain.PhasePending)
	if err := c.Step(ctx); err != nil {
		t.Fatal(err)
	}
	runs, _ = c.opts.Store.ListControlPlaneBackups(ctx)
	if runs[0].Phase != domain.PhaseFailed || strings.Contains(runs[0].Error, "secret") || strings.Contains(runs[0].Error, "https") {
		t.Fatalf("run=%+v", runs[0])
	}
	for _, e := range events.items {
		if e.Type != "Warning" || e.Reason != "ControlPlaneBackupFailed" {
			t.Fatalf("event=%+v", e)
		}
	}
	entries, err := filepath.Glob(filepath.Join(c.opts.DataDir, ".cpbackup-*"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging files=%v err=%v", entries, err)
	}
}
func TestPolicyValidation(t *testing.T) {
	ctx, c, _, _, _ := fixture(t)
	p, _ := Policy(ctx, c.opts.Store)
	for _, tc := range []struct {
		name   string
		change func(*domain.ControlPlaneBackupPolicy)
	}{{"cron seconds", func(p *domain.ControlPlaneBackupPolicy) { p.Schedule = "0 0 3 * * *" }}, {"cron timezone", func(p *domain.ControlPlaneBackupPolicy) { p.Schedule = "CRON_TZ=Europe/Paris 0 3 * * *" }}, {"retain zero", func(p *domain.ControlPlaneBackupPolicy) { p.RetainCount = 0 }}, {"retain high", func(p *domain.ControlPlaneBackupPolicy) { p.RetainCount = 1001 }}, {"missing storage", func(p *domain.ControlPlaneBackupPolicy) { p.Storage.Name = "missing" }}} {
		t.Run(tc.name, func(t *testing.T) {
			copy := p
			tc.change(&copy)
			if err := Validate(ctx, c.opts.Store, copy); err == nil {
				t.Fatal("accepted invalid policy")
			}
		})
	}
	if err := Validate(ctx, c.opts.Store, p); err != nil {
		t.Fatal(err)
	}
}
