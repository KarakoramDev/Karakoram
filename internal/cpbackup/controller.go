// SPDX-License-Identifier: Apache-2.0
package cpbackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/minio/minio-go/v7"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type Recorder interface {
	Record(context.Context, domain.Event) error
}
type ObjectClient interface {
	PutObject(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (minio.UploadInfo, error)
	RemoveObject(context.Context, string, string, minio.RemoveObjectOptions) error
}
type Options struct {
	Store     *store.Store
	Sealer    *secrets.Sealer
	Clock     engine.Clock
	Events    Recorder
	DataDir   string
	Version   string
	Commit    string
	Logger    *slog.Logger
	NewClient func(domain.BackupStorage) (ObjectClient, func(), error)
}
type Controller struct {
	opts Options
	mu   sync.Mutex
}
type scheduleCursor struct {
	Spec    domain.ControlPlaneBackupPolicy `json:"spec"`
	LastDue time.Time                       `json:"lastDue"`
}

func New(o Options) *Controller {
	if o.Clock == nil {
		o.Clock = engine.RealClock{}
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.NewClient == nil {
		o.NewClient = func(b domain.BackupStorage) (ObjectClient, func(), error) {
			return backupstorage.NewClient(b, b.Credentials)
		}
	}
	return &Controller{opts: o}
}

// Run fails interrupted Running records once, then drains durable Pending work.
func (c *Controller) Run(ctx context.Context) error {
	if err := c.Recover(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		if err := c.Step(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.opts.Logger.Error("Control-plane backup controller failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
func (c *Controller) Recover(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	runs, err := c.opts.Store.ListControlPlaneBackups(ctx)
	if err != nil {
		return err
	}
	for _, b := range runs {
		if b.Phase == domain.PhaseRunning {
			if err = c.finish(ctx, &b, errors.New("backup interrupted by control-plane restart")); err != nil {
				return err
			}
		}
	}
	return nil
}
func (c *Controller) Step(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.schedule(ctx); err != nil && !errors.Is(err, store.ErrConflict) {
		return err
	}
	runs, err := c.opts.Store.ListControlPlaneBackups(ctx)
	if err != nil {
		return err
	}
	for i := len(runs) - 1; i >= 0; i-- {
		b := runs[i]
		if b.Phase != domain.PhasePending {
			continue
		}
		now := c.opts.Clock.Now().UTC()
		b.StartedAt = &now
		b.Phase = domain.PhaseRunning
		if err = c.opts.Store.SaveControlPlaneBackup(ctx, &b, domain.PhasePending); err != nil {
			return err
		}
		runErr := c.execute(ctx, &b)
		return c.finish(context.WithoutCancel(ctx), &b, runErr)
	}
	return nil
}
func (c *Controller) schedule(ctx context.Context) error {
	p, err := Policy(ctx, c.opts.Store)
	if err != nil {
		return err
	}
	now := c.opts.Clock.Now().UTC()
	raw, err := c.opts.Store.GetSetting(ctx, "controlPlaneBackup/cursor")
	var cursor scheduleCursor
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	if err == nil {
		if err = json.Unmarshal(raw, &cursor); err != nil {
			return err
		}
	}
	if cursor.Spec != p || cursor.LastDue.IsZero() {
		raw, _ = json.Marshal(scheduleCursor{p, now})
		return c.opts.Store.SetSetting(ctx, "controlPlaneBackup/cursor", raw)
	}
	if !p.Enabled {
		return nil
	}
	schedule, err := parser.Parse(p.Schedule)
	if err != nil {
		return err
	}
	due := schedule.Next(cursor.LastDue)
	if due.After(now) {
		return nil
	}
	// Advancing to observation time coalesces all missed occurrences into one run.
	sum := sha256.Sum256([]byte(due.Format(time.RFC3339Nano) + "/" + p.Storage.Namespace + "/" + p.Storage.Name))
	b := domain.ControlPlaneBackup{ID: "cp-" + hex.EncodeToString(sum[:16]), Trigger: "schedule", Phase: domain.PhasePending, Storage: p.Storage, CreatedAt: now}
	raw, _ = json.Marshal(scheduleCursor{p, now})
	return c.opts.Store.EnqueueControlPlaneBackup(ctx, &b, raw)
}
func (c *Controller) execute(ctx context.Context, b *domain.ControlPlaneBackup) error {
	storage, err := c.opts.Store.GetBackupStorage(ctx, b.Storage.Namespace, b.Storage.Name)
	if err != nil {
		return errors.New("backup storage unavailable")
	}
	client, closeClient, err := c.opts.NewClient(*storage)
	if err != nil {
		return errors.New("backup storage client unavailable")
	}
	defer closeClient()
	file, err := os.CreateTemp(c.opts.DataDir, ".cpbackup-*")
	if err != nil {
		return errors.New("cannot create backup staging file")
	}
	out := file.Name()
	file.Close()
	defer os.Remove(out)
	defer os.Remove(out + ".json")
	m, err := exportAt(ctx, c.opts.Store, c.opts.Sealer, out, c.opts.Version, c.opts.Commit, c.opts.Clock.Now().UTC())
	if err != nil {
		return errors.New("control-plane snapshot export failed")
	}
	prefix := path.Join(strings.Trim(storage.Prefix, "/"), "karakoram-control-plane", m.InstallationID) + "/"
	b.ObjectKey = prefix + m.CreatedAt.UTC().Format("20060102T150405Z") + "-" + b.ID + ".db.gz"
	b.SizeBytes = m.SizeBytes
	b.SHA256 = m.SHA256
	b.SchemaVersion = m.SchemaVersion
	b.KarakoramVersion = m.KarakoramVersion
	b.KeyFingerprint = m.KeyFingerprint
	archive, err := os.Open(out)
	if err != nil {
		return errors.New("cannot read backup archive")
	}
	defer archive.Close()
	if _, err = client.PutObject(ctx, storage.Bucket, b.ObjectKey, archive, m.SizeBytes, minio.PutObjectOptions{ContentType: "application/gzip"}); err != nil {
		return errors.New("backup archive upload failed")
	}
	manifest, _ := json.Marshal(m)
	if _, err = client.PutObject(ctx, storage.Bucket, manifestKey(b.ObjectKey), bytes.NewReader(manifest), int64(len(manifest)), minio.PutObjectOptions{ContentType: "application/json"}); err != nil {
		_ = client.RemoveObject(ctx, storage.Bucket, b.ObjectKey, minio.RemoveObjectOptions{})
		return errors.New("backup manifest upload failed")
	}
	return nil
}
func manifestKey(key string) string { return strings.TrimSuffix(key, ".db.gz") + ".json" }
func (c *Controller) finish(ctx context.Context, b *domain.ControlPlaneBackup, runErr error) error {
	now := c.opts.Clock.Now().UTC()
	b.CompletedAt = &now
	b.Phase = domain.PhaseSucceeded
	reason, kind, message := "ControlPlaneBackupSucceeded", "Normal", "Control-plane backup succeeded"
	if runErr != nil {
		b.Phase = domain.PhaseFailed
		b.Error = runErr.Error()
		reason, kind, message = "ControlPlaneBackupFailed", "Warning", b.Error
	}
	if err := c.opts.Store.SaveControlPlaneBackup(ctx, b, domain.PhaseRunning); err != nil {
		return err
	}
	if c.opts.Events != nil {
		if err := c.opts.Events.Record(ctx, domain.Event{Type: kind, Reason: reason, Message: message, CreatedAt: now}); err != nil {
			return err
		}
	}
	if runErr == nil {
		return c.retain(ctx)
	}
	return nil
}
func (c *Controller) retain(ctx context.Context) error {
	p, err := Policy(ctx, c.opts.Store)
	if err != nil {
		return err
	}
	id, err := c.opts.Store.EnsureInstallationID(ctx)
	if err != nil {
		return err
	}
	runs, err := c.opts.Store.ListControlPlaneBackups(ctx)
	if err != nil {
		return err
	}
	count := 0
	for _, b := range runs {
		if b.Phase != domain.PhaseSucceeded {
			continue
		}
		storage, err := c.opts.Store.GetBackupStorage(ctx, b.Storage.Namespace, b.Storage.Name)
		if err != nil {
			return errors.New("retention storage unavailable")
		}
		prefix := path.Join(strings.Trim(storage.Prefix, "/"), "karakoram-control-plane", id) + "/"
		if !strings.HasPrefix(b.ObjectKey, prefix) {
			continue
		}
		count++
		if count <= p.RetainCount {
			continue
		}
		client, closeClient, err := c.opts.NewClient(*storage)
		if err != nil {
			return errors.New("retention client unavailable")
		}
		err = client.RemoveObject(ctx, storage.Bucket, b.ObjectKey, minio.RemoveObjectOptions{})
		if err == nil {
			err = client.RemoveObject(ctx, storage.Bucket, manifestKey(b.ObjectKey), minio.RemoveObjectOptions{})
		}
		closeClient()
		if err != nil {
			return errors.New("backup retention deletion failed")
		}
		if err = c.opts.Store.DeleteControlPlaneBackup(ctx, b.ID); err != nil {
			return err
		}
	}
	return nil
}
