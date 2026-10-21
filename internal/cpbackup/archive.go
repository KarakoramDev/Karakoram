// SPDX-License-Identifier: Apache-2.0
package cpbackup

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"io"
	"os"
	"path/filepath"
	"time"
)

type Manifest struct {
	Format           string    `json:"format"`
	InstallationID   string    `json:"installationId"`
	CreatedAt        time.Time `json:"createdAt"`
	KarakoramVersion string    `json:"karakoramVersion"`
	Commit           string    `json:"commit"`
	SchemaVersion    string    `json:"schemaVersion"`
	KeyFingerprint   string    `json:"keyFingerprint"`
	SHA256           string    `json:"sha256"`
	SizeBytes        int64     `json:"sizeBytes"`
}

// Export writes a consistent gzip snapshot and its adjacent manifest.
func Export(ctx context.Context, st *store.Store, sealer *secrets.Sealer, out, version, commit string) (Manifest, error) {
	return exportAt(ctx, st, sealer, out, version, commit, time.Now().UTC())
}
func exportAt(ctx context.Context, st *store.Store, sealer *secrets.Sealer, out, version, commit string, now time.Time) (m Manifest, err error) {
	// A syntactically valid key is not evidence of custody of existing secrets.
	// Authenticate before creating snapshots, outputs or a fingerprint receipt.
	if err = st.AuthenticateSealer(ctx, sealer); err != nil {
		return m, err
	}
	id, err := st.EnsureInstallationID(ctx)
	if err != nil {
		return m, err
	}
	f, err := os.CreateTemp(st.DataDir(), ".state-export-*")
	if err != nil {
		return m, err
	}
	snapshot := f.Name()
	f.Close()
	defer os.Remove(snapshot)
	if err = st.Snapshot(ctx, snapshot); err != nil {
		return m, err
	}
	// Read the schema receipt from the exported file, not from a potentially newer live database.
	schema, err := snapshotSchema(ctx, snapshot)
	if err != nil {
		return m, err
	}
	in, err := os.Open(snapshot)
	if err != nil {
		return m, err
	}
	defer in.Close()
	archive, err := os.CreateTemp(filepath.Dir(out), ".state-gzip-*")
	if err != nil {
		return m, err
	}
	defer os.Remove(archive.Name())
	hash := sha256.New()
	gz := gzip.NewWriter(io.MultiWriter(archive, hash))
	_, err = io.Copy(gz, in)
	err = errors.Join(err, gz.Close())
	if err == nil {
		err = archive.Sync()
	}
	info, statErr := archive.Stat()
	err = errors.Join(err, statErr, archive.Close())
	if err != nil {
		return m, err
	}
	m = Manifest{Format: "karakoram-state/v1", InstallationID: id, CreatedAt: now, KarakoramVersion: version, Commit: commit, SchemaVersion: schema, KeyFingerprint: sealer.Fingerprint(), SHA256: hex.EncodeToString(hash.Sum(nil)), SizeBytes: info.Size()}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return m, err
	}
	mf, err := os.CreateTemp(filepath.Dir(out), ".state-manifest-*")
	if err != nil {
		return m, err
	}
	defer os.Remove(mf.Name())
	_, err = mf.Write(raw)
	if err == nil {
		err = mf.Sync()
	}
	err = errors.Join(err, mf.Close())
	if err != nil {
		return m, err
	}
	if err = os.Rename(archive.Name(), out); err != nil {
		return m, err
	}
	if err = os.Rename(mf.Name(), out+".json"); err != nil {
		return m, err
	}
	dir, err := os.Open(filepath.Dir(out))
	if err != nil {
		return m, err
	}
	defer dir.Close()
	return m, dir.Sync()
}
