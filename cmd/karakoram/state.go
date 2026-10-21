// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package main

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/cpbackup"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/KarakoramDev/Karakoram/internal/version"
	"github.com/minio/minio-go/v7"
)

func stateAdmin(ctx context.Context, command string, args []string, out, errOut io.Writer) error {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(errOut)
	var output, file, endpoint, bucket, key, region string
	var replace, pathStyle bool
	if command == "backup-state" {
		fs.StringVar(&output, "out", "", "Output gzip archive")
	} else {
		fs.StringVar(&file, "file", "", "Local gzip archive")
		fs.StringVar(&endpoint, "s3-endpoint", "", "S3 endpoint origin")
		fs.StringVar(&bucket, "bucket", "", "S3 bucket")
		fs.StringVar(&key, "key", "", "S3 archive object key")
		fs.StringVar(&region, "region", "", "S3 region")
		fs.BoolVar(&pathStyle, "path-style", false, "Use S3 path-style addressing")
		fs.BoolVar(&replace, "replace", false, "Preserve and replace existing state")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errUsage
	}
	dir := os.Getenv("KARAKORAM_DATA_DIR")
	if dir == "" {
		dir = "/mnt/karakoramdata"
	}
	sealer, err := secrets.FromEnv()
	if err != nil {
		return err
	}
	if command == "backup-state" {
		if output == "" {
			return errors.New("--out is required")
		}
		dbPath := filepath.Join(dir, "karakoram.db")
		if _, err = os.Stat(dbPath); err != nil {
			return fmt.Errorf("open existing database: %w", err)
		}
		st, err := store.Open(dbPath, sealer)
		if err != nil {
			return err
		}
		defer st.Close()
		_, err = cpbackup.Export(ctx, st, sealer, output, version.Version, version.Commit)
		if err == nil {
			_, err = fmt.Fprintln(out, "Control-plane state exported.")
		}
		return err
	}
	if (file == "") == (endpoint == "") {
		return errors.New("specify either --file or --s3-endpoint")
	}
	if file != "" && (bucket != "" || key != "" || region != "" || pathStyle) {
		return errors.New("S3 flags cannot be combined with --file")
	}
	if endpoint != "" {
		if bucket == "" || key == "" {
			return errors.New("--bucket and --key are required")
		}
		storage := domain.BackupStorage{Type: "s3", EndpointURL: endpoint, Bucket: bucket, Region: region, ForcePathStyle: pathStyle, VerifyTLS: true}
		creds := domain.StorageCredentials{AccessKeyID: os.Getenv("KARAKORAM_RESTORE_S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("KARAKORAM_RESTORE_S3_SECRET_ACCESS_KEY")}
		client, closeClient, err := backupstorage.NewClient(storage, creds)
		if err != nil {
			_, message := backupstorage.Failure(err)
			return errors.New(message)
		}
		defer closeClient()
		temp, err := os.MkdirTemp("", "karakoram-restore-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(temp)
		file = filepath.Join(temp, "state.db.gz")
		for _, item := range []struct{ key, path string }{{key, file}, {strings.TrimSuffix(key, ".db.gz") + ".json", file + ".json"}} {
			obj, err := client.GetObject(ctx, bucket, item.key, minio.GetObjectOptions{})
			if err != nil {
				return errors.New("fetch S3 state backup failed")
			}
			dst, err := os.OpenFile(item.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				obj.Close()
				return err
			}
			_, copyErr := io.Copy(dst, obj)
			err = errors.Join(copyErr, dst.Close(), obj.Close())
			if err != nil {
				return errors.New("fetch S3 state backup failed")
			}
		}
	}
	if err = restoreState(ctx, dir, file, sealer, replace); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "Control-plane state restored. Start the server with the original master key.")
	return err
}

func restoreState(ctx context.Context, dir, archive string, sealer *secrets.Sealer, replace bool) error {
	manifestFile, err := os.Open(archive + ".json")
	if err != nil {
		return err
	}
	var m cpbackup.Manifest
	err = json.NewDecoder(io.LimitReader(manifestFile, 1<<20)).Decode(&m)
	manifestFile.Close()
	if err != nil {
		return fmt.Errorf("read state manifest: %w", err)
	}
	if m.Format != "karakoram-state/v1" {
		return errors.New("unsupported state manifest format")
	}
	if m.KeyFingerprint != sealer.Fingerprint() {
		return fmt.Errorf("master key fingerprint mismatch: backup %s, current %s; supply the original master key using KARAKORAM_MASTER_KEY", m.KeyFingerprint, sealer.Fingerprint())
	}
	if m.SchemaVersion > store.LatestSchemaVersion() {
		return errors.New("state backup schema is newer than this binary")
	}
	src, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer src.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, src)
	if err != nil {
		return err
	}
	if size != m.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != m.SHA256 {
		return errors.New("state backup sha256 or size mismatch")
	}
	if _, err = src.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "karakoram.db.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("database is in use; stop the server before restoring")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	path := filepath.Join(dir, "karakoram.db")
	exists := false
	if _, err = os.Stat(path); err == nil {
		exists = true
	} else if !os.IsNotExist(err) {
		return err
	}
	if exists && !replace {
		return errors.New("database already exists; use --replace to preserve and replace it")
	}
	// Keep the exclusive SQLite connection alive through publication, also detecting
	// legacy servers which do not yet participate in the lifetime advisory lock.
	if exists {
		db, err := sql.Open("sqlite", "file:"+url.PathEscape(path)+"?mode=rw&_pragma=busy_timeout(0)")
		if err != nil {
			return err
		}
		defer db.Close()
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		// Fold committed WAL data into the main file and remove SQLite sidecars
		// before preserving it. DELETE mode also refuses active legacy readers.
		var mode string
		if err = conn.QueryRowContext(ctx, "PRAGMA journal_mode=DELETE").Scan(&mode); err != nil || mode != "delete" {
			return errors.New("database is in use or cannot checkpoint; stop the server before restoring")
		}
		if _, err = conn.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
			return errors.New("database is in use; stop the server before restoring")
		}
		defer conn.ExecContext(context.Background(), "ROLLBACK")
	}
	tmp, err := os.CreateTemp(dir, ".restore-state-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	gz, err := gzip.NewReader(src)
	if err != nil {
		tmp.Close()
		return fmt.Errorf("invalid state gzip: %w", err)
	}
	_, copyErr := io.Copy(tmp, gz)
	err = errors.Join(copyErr, gz.Close(), tmp.Sync(), tmp.Close())
	if err != nil {
		return fmt.Errorf("decode state gzip: %w", err)
	}
	if err = validateStateSQLite(ctx, tmpPath, m.SchemaVersion); err != nil {
		return err
	}
	if exists {
		backup := path + ".replaced-" + time.Now().UTC().Format("20060102T150405Z")
		if err = preserveState(path, backup); err != nil {
			return err
		}
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// preserveState durably retains the checkpointed old inode without removing the
// authoritative name. An error or crash before the subsequent atomic rename
// therefore leaves the old database available, never a fresh-init opportunity.
func preserveState(path, backup string) error {
	for _, ext := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(path + ext); err == nil {
			return fmt.Errorf("database sidecar %s remains after checkpoint; refusing replacement", ext)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err = errors.Join(f.Sync(), f.Close()); err != nil {
		return err
	}
	// Link is exclusive: a colliding backup is never overwritten.
	if err = os.Link(path, backup); err != nil {
		return fmt.Errorf("preserve existing state: %w", err)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func validateStateSQLite(ctx context.Context, path, manifestSchema string) error {
	db, err := sql.Open("sqlite", "file:"+url.PathEscape(path)+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return fmt.Errorf("state SQLite integrity check: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var result string
		if err = rows.Scan(&result); err != nil {
			return err
		}
		if result != "ok" {
			return errors.New("state SQLite integrity check failed")
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	var schema string
	if err = db.QueryRowContext(ctx, "SELECT COALESCE(MAX(name),'') FROM schema_migrations").Scan(&schema); err != nil {
		return err
	}
	if schema > store.LatestSchemaVersion() {
		return errors.New("state backup schema is newer than this binary")
	}
	if schema != manifestSchema {
		return errors.New("state backup schema does not match manifest")
	}
	return nil
}
