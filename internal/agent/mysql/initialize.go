// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func initialize(ctx context.Context, s myspec.Spec, p paths) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	// Runtime GR plugin options cannot be used during --initialize.
	cmd := exec.CommandContext(ctx, filepath.Join(s.ServerDir, "bin", "mysqld"), "--no-defaults", "--basedir="+s.ServerDir, "--datadir="+p.data, "--innodb-buffer-pool-size=134217728", "--initialize-insecure")
	cmd.Env = nativeEnv()
	if err := cmd.Run(); err != nil {
		return errors.New("MySQL data initialization failed; explicit recovery required")
	}
	return durableJSON(filepath.Join(p.root, "initialized.json"), myspec.DataIdentity{Initialized: true})
}
func credentialsRecorded(s myspec.Spec, p paths) bool {
	if _, err := os.Lstat(filepath.Join(p.root, "credentials-pending.json")); !os.IsNotExist(err) {
		return false
	}
	b, err := os.ReadFile(filepath.Join(p.root, "credentials.sha256"))
	h := credentialHash(s)
	return err == nil && string(b) == hex.EncodeToString(h[:])
}
func recordCredentials(s myspec.Spec, p paths) error {
	h := credentialHash(s)
	if err := durableText(filepath.Join(p.root, "credentials.sha256"), hex.EncodeToString(h[:])); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(p.root, "credentials-pending.json")); err != nil && !os.IsNotExist(err) {
		return err
	}
	d, err := os.Open(p.root)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func durableText(path, text string) error {
	// This receipt stores only a one-way digest, never credential material.
	f, err := os.CreateTemp(filepath.Dir(path), ".credential-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.WriteString(text)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func waitLocal(ctx context.Context, s myspec.Spec, p paths) (*sql.DB, error) {
	timeout := 40 * time.Second
	if s.Galera != nil {
		timeout = 3 * time.Minute // encrypted physical SST includes backup and prepare
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	db, err := localDB(s, p)
	if err != nil {
		return nil, err
	}
	for {
		if db.PingContext(ctx) == nil {
			if s.Galera == nil {
				return db, nil
			}
			status, err := galeraStatusDB(ctx, db)
			if err == nil && status.Eligible() {
				return db, nil
			}
		}
		select {
		case <-ctx.Done():
			db.Close()
			return nil, errors.New("MySQL did not accept authenticated local connections before deadline")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
