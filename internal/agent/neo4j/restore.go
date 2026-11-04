// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/minio/minio-go/v7"
)

func calendarVersion(v string) ([3]int, error) {
	var result [3]int
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return result, errors.New("invalid calendar version")
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return result, errors.New("invalid calendar version")
		}
		result[i] = n
	}
	if result[0] < 2025 || result[1] < 1 || result[1] > 12 {
		return result, errors.New("invalid calendar version")
	}
	return result, nil
}
func validateManifest(m neo4jspec.Manifest, prefix, version string) error {
	if err := validateObjectPrefix(prefix); err != nil {
		return err
	}
	if m.Format != neo4jspec.ManifestFormat || m.Edition != "community" {
		return errors.New("unsupported dump manifest")
	}
	source, err := calendarVersion(m.Neo4jVersion)
	if err != nil {
		return err
	}
	target, err := calendarVersion(version)
	if err != nil {
		return err
	}
	for i := range source {
		if source[i] > target[i] {
			return errors.New("backup version is newer than target")
		}
		if source[i] < target[i] {
			break
		}
	}
	if len(m.Databases) != len(neo4jspec.Databases) {
		return errors.New("manifest database set is incomplete")
	}
	seen := map[string]bool{}
	for _, o := range m.Databases {
		known := false
		for _, db := range neo4jspec.Databases {
			if o.Database == db {
				known = true
			}
		}
		sum, err := hex.DecodeString(o.SHA256)
		if !known || seen[o.Database] || o.Size < 0 || err != nil || len(sum) != sha256.Size || !strings.HasPrefix(o.Key, prefix+"/") || path.Clean(o.Key) != o.Key || strings.ContainsAny(o.Key, "\\\x00\r\n") {
			return errors.New("invalid dump object manifest")
		}
		seen[o.Database] = true
	}
	return nil
}

type verifiedDumpReader struct {
	source   io.Reader
	expected neo4jspec.DumpObject
	h        hash.Hash
	n        int64
	verified bool
	abort    context.CancelFunc
}

func (v *verifiedDumpReader) Read(p []byte) (int, error) {
	n, err := v.source.Read(p)
	v.n += int64(n)
	_, _ = v.h.Write(p[:n])
	if v.n > v.expected.Size {
		v.abort()
		return n, errors.New("dump size mismatch")
	}
	if err == io.EOF {
		if v.n != v.expected.Size || !strings.EqualFold(hex.EncodeToString(v.h.Sum(nil)), v.expected.SHA256) {
			v.abort()
			return n, errors.New("dump checksum or size mismatch")
		}
		v.verified = true
	}
	return n, err
}
func exists(name string) bool { _, err := os.Stat(name); return err == nil }

// recoverSwap is idempotent across crashes during either rename. Uncommitted
// replacements are rolled back; a committed replacement is retained.
func recoverSwap(live string, r *jobReceipt) error {
	if r.Swap == "" {
		return nil
	}
	// All swap paths are siblings fixed by the loader, never manifest-controlled.
	if r.Stage != live+".restore-stage" || r.Old != live+".restore-old" {
		return errors.New("invalid restore swap journal")
	}
	if r.Swap == "committed" {
		if err := os.RemoveAll(r.Old); err != nil {
			return err
		}
	} else if exists(r.Old) {
		if err := os.RemoveAll(live); err != nil {
			return err
		}
		if err := os.Rename(r.Old, live); err != nil {
			return err
		}
	} else if !exists(live) {
		return errors.New("restore rollback store missing")
	}
	if err := os.RemoveAll(r.Stage); err != nil {
		return err
	}
	return syncDir(filepath.Dir(live))
}
func (e *Engine) restoreJob(q neo4jspec.RestoreRequest) offlineJob {
	return func(ctx context.Context, n *agent.Node, s neo4jspec.Spec, r *jobReceipt, phase func(string) error) (result error) {
		if err := validateManifest(q.Manifest, q.Source.Prefix, s.Version); err != nil {
			return err
		}
		client, cleanup, err := newJobS3(q.Source)
		if err != nil {
			return errors.New("object storage configuration rejected")
		}
		defer cleanup()
		r.held = true
		if err = n.Stop(ctx, neo4jspec.Process); err != nil {
			return errors.New("server stop failed")
		}
		live := dataDir(n)
		r.Stage = live + ".restore-stage"
		r.Old = live + ".restore-old"
		if exists(r.Old) || exists(r.Stage) {
			return errors.New("restore staging directory already exists")
		}
		r.Swap = "staging"
		if err = phase(neo4jspec.PhaseLoading); err != nil {
			return err
		}
		if err = os.MkdirAll(r.Stage, 0700); err != nil {
			return err
		}
		defer func() {
			if result != nil {
				if rollbackErr := recoverSwap(live, r); rollbackErr != nil {
					result = rollbackErr
				} else {
					r.Swap = ""
				}
			}
			if r.Swap == "" {
				_ = os.RemoveAll(r.Stage)
			}
		}()
		additional := filepath.Join(r.Stage, "load.conf")
		// Neo4j's additional-config overrides only data paths for offline loading;
		// the normal managed TLS/configuration remains unchanged.
		if strings.ContainsAny(r.Stage, "\r\n") {
			return errors.New("invalid staging path")
		}
		text := fmt.Sprintf("server.directories.data=%s\nserver.directories.transaction.logs.root=%s\n", r.Stage, filepath.Join(r.Stage, "transactions"))
		if err = os.WriteFile(additional, []byte(text), 0600); err != nil {
			return err
		}
		for _, db := range neo4jspec.Databases {
			var object neo4jspec.DumpObject
			for _, o := range q.Manifest.Databases {
				if o.Database == db {
					object = o
				}
			}
			if err = phase(neo4jspec.PhaseFetching); err != nil {
				return err
			}
			source, getErr := client.GetObject(ctx, q.Source.Bucket, object.Key, minio.GetObjectOptions{})
			if getErr != nil {
				return errors.New("dump fetch failed")
			}
			loadCtx, cancel := context.WithCancel(ctx)
			reader := &verifiedDumpReader{source: source, expected: object, h: sha256.New(), abort: cancel}
			cmd := adminCommand(loadCtx, s, confDir(n), "database", "load", db, "--from-stdin", "--overwrite-destination=true", "--additional-config="+additional)
			cmd.Stdin = reader
			cmd.Stdout = io.Discard
			cmd.Stderr = &boundedStderr{}
			if err = phase(neo4jspec.PhaseLoading); err != nil {
				cancel()
				source.Close()
				return err
			}
			loadErr := cmd.Run()
			cancel()
			source.Close()
			if loadErr != nil || !reader.verified {
				return errors.New("dump verification or database load failed")
			}
		}
		if err = os.Remove(additional); err != nil {
			return err
		}
		r.Swap = "moving-old"
		if err = phase(neo4jspec.PhaseLoading); err != nil {
			return err
		}
		if err = os.Rename(live, r.Old); err != nil {
			return err
		}
		if err = syncDir(filepath.Dir(live)); err != nil {
			return err
		}
		r.Swap = "installing"
		if err = phase(neo4jspec.PhaseLoading); err != nil {
			return err
		}
		if err = os.Rename(r.Stage, live); err != nil {
			return err
		}
		if err = syncDir(filepath.Dir(live)); err != nil {
			return err
		}
		r.Swap = "installed"
		if err = phase(neo4jspec.PhaseRecovering); err != nil {
			return err
		}
		if err = e.recoverAdmin(ctx, n, s, q.AdminPassword); err != nil {
			return errors.New("administrator recovery failed")
		}
		r.Manifest = &q.Manifest
		return nil
	}
}
