// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/minio/minio-go/v7"
)

const dumpPartSize = 64 * 1024 * 1024

// boundedStderr drains all diagnostics without retaining unbounded output or
// exposing native diagnostics (which may contain sensitive environment values).
type boundedStderr struct{ data []byte }

func (b *boundedStderr) Write(p []byte) (int, error) {
	n := len(p)
	remaining := 8192 - len(b.data)
	if remaining > 0 {
		b.data = append(b.data, p[:min(remaining, len(p))]...)
	}
	return n, nil
}

type countedReader struct {
	r io.Reader
	n int64
}

func (r *countedReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}
func (e *Engine) backupJob(q neo4jspec.BackupRequest) offlineJob {
	return func(ctx context.Context, n *agent.Node, s neo4jspec.Spec, r *jobReceipt, phase func(string) error) error {
		client, cleanup, err := newJobS3(q.Target)
		if err != nil {
			return errors.New("object storage configuration rejected")
		}
		defer cleanup()
		r.held = true
		if err = n.Stop(ctx, neo4jspec.Process); err != nil {
			return errors.New("server stop failed")
		}
		manifest := neo4jspec.Manifest{Format: neo4jspec.ManifestFormat, Neo4jVersion: s.Version, Edition: "community", CreatedAt: time.Now().UTC().Format(time.RFC3339)}
		for _, db := range neo4jspec.Databases {
			if err = phase(neo4jspec.PhaseDumping); err != nil {
				return err
			}
			commandCtx, cancel := context.WithCancel(ctx)
			cmd := adminCommand(commandCtx, s, confDir(n), "database", "dump", db, "--to-stdout")
			cmd.Stderr = &boundedStderr{}
			stdout, pipeErr := cmd.StdoutPipe()
			if pipeErr != nil {
				cancel()
				return errors.New("dump pipe failed")
			}
			if err = cmd.Start(); err != nil {
				cancel()
				return errors.New("dump start failed")
			}
			if err = phase(neo4jspec.PhaseUploading); err != nil {
				cancel()
				_ = cmd.Wait()
				return err
			}
			h := sha256.New()
			counter := &countedReader{r: io.TeeReader(stdout, h)}
			key := q.Target.Prefix + "/" + db + neo4jspec.DumpSuffix
			_, uploadErr := client.PutObject(ctx, q.Target.Bucket, key, counter, -1, minio.PutObjectOptions{ContentType: "application/octet-stream", PartSize: dumpPartSize, NumThreads: 1})
			if uploadErr != nil {
				cancel()
			}
			waitErr := cmd.Wait()
			cancel()
			if uploadErr != nil || waitErr != nil {
				return errors.New("database dump upload failed")
			}
			manifest.Databases = append(manifest.Databases, neo4jspec.DumpObject{Database: db, Key: key, Size: counter.n, SHA256: hex.EncodeToString(h.Sum(nil))})
		}
		body, err := json.Marshal(manifest)
		if err != nil {
			return err
		}
		if _, err = client.PutObject(ctx, q.Target.Bucket, q.Target.Prefix+"/"+neo4jspec.ManifestObject, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{ContentType: "application/json", DisableMultipart: true}); err != nil {
			return errors.New("manifest upload failed")
		}
		r.Manifest = &manifest
		return nil
	}
}
