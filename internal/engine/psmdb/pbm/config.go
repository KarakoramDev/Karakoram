// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package pbm orchestrates durable Percona Backup for MongoDB operations through node agents.
package pbm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"gopkg.in/yaml.v3"
)

// Node identifies a mongod running a PBM agent. Config-server nodes must be listed first for sharded clusters.
type Node struct{ Member, ReplSet string }

// Hooks performs authenticated database administration around restores.
type Hooks interface {
	ResetSystemUsers(context.Context, string) error
	SetBalancer(context.Context, bool) error
}

// Env supplies a cluster's PBM topology, storage and orchestration dependencies.
type Env struct {
	RC          *engine.Context
	Nodes       []Node
	Routers     []string
	Sharded     bool
	Storage     domain.BackupStorage
	Credentials domain.StorageCredentials
	Prefix      string
	PITR        domain.PITRConfig
	Hooks       Hooks
}

// RenderConfig renders PBM's bounded-memory S3 configuration. Callers gate PITR before applying it.
func RenderConfig(env Env) ([]byte, error) {
	if env.Storage.Type != "s3" {
		return nil, fmt.Errorf("PBM storage type %q: only s3 is supported", env.Storage.Type)
	}
	if env.Storage.EndpointURL == "" || env.Storage.Bucket == "" {
		return nil, errors.New("PBM S3 endpoint and bucket are required")
	}
	if env.Credentials.AccessKeyID == "" || env.Credentials.SecretAccessKey == "" {
		return nil, errors.New("PBM S3 credentials are required")
	}
	region := env.Storage.Region
	if region == "" {
		region = "us-east-1"
	}
	span := env.PITR.OplogSpanMinutes
	if span <= 0 {
		span = 1
	}
	prefix := env.Prefix
	if prefix == "" {
		prefix = env.Storage.Prefix
	}
	cfg := struct {
		Storage struct {
			Type string `yaml:"type"`
			S3   struct {
				Endpoint    string `yaml:"endpointUrl"`
				Region      string `yaml:"region"`
				Bucket      string `yaml:"bucket"`
				Prefix      string `yaml:"prefix"`
				PathStyle   bool   `yaml:"forcePathStyle"`
				Insecure    bool   `yaml:"insecureSkipTLSVerify"`
				Credentials struct {
					Access string `yaml:"access-key-id"`
					Secret string `yaml:"secret-access-key"`
				} `yaml:"credentials"`
			} `yaml:"s3"`
		} `yaml:"storage"`
		PITR struct {
			Enabled bool `yaml:"enabled"`
			Span    int  `yaml:"oplogSpanMin"`
		} `yaml:"pitr"`
		Backup struct {
			Compression string `yaml:"compression"`
		} `yaml:"backup"`
		Restore struct {
			Workers int `yaml:"numDownloadWorkers"`
			Buffer  int `yaml:"maxDownloadBufferMb"`
			Chunk   int `yaml:"downloadChunkMb"`
		} `yaml:"restore"`
	}{}
	cfg.Storage.Type = "s3"
	cfg.Storage.S3.Endpoint, cfg.Storage.S3.Region, cfg.Storage.S3.Bucket, cfg.Storage.S3.Prefix = env.Storage.EndpointURL, region, env.Storage.Bucket, prefix
	cfg.Storage.S3.PathStyle, cfg.Storage.S3.Insecure = env.Storage.ForcePathStyle, !env.Storage.VerifyTLS
	cfg.Storage.S3.Credentials.Access, cfg.Storage.S3.Credentials.Secret = env.Credentials.AccessKeyID, env.Credentials.SecretAccessKey
	cfg.PITR.Enabled, cfg.PITR.Span = env.PITR.Enabled, span
	cfg.Backup.Compression = "s2"
	cfg.Restore.Workers, cfg.Restore.Buffer, cfg.Restore.Chunk = 1, 64, 8
	return yaml.Marshal(cfg)
}

// EnsureConfig applies a changed configuration on a healthy node, enabling PITR only with a successful base snapshot.
func EnsureConfig(ctx context.Context, env Env, appliedHash string) (string, error) {
	desiredPITR := env.PITR.Enabled
	if desiredPITR && appliedHash == "" {
		// PBM list itself requires a configuration. Install the destination with
		// PITR off before consulting the first base or a changed destination.
		disabled := env
		disabled.PITR.Enabled = false
		if _, err := EnsureConfig(ctx, disabled, ""); err != nil {
			return "", err
		}
	}
	if desiredPITR {
		inv, err := inventory(ctx, env)
		if err != nil {
			return "", err
		}
		env.PITR.Enabled = false
		for _, b := range inv.Snapshots {
			if b.Status == "done" {
				env.PITR.Enabled = true
				break
			}
		}
	}
	cfg, err := RenderConfig(env)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(cfg)
	hash := hex.EncodeToString(sum[:])
	if hash == appliedHash {
		return hash, nil
	}
	member, err := healthyMember(ctx, env)
	if err != nil {
		return "", err
	}
	if _, err := command(ctx, env, member, "", "config", cfg, "--file"); err != nil {
		return "", err
	}
	return hash, nil
}

// recordedEnv resolves immutable backup provenance without a source cluster.
func recordedEnv(ctx context.Context, env Env, namespace, storage, prefix string) (Env, error) {
	s, err := env.RC.Store.GetBackupStorage(ctx, namespace, storage)
	if err != nil {
		return Env{}, fmt.Errorf("read recorded storage: %w", err)
	}
	env.Storage, env.Credentials, env.Prefix = *s, s.Credentials, prefix
	env.PITR.Enabled = false
	return env, nil
}

func sourceEnv(ctx context.Context, env Env, source domain.ClusterRef, storage string) (Env, error) {
	c, err := env.RC.Store.GetCluster(ctx, source)
	if err != nil {
		return Env{}, fmt.Errorf("read source cluster: %w", err)
	}
	if storage == "" {
		storage = c.Spec.Backup.Storage
	}
	s, err := env.RC.Store.GetBackupStorage(ctx, source.Namespace, storage)
	if err != nil {
		return Env{}, fmt.Errorf("read source storage: %w", err)
	}
	var state struct {
		UID    string `json:"uid"`
		Prefix string `json:"pbmPrefix"`
	}
	if err := json.Unmarshal(c.Internal, &state); err != nil {
		return Env{}, fmt.Errorf("read source PBM prefix: %w", err)
	}
	if state.Prefix == "" {
		if state.UID == "" {
			return Env{}, errors.New("source cluster has no durable PBM prefix or UID")
		}
		state.Prefix = "karakoram/" + source.Name + "-" + state.UID
	}
	env.Storage, env.Credentials, env.Prefix = *s, s.Credentials, state.Prefix
	if s.Prefix != "" {
		env.Prefix = strings.TrimRight(s.Prefix, "/") + "/" + env.Prefix
	}
	env.PITR.Enabled = false
	return env, nil
}

func alreadyGone(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "not found") || strings.Contains(text, "does not exist") || strings.Contains(text, "no such backup")
}
