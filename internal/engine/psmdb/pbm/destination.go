// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

func primaryEnv(ctx context.Context, env Env) (Env, error) {
	name := env.RC.Cluster.Spec.Backup.Storage
	if name == "" {
		return env, nil
	}
	storage, err := env.RC.Store.GetBackupStorage(ctx, name)
	if err != nil {
		return Env{}, err
	}
	var state struct {
		Prefix string `json:"pbmPrefix"`
	}
	if err := json.Unmarshal(env.RC.Cluster.Internal, &state); err != nil {
		return Env{}, err
	}
	if state.Prefix == "" {
		return Env{}, errors.New("target cluster has no durable PBM prefix")
	}
	env.Storage, env.Credentials, env.Prefix = *storage, storage.Credentials, state.Prefix
	if storage.Prefix != "" {
		env.Prefix = strings.TrimRight(storage.Prefix, "/") + "/" + env.Prefix
	}
	env.PITR = env.RC.Cluster.Spec.Backup.PITR
	return env, nil
}

// Reestablish the primary's inventory before letting ordinary status/PITR run.
// Each resync is asynchronous, with a durable initiation/observation boundary.
func reconcilePrimary(ctx context.Context, env Env, cp *checkpoint) (bool, error) {
	target, err := primaryEnv(ctx, env)
	if err != nil {
		return false, err
	}
	if target.Storage.Name == env.Storage.Name && target.Prefix == env.Prefix {
		return true, nil
	}
	switch cp.Step {
	case "target-configure":
		disabled := target
		disabled.PITR.Enabled = false
		cfg, err := RenderConfig(disabled)
		if err != nil {
			return false, err
		}
		if _, err = command(ctx, target, "", "", "config", cfg, "--file"); err != nil {
			return false, err
		}
		cp.Step = "target-resync"
	case "target-resync":
		if _, err = command(ctx, target, "", "", "config", nil, "--force-resync"); err != nil {
			return false, err
		}
		cp.Step = "target-inventory"
	case "target-inventory":
		done, err := resyncDone(ctx, target, "")
		if err != nil || !done {
			return false, err
		}
		if _, err = EnsureConfig(ctx, target, ""); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}
