// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// DeleteAll removes PITR chunks before snapshots, one snapshot per reconciliation. It merges a durable
// pbmCleanup checkpoint into Cluster.Internal; the provider must persist Internal before calling again.
func DeleteAll(ctx context.Context, env Env) (engine.OpResult, error) {
	state := map[string]json.RawMessage{}
	if len(env.RC.Cluster.Internal) != 0 {
		if err := json.Unmarshal(env.RC.Cluster.Internal, &state); err != nil {
			return engine.OpResult{}, err
		}
	}
	var cp checkpoint
	if raw := state["pbmCleanup"]; len(raw) != 0 {
		if err := json.Unmarshal(raw, &cp); err != nil {
			return engine.OpResult{}, err
		}
	}
	prefix := env.Prefix
	if prefix == "" {
		prefix = env.Storage.Prefix
	}
	sum := sha256.Sum256([]byte(env.Storage.Name + "\x00" + env.Storage.EndpointURL + "\x00" + env.Storage.Bucket + "\x00" + prefix))
	destination := hex.EncodeToString(sum[:])
	if cp.Destination != destination {
		cp = checkpoint{Destination: destination}
	}
	finish := func(done bool, err error) (engine.OpResult, error) {
		res := result(cp, done)
		state["pbmCleanup"] = res.Checkpoint
		env.RC.Cluster.Internal, _ = json.Marshal(state)
		return res, err
	}
	switch cp.Step {
	case "":
		disabled := env
		disabled.PITR.Enabled = false
		cfg, err := RenderConfig(disabled)
		if err != nil {
			return finish(false, err)
		}
		_, err = command(ctx, env, "", "", "config", cfg, "--file")
		if err != nil {
			return finish(false, err)
		}
		cp.Step = "resync"
	case "resync":
		_, err := command(ctx, env, "", "", "config", nil, "--force-resync")
		if err != nil {
			return finish(false, err)
		}
		cp.Step = "inventory"
	case "inventory":
		done, err := resyncDone(ctx, env, "")
		if err != nil {
			return finish(false, err)
		}
		if !done {
			return finish(false, nil)
		}
		cp.Step = "delete-pitr"
	case "delete-pitr":
		_, err := command(ctx, env, "", "", "delete-pitr", nil, "--all", "--yes")
		if err != nil && !alreadyGone(err) {
			return finish(false, err)
		}
		cp.Step = "observe-pitr"
	case "observe-pitr":
		inv, err := inventory(ctx, env)
		if err != nil {
			return finish(false, err)
		}
		if len(inv.PITR.Ranges) != 0 {
			return finish(false, nil)
		}
		cp.Step = "snapshots"
	case "snapshots":
		inv, err := inventory(ctx, env)
		if err != nil {
			return finish(false, err)
		}
		if len(inv.Snapshots) == 0 {
			cp.Step = "done"
			return finish(true, nil)
		}
		cp.PBMName, cp.Step = inv.Snapshots[0].Name, "delete-snapshot"
	case "delete-snapshot":
		_, err := command(ctx, env, "", "", "delete-backup", nil, cp.PBMName, "--yes")
		if err != nil && !alreadyGone(err) {
			return finish(false, err)
		}
		cp.Step = "observe-snapshot"
	case "observe-snapshot":
		inv, err := inventory(ctx, env)
		if err != nil {
			return finish(false, err)
		}
		for _, s := range inv.Snapshots {
			if s.Name == cp.PBMName {
				return finish(false, nil)
			}
		}
		records, err := env.RC.Store.ListBackups(ctx, env.RC.Cluster.Ref())
		if err != nil {
			return finish(false, err)
		}
		for _, b := range records {
			if b.PBMName == cp.PBMName && b.Storage == env.Storage.Name {
				if b.RecoveryIdentity != "" {
					if err := env.RC.Secrets.DeleteClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/userAdmin"); err != nil && !errors.Is(err, engine.ErrNotFound) && !errors.Is(err, store.ErrNotFound) {
						return finish(false, err)
					}
				}
				if err := env.RC.Store.DeleteBackup(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}, b.Name); err != nil {
					return finish(false, err)
				}
			}
		}
		cp.Step, cp.PBMName = "snapshots", ""
	case "done":
		return finish(true, nil)
	default:
		return finish(false, fmt.Errorf("unknown PBM cleanup step %q", cp.Step))
	}
	return finish(false, nil)
}
