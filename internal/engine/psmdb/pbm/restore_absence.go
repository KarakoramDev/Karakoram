// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/minio/minio-go/v7"
)

// RestoreAbsenceObserver checks native command age, metadata, locks and storage.
// Its observation is not proof that historical handlers cannot still execute.
type RestoreAbsenceObserver interface {
	ObserveRestoreAbsence(context.Context, string, Env) (int64, bool, error)
}

// restoreAbsent preserves positive member metadata for native-outcome polling;
// only exact not-found from every member permits the remaining absence checks.
func restoreAbsent(ctx context.Context, env Env, name string) (int64, bool, []byte, error) {
	observer, ok := env.Hooks.(RestoreAbsenceObserver)
	if !ok {
		return 0, false, nil, nil
	}
	// Unlike command's availability fallback, absence needs every eligible member.
	count := 0
	for _, node := range env.Nodes {
		if env.Sharded && node.ReplSet != "cfg" {
			continue
		}
		count++
		data, err := commandMember(ctx, env, node.Member, "", "describe-restore", nil, name, "--out=json")
		if err == nil {
			return 0, false, data, nil
		}
		if !restoreMetadataPending(err) {
			return 0, false, nil, err
		}
	}
	if count == 0 {
		return 0, false, nil, errors.New("no eligible PBM restore observers")
	}
	commandTS, absent, err := observer.ObserveRestoreAbsence(ctx, name, env)
	return commandTS, absent, nil, err
}

// RestoreStorageAbsent checks the recorded destination, without modifying it.
func RestoreStorageAbsent(ctx context.Context, env Env, name string) (bool, error) {
	client, closeClient, err := backupstorage.NewClient(env.Storage, env.Credentials)
	if err != nil {
		return false, err
	}
	defer closeClient()
	prefix := env.Prefix
	if prefix == "" {
		prefix = env.Storage.Prefix
	}
	if err := domain.ValidateBackupStoragePrefix(prefix); err != nil {
		return false, err
	}
	prefix = domain.BackupObjectPrefix(prefix, ".pbm.restore") + "/"
	// Drain the paginated iterator: a partial listing is never evidence of absence.
	for object := range client.ListObjectsIter(ctx, env.Storage.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if object.Err != nil {
			return false, fmt.Errorf("restore storage observation unavailable")
		}
		if !strings.HasPrefix(object.Key, prefix) {
			return false, errors.New("restore storage returned an out-of-prefix object")
		}
		relative := strings.TrimPrefix(object.Key, prefix)
		if relative == name+".json" || strings.HasPrefix(relative, name+"/") {
			return false, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}

// ResetRestoreAbsence invalidates a poll's previous proof and any legacy
// verification cursor when an attempt cannot reach a complete observation.
func ResetRestoreAbsence(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return raw, nil
	}
	var cp checkpoint
	if err := json.Unmarshal(raw, &cp); err != nil {
		return nil, fmt.Errorf("reset restore absence checkpoint: %w", err)
	}
	if cp.Step == "verify-fence" && cp.Index != 0 {
		cp.Index = 0
		return json.Marshal(cp)
	}
	if cp.Step != "poll" || cp.RestoreAbsentSince == nil {
		return raw, nil
	}
	cp.RestoreAbsentSince = nil
	return json.Marshal(cp)
}

func restoreFenceSupported(ctx context.Context, env Env) error {
	for _, node := range env.Nodes {
		client, err := env.RC.Agents.Dial(ctx, env.RC.Cluster.Ref(), node.Member)
		if err != nil {
			return err
		}
		status, err := engine.ObserveAgentStatus(ctx, client, "PSMDB", node.Member)
		client.Close()
		if err != nil {
			return err
		}
		if !status.PBMRestoreFence {
			return errors.New("PBM restore outcome unknown: member lacks supervised restart-fence capability; preserve restore holds")
		}
	}
	return nil
}

func fenceRestoreMember(ctx context.Context, env Env, operationID string, commandTS int64, member string) error {
	if operationID == "" || commandTS <= 0 {
		return errors.New("PBM restore fencing requires a durable operation and native command timestamp")
	}
	client, err := env.RC.Agents.Dial(ctx, env.RC.Cluster.Ref(), member)
	if err != nil {
		return err
	}
	defer client.Close()
	_, err = client.FencePBMRestore(ctx, operationID+"/pbm-fence/"+member, commandTS)
	return err
}
