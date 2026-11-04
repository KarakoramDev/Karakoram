// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"path/filepath"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type preparedFile struct {
	Path      string `json:"path"`
	Directory bool   `json:"directory,omitempty"`
	Mode      uint32 `json:"mode"`
	Size      int64  `json:"size,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
}

func preparedPhysicalFiles(root string) ([]preparedFile, error) {
	var files []preparedFile
	err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if name == root {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return errors.New("physical_staging_file_invalid")
		}
		relative, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		file := preparedFile{Path: filepath.ToSlash(relative), Directory: info.IsDir(), Mode: uint32(info.Mode().Perm())}
		if !file.Directory {
			file.SHA256, file.Size, err = hashFile(name)
			if err != nil {
				return err
			}
		}
		files = append(files, file)
		return nil
	})
	return files, err
}

func physicalFilesMatch(root string, expected []preparedFile) error {
	actual, err := preparedPhysicalFiles(root)
	if err != nil {
		return err
	}
	if len(expected) == 0 || len(actual) != len(expected) {
		return errors.New("physical_staging_set_changed")
	}
	for i, file := range actual {
		if file != expected[i] {
			return errors.New("physical_staging_checksum_changed")
		}
	}
	return nil
}

func frozenRestorePlanDigest(receipt restoreReceipt) (string, error) {
	if receipt.Replay == nil {
		return "", errors.New("replay_plan_missing")
	}
	raw, err := json.Marshal(struct {
		RequestHash string              `json:"requestHash"`
		Backup      myspec.BackupResult `json:"backup"`
		Replay      *restoreReplayPlan  `json:"replay"`
	}{receipt.Hash, receipt.Backup, receipt.Replay})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func verifyFrozenRestorePlan(receipt restoreReceipt, stage string, physical bool) error {
	digest, err := frozenRestorePlanDigest(receipt)
	if err != nil || !myspec.ValidDigest(receipt.Status.PlanDigest) || digest != receipt.Status.PlanDigest {
		return errors.New("restore_plan_digest_changed")
	}
	if err = verifyReplayFiles(receipt.Replay.Logs, stage); err != nil {
		return err
	}
	if physical {
		return physicalFilesMatch(filepath.Join(stage, "0"), receipt.Replay.Physical)
	}
	return nil
}

func requireFrozenPlanDigest(request myspec.RestoreRequest, receipt restoreReceipt, preflight bool) error {
	if preflight && request.PlanDigest == "" {
		return nil
	}
	if !myspec.ValidDigest(request.PlanDigest) || request.PlanDigest != receipt.Status.PlanDigest {
		return errors.New("restore_plan_digest_mismatch")
	}
	return nil
}
