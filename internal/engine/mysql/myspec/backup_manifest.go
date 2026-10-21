// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// BackupObject uses the same key/size/SHA-256 integrity convention as archived
// binlog sidecars. VersionID pins immutable object versions when S3 provides one.
type BackupObject struct {
	Key       string `json:"key"`
	VersionID string `json:"versionId,omitempty"`
	Size      int64  `json:"size"`
	SHA256    string `json:"sha256"`
}

type BackupObjectManifest struct {
	Version      int            `json:"version"`
	ObjectPrefix string         `json:"objectPrefix"`
	Objects      []BackupObject `json:"objects"`
}

// BackupManifestRef is sealed with managed-account recovery provenance. The
// repository manifest is not trusted merely because it is stored beside data.
type BackupManifestRef struct {
	ObjectPrefix string `json:"objectPrefix"`
	Digest       string `json:"digest"`
	Size         int64  `json:"size"`
}

func ValidDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (m BackupObjectManifest) Validate() error {
	if m.Version != 1 || !safeObject(m.ObjectPrefix) || len(m.Objects) == 0 {
		return fmt.Errorf("invalid physical backup manifest")
	}
	previous := ""
	for _, object := range m.Objects {
		if !strings.HasPrefix(object.Key, m.ObjectPrefix+"/") || object.Key <= previous || object.Size < 0 || !ValidDigest(object.SHA256) {
			return fmt.Errorf("invalid physical backup object")
		}
		previous = object.Key
	}
	return nil
}

func (m BackupObjectManifest) Canonical() ([]byte, string, error) {
	if err := m.Validate(); err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:]), nil
}

func ValidateBackupManifestRefs(paths []string, refs []BackupManifestRef) error {
	if len(paths) == 0 || len(paths) != len(refs) {
		return fmt.Errorf("restore requires sealed physical object manifests for every backup")
	}
	for i, prefix := range paths {
		if refs[i].ObjectPrefix != prefix || !ValidDigest(refs[i].Digest) || refs[i].Size <= 0 {
			return fmt.Errorf("restore physical manifest provenance mismatch")
		}
	}
	return nil
}
