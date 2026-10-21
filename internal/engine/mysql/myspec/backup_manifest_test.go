// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import (
	"bytes"
	"testing"
)

const manifestTestDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestPhysicalBackupManifestCanonicalIntegrity(t *testing.T) {
	manifest := BackupObjectManifest{Version: 1, ObjectPrefix: "repo/base/full", Objects: []BackupObject{{Key: "repo/base/full/ibdata1.00000000000000000000", VersionID: "immutable-version", Size: 42, SHA256: manifestTestDigest}}}
	raw, digest, err := manifest.Canonical()
	if err != nil || !ValidDigest(digest) {
		t.Fatalf("manifest digest=%s err=%v", digest, err)
	}
	again, same, err := manifest.Canonical()
	if err != nil || same != digest || !bytes.Equal(raw, again) {
		t.Fatal("unstable canonical manifest")
	}
	changed := manifest
	changed.Objects = append([]BackupObject{}, manifest.Objects...)
	changed.Objects[0].VersionID = "replacement-version"
	if _, other, err := changed.Canonical(); err != nil || other == digest {
		t.Fatal("version substitution did not change sealed digest")
	}
	changed.Objects[0].Key = "other/base/full/ibdata1.00000000000000000000"
	if _, _, err := changed.Canonical(); err == nil {
		t.Fatal("object escaped sealed prefix")
	}
	manifest.Objects = append(manifest.Objects, manifest.Objects[0])
	if _, _, err := manifest.Canonical(); err == nil {
		t.Fatal("duplicate/unsorted manifest accepted")
	}
}

func TestRestoreRequiresEverySealedPhysicalManifest(t *testing.T) {
	paths := []string{"repo/base/full", "repo/base/inc"}
	refs := []BackupManifestRef{{ObjectPrefix: paths[0], Digest: manifestTestDigest, Size: 1}, {ObjectPrefix: paths[1], Digest: manifestTestDigest, Size: 1}}
	if err := ValidateBackupManifestRefs(paths, refs); err != nil {
		t.Fatal(err)
	}
	if err := ValidateBackupManifestRefs(paths, refs[:1]); err == nil {
		t.Fatal("missing incremental manifest accepted")
	}
	refs[1].ObjectPrefix = "repo/base/another"
	if err := ValidateBackupManifestRefs(paths, refs); err == nil {
		t.Fatal("substituted chain prefix accepted")
	}
}
