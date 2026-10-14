// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package version

import (
	"strings"
	"testing"
)

func TestBuildMetadataLabels(t *testing.T) {
	oldVersion, oldCommit := Version, Commit
	t.Cleanup(func() { Version, Commit = oldVersion, oldCommit })
	for _, tc := range []struct{ version, commit, wantVersion, wantCommit string }{
		{"dev", "unknown", unknownMetadata, unknownMetadata},
		{"", "", unknownMetadata, unknownMetadata},
		{"1.2.3", "abcdef", "1.2.3", "abcdef"},
		{"dev", "abcdef", "dev", "abcdef"},
	} {
		Version, Commit = tc.version, tc.commit
		if VersionLabel() != tc.wantVersion || CommitLabel() != tc.wantCommit {
			t.Fatalf("labels for %q/%q = %q/%q", Version, Commit, VersionLabel(), CommitLabel())
		}
		if !strings.Contains(String(), tc.wantVersion) || !strings.Contains(String(), tc.wantCommit) {
			t.Fatal(String())
		}
	}
}
