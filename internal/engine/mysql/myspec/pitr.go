// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import "time"

// PITRCoverageRequest asks an agent to prove archived coverage from the bytes of
// an immutable physical backup, not from a union of numeric GTID intervals.
type PITRCoverageRequest struct {
	Source          Repository          `json:"source"`
	ObjectPaths     []string            `json:"objectPaths"`
	ObjectManifests []BackupManifestRef `json:"objectManifests"`
}

type PITRWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type PITRCoverageResponse struct {
	Windows []PITRWindow `json:"windows"`
}
