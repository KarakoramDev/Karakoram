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
	// RequireFresh waits for verification started after this admission request.
	RequireFresh bool `json:"requireFresh,omitempty"`
	// AcceptPending negotiates progress responses; absent means pending is an error.
	AcceptPending bool `json:"acceptPending,omitempty"`
}

type PITRWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type PITRCoverageResponse struct {
	Pending               bool                  `json:"pending,omitempty"`
	Progress              *PITRCoverageProgress `json:"progress,omitempty"`
	VerificationStartedAt time.Time             `json:"verificationStartedAt,omitzero"`
	VerifiedAt            time.Time             `json:"verifiedAt,omitzero"`
	Fresh                 bool                  `json:"fresh,omitempty"`
	Windows               []PITRWindow          `json:"windows"`
	// SourceUUID is read from the verified baseline, not backup inventory.
	SourceUUID string `json:"sourceUUID,omitempty"`
	// LatestForeignEventTime is diagnostic only: transactions archived by a
	// different physical source cannot extend this baseline's replay lineage.
	LatestForeignEventTime time.Time `json:"latestForeignEventTime,omitzero"`
}

type PITRCoverageProgress struct {
	Stage     string `json:"stage"`
	Processed int    `json:"processed"`
	Total     int    `json:"total"`
}
