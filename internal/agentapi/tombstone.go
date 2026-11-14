// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agentapi

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ResolutionTombstoneStatus reports local installation and native resolver proof.
type ResolutionTombstoneStatus struct {
	Writer            string    `json:"writer,omitempty"`
	Supported         bool      `json:"supported"`
	Reason            string    `json:"reason,omitempty"`
	Hosts             []string  `json:"hosts,omitempty"`
	VerifiedAt        time.Time `json:"verifiedAt,omitempty"`
	SetHash           string    `json:"setHash,omitempty"`
	Generation        string    `json:"generation,omitempty"`
	ProcessGeneration string    `json:"processGeneration,omitempty"`
}

// ResolutionSetHash identifies the complete canonical desired resolver set.
func ResolutionSetHash(hosts []string) string {
	set := slices.Clone(hosts)
	slices.Sort(set)
	sum := sha256.Sum256([]byte(strings.Join(slices.Compact(set), "\n")))
	return hex.EncodeToString(sum[:])
}

// ResolutionProcessGeneration binds proof to the actual supervised native launch.
func ResolutionProcessGeneration(p ProcessStatus) string {
	return fmt.Sprintf("%s/%d/%d/%v", p.SupervisorSession, p.Launch, p.PID, p.StartedAt)
}

const ResolutionProofLease = 7 * time.Second
