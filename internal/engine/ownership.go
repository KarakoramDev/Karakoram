// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// OwnerVariable is the non-sensitive ownership marker written on every member
// service. Tokens without sudo mode can read it, unlike the agent token secret.
const OwnerVariable = "KARAKORAM_OWNER"

// OwnerMarker derives a service's ownership marker: an HMAC-SHA256 keyed by
// the member's random agent token over the cluster, member and service
// hostname. It reveals nothing about the token and cannot be produced for
// another service without it; a hostname alone is never ownership evidence.
func OwnerMarker(agentToken, cluster, member, hostname string) string {
	mac := hmac.New(sha256.New, []byte(agentToken))
	mac.Write([]byte("karakoram-owner-v1\x00" + cluster + "\x00" + member + "\x00" + hostname))
	return "v1." + hex.EncodeToString(mac.Sum(nil))
}

// OwnerVars returns the non-sensitive import variables for one member service.
func OwnerVars(agentToken, cluster, member, hostname string) map[string]string {
	return map[string]string{OwnerVariable: OwnerMarker(agentToken, cluster, member, hostname)}
}

// HasOwnership reports whether a service carries member ownership evidence:
// its non-sensitive marker, or the legacy agent-token secret when the token
// can read sensitive values. Masked values are absent from Ownership.
func HasOwnership(s *infra.Service, agentToken, cluster, member string) bool {
	if s == nil || agentToken == "" {
		return false
	}
	o := s.Ownership
	want := OwnerMarker(agentToken, cluster, member, s.Hostname)
	if subtle.ConstantTimeCompare([]byte(o[OwnerVariable]), []byte(want)) == 1 {
		return true
	}
	secret, ok := o["KARAKORAM_AGENT_TOKEN"]
	return ok && o["KARAKORAM_CLUSTER"] == cluster && o["KARAKORAM_MEMBER"] == member && subtle.ConstantTimeCompare([]byte(secret), []byte(agentToken)) == 1
}

// UnverifiedOwnership is the conflict for a service without ownership evidence.
// When legacy evidence exists only as masked sensitive values, it tells the
// operator how to recover instead of retrying silently.
func UnverifiedOwnership(s *infra.Service) error {
	if s.Masked && s.Ownership[OwnerVariable] == "" {
		return fmt.Errorf("%w: service %s has no verified cluster ownership: it carries only sensitive ownership variables, which the namespace backend masks for the current credentials, and its ID was never recorded; delete the service in the namespace backend so Karakoram recreates it, or let one reconcile run with credentials that can read sensitive variables (a sudo-mode token on Zerops) to record its ownership", ErrConflict, s.Hostname)
	}
	return fmt.Errorf("%w: service %s has no verified cluster ownership", ErrConflict, s.Hostname)
}

// EnsureOwnerMarker records the non-sensitive marker on a service whose
// ownership was proven by other evidence (recorded ID, own import, readable
// legacy secret), so later checks work without reading sensitive values.
func EnsureOwnerMarker(ctx context.Context, b infra.Backend, s *infra.Service, agentToken, cluster, member string) error {
	if s == nil || agentToken == "" {
		return nil
	}
	want := OwnerMarker(agentToken, cluster, member, s.Hostname)
	if s.Ownership[OwnerVariable] == want {
		return nil
	}
	// Services being created, changed or deleted are left alone; the
	// recorded ID already proves them and a later check writes the marker.
	switch s.Status {
	case "", "ACTIVE", "READY_TO_DEPLOY", "STOPPED":
	default:
		return nil
	}
	if err := b.SetEnvVars(ctx, s.Hostname, map[string]string{OwnerVariable: want}); err != nil {
		return fmt.Errorf("record ownership marker on %s: %w", s.Hostname, err)
	}
	if s.Ownership == nil {
		s.Ownership = map[string]string{}
	}
	s.Ownership[OwnerVariable] = want
	return nil
}

// ImportedServiceID returns the ID a Karakoram import created for hostname, if
// observed s is that service. The IDs come from the import response recorded
// in the operation or from its own platform processes, never from the hostname.
func ImportedServiceID(op infra.Op, st infra.OpState, s *infra.Service) string {
	if s == nil || s.ID == "" {
		return ""
	}
	if id := op.Services[s.Hostname]; id != "" {
		if id == s.ID {
			return id
		}
		return ""
	}
	if slices.Contains(st.ServiceIDs, s.ID) {
		return s.ID
	}
	return ""
}
