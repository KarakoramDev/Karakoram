// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent/tombstone"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type resolutionTombstone struct {
	Host             string            `json:"host"`
	Epoch            string            `json:"epoch"`
	RuntimeID        string            `json:"runtimeId"`
	NativeUUID       string            `json:"nativeUUID"`
	CreatedAt        time.Time         `json:"createdAt"`
	ReleaseNotBefore time.Time         `json:"releaseNotBefore,omitempty"`
	LastVerification time.Time         `json:"lastVerification,omitempty"`
	Unprotected      bool              `json:"unprotected,omitempty"`
	ReleasePending   bool              `json:"releasePending,omitempty"`
	Generations      map[string]string `json:"generations,omitempty"`
}

func resolutionTombstoneHosts(s *state, m *member) []string {
	if m.Role != "data" {
		return nil
	}
	var hosts []string
	for _, t := range s.ResolutionTombstones {
		if t.Host != m.Name && !t.ReleasePending {
			hosts = append(hosts, t.Host)
		}
	}
	slices.Sort(hosts)
	return slices.Compact(hosts)
}

func projectResolutionTombstones(ctx context.Context, rc *engine.Context, s *state) error {
	projector, ok := rc.Backend.(infra.ResolutionTombstoneProjector)
	if !ok {
		return nil
	}
	for _, m := range s.data() {
		if m.ServiceID == "" || m.Retirement == "credentials" ||
			s.Pending != nil && s.Pending.Member == m.Name && s.Pending.Action == "delete-runtime" {
			continue
		}
		if err := projector.ProjectResolutionTombstones(ctx, m.Name, resolutionTombstoneHosts(s, m)); err != nil {
			return fmt.Errorf("project resolver tombstones on %s: %w", m.Name, err)
		}
	}
	return nil
}

func gateResolutionTombstone(ctx context.Context, rc *engine.Context, s *state, m *member) (bool, error) {
	if s.Topology != "group" || !m.ConfirmedVolumeLost || m.Role != "data" {
		return true, nil
	}
	var journal *resolutionTombstone
	for i := range s.ResolutionTombstones {
		t := &s.ResolutionTombstones[i]
		if t.Host == m.Name && t.RuntimeID == m.ServiceID {
			journal = t
			break
		}
	}
	if journal == nil {
		if !tombstone.ValidHost(m.Name) {
			return false, engine.Invalid("invalid retired hostname %q", m.Name)
		}
		var epoch [16]byte
		if _, err := rand.Read(epoch[:]); err != nil {
			return false, err
		}
		s.ResolutionTombstones = append(s.ResolutionTombstones, resolutionTombstone{Host: m.Name, Epoch: hex.EncodeToString(epoch[:]), RuntimeID: m.ServiceID, NativeUUID: m.ServerUUID, CreatedAt: now(rc)})
		publishRevision(s)
		rc.SyncWait = "Persisting retired member resolver tombstone before runtime stop"
		// Return to the reconciler's durable state checkpoint before any projection
		// or Stop. A process crash here can only replay installation, never skip it.
		return false, nil
	}
	if err := projectResolutionTombstones(ctx, rc, s); err != nil {
		return false, err
	}
	if journal.Unprotected {
		return true, nil
	}
	var unsupported, waiting []string
	for _, peer := range s.data() {
		if peer.Name == m.Name || peer.Offline || peer.ServiceID == "" {
			continue
		}
		status, _, err := mysqlAgentStatus(ctx, rc, peer.Name)
		if err != nil {
			return false, err
		}
		proof := status.ResolutionTombstones
		if proof == nil || !proof.Supported {
			unsupported = append(unsupported, peer.Name)
			continue
		}
		if !currentResolutionProof(status, resolutionTombstoneHosts(s, peer), now(rc)) || !proof.VerifiedAt.After(journal.CreatedAt) {
			waiting = append(waiting, peer.Name)
			continue
		}
		if proof.VerifiedAt.After(journal.LastVerification) {
			journal.LastVerification = proof.VerifiedAt
		}
		recordResolutionGeneration(journal, peer.Name, proof)
	}
	if len(unsupported) > 0 {
		slices.Sort(unsupported)
		message := fmt.Sprintf("Retirement of %s is unprotected on survivors %s; commits may stall for ~5 min", m.Name, strings.Join(unsupported, ", "))
		if rc.Events != nil {
			if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Resource: m.Name, Type: "Warning", Reason: "RetirementUnprotected", Message: message, CreatedAt: now(rc)}); err != nil {
				return false, err
			}
		}
		journal.Unprotected, s.RetirementUnprotected = true, message
		// The unprotected decision must itself cross a durable checkpoint before Stop.
		rc.SyncWait = message
		return false, nil
	}
	if len(waiting) > 0 {
		rc.SyncWait = "Waiting for resolver tombstone verification on " + strings.Join(waiting, ", ")
		for _, name := range waiting {
			agent, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
			if err != nil {
				return false, err
			}
			if err := agent.Reload(ctx); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	return true, nil
}

func holdResolutionTombstone(rc *engine.Context, s *state, m *member) {
	for i := range s.ResolutionTombstones {
		t := &s.ResolutionTombstones[i]
		if t.Host == m.Name && t.ReleaseNotBefore.IsZero() {
			t.ReleaseNotBefore = maxTime(now(rc), t.LastVerification).Add(600 * time.Second)
		}
	}
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func reconcileResolutionTombstones(ctx context.Context, rc *engine.Context, s *state) error {
	if len(s.ResolutionTombstones) == 0 || rc.Cluster.Deleting {
		return nil
	}
	at := now(rc)
	peers := s.data()
	if len(peers) == 0 {
		return nil
	}
	ctx = freshMySQLReads(ctx)
	// Cancellation must cross a durable checkpoint before the full set is
	// projected again, including before a new member's first deployment.
	if rearmed, deferred := rearmPendingResolutionTombstones(ctx, rc, s, peers, at); rearmed || deferred {
		return nil
	}
	if err := projectResolutionTombstones(ctx, rc, s); err != nil {
		return err
	}
	writer := false
	allReduced := true
	for _, m := range peers {
		if m.Stage != "joined" || m.Removing || m.Offline || m.ConfirmedVolumeLost || m.ServiceID == "" {
			return nil
		}
		ok, err := ready(ctx, rc, m)
		if err != nil || !ok {
			return nil
		}
		native, err := live(ctx, rc, m.Name)
		if err != nil {
			return nil
		}
		writer = writer || onlinePrimary(native)
		status, _, err := mysqlAgentStatus(ctx, rc, m.Name)
		if err != nil {
			return nil
		}
		proof := status.ResolutionTombstones
		ownsEntries := proof != nil && proof.Supported
		proofOK := currentResolutionProof(status, resolutionTombstoneHosts(s, m), at)
		allReduced = allReduced && (!ownsEntries || proofOK)
		for i := range s.ResolutionTombstones {
			t := &s.ResolutionTombstones[i]
			if !t.ReleasePending && ownsEntries && proofOK {
				recordResolutionGeneration(t, m.Name, proof)
			} else if !t.ReleasePending && !ownsEntries {
				if t.Generations == nil {
					t.Generations = map[string]string{}
				}
				t.Generations[m.Name] = "unsupported"
			}
		}
	}
	if !writer {
		return nil
	}
	if !allReduced {
		rc.SyncWait = "Waiting for current resolver set acknowledgements before release"
		return nil
	}
	changed := false
	for i := range s.ResolutionTombstones {
		t := &s.ResolutionTombstones[i]
		if !t.ReleasePending && !t.ReleaseNotBefore.IsZero() && !at.Before(t.ReleaseNotBefore) {
			// Persist release intent and publish the reduced set first. The next
			// pass projects it, then waits for exact-set acknowledgements.
			t.ReleasePending, changed = true, true
		}
	}
	if changed {
		publishRevision(s)
		return nil
	}
	if allReduced {
		s.ResolutionTombstones = slices.DeleteFunc(s.ResolutionTombstones, func(t resolutionTombstone) bool { return t.ReleasePending })
	}
	return nil
}

// A release-pending journal retains its qualified cohort. A new member or
// verified process generation invalidates that cohort and restores the full
// set before any further projection or provisioning can occur.
// The second result defers release (including projection) without preventing
// the caller from recovering runtimes or settling retirement receipts.
func rearmPendingResolutionTombstones(ctx context.Context, rc *engine.Context, s *state, peers []*member, at time.Time) (bool, bool) {
	if !slices.ContainsFunc(s.ResolutionTombstones, func(t resolutionTombstone) bool { return t.ReleasePending }) {
		return false, false
	}
	rearms := make([]time.Time, len(s.ResolutionTombstones))
	membershipChanged := false
	for i, t := range s.ResolutionTombstones {
		if !t.ReleasePending {
			continue
		}
		for _, m := range peers {
			if _, known := t.Generations[m.Name]; !known {
				rearms[i], membershipChanged = at, true
			}
		}
	}
	deferred := false
	if !membershipChanged {
		for _, m := range peers {
			if m.Stage != "joined" || m.Removing || m.Offline || m.ConfirmedVolumeLost || m.ServiceID == "" {
				deferred = true
				continue
			}
			status, _, err := mysqlAgentStatus(ctx, rc, m.Name)
			if err != nil {
				deferred = true
				continue
			}
			if !currentResolutionProof(status, resolutionTombstoneHosts(s, m), at) {
				continue
			}
			for i, t := range s.ResolutionTombstones {
				if t.ReleasePending && t.Generations[m.Name] != status.ResolutionTombstones.Generation {
					rearms[i] = maxTime(rearms[i], status.ResolutionTombstones.VerifiedAt)
				}
			}
		}
	}
	rearmed := false
	for i, verified := range rearms {
		if verified.IsZero() {
			continue
		}
		t := &s.ResolutionTombstones[i]
		t.ReleasePending = false
		t.ReleaseNotBefore = maxTime(t.ReleaseNotBefore, verified.Add(600*time.Second))
		// Keep the old cohort identity until the restored full set is
		// verified. That qualification can extend the deadline further.
		rearmed = true
	}
	if rearmed {
		publishRevision(s)
		rc.SyncWait = "Requalifying resolver retirement hold after survivor or process change"
	}
	return rearmed, deferred
}

func currentResolutionProof(status agentapi.Status, hosts []string, at time.Time) bool {
	p := status.ResolutionTombstones
	return p != nil && p.Supported && p.Reason == "" &&
		p.SetHash == agentapi.ResolutionSetHash(hosts) && p.Generation != "" &&
		p.ProcessGeneration == agentapi.ResolutionProcessGeneration(status.Processes["mysqld"]) &&
		!p.VerifiedAt.IsZero() && !p.VerifiedAt.After(at.Add(time.Second)) &&
		at.Sub(p.VerifiedAt) <= agentapi.ResolutionProofLease
}

func recordResolutionGeneration(t *resolutionTombstone, member string, p *agentapi.ResolutionTombstoneStatus) {
	if t.Generations == nil {
		t.Generations = map[string]string{}
	}
	if t.Generations[member] != p.Generation {
		t.Generations[member] = p.Generation
		// A fresh survivor/native launch gets its own full hold. Extend only;
		// ordinary heartbeat reverification of the same generation never rearms.
		if !t.ReleaseNotBefore.IsZero() {
			t.ReleaseNotBefore = maxTime(t.ReleaseNotBefore, p.VerifiedAt.Add(600*time.Second))
		}
	}
}

func applyResolutionTombstoneStatus(rc *engine.Context, s *state, status *domain.ClusterStatus) {
	if len(s.ResolutionTombstones) > 0 {
		status.Conditions = append(status.Conditions, domain.Condition{Type: "RetirementHold", Status: "True", Reason: "ResolutionTombstonesRetained", Message: "Retired member resolver entries are held or awaiting release acknowledgement; control-plane downgrade is unsafe", LastTransitionTime: now(rc)})
	}
	if s.RetirementUnprotected == "" {
		return
	}
	status.Conditions = append(status.Conditions, domain.Condition{Type: "RetirementProtection", Status: "False", Reason: "RetirementUnprotected", Message: s.RetirementUnprotected, LastTransitionTime: now(rc)})
}
