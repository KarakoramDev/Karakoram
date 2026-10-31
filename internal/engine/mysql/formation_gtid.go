// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// The empty-member admission predates native initialization. It is permission to
// replace only that joining member, never evidence inferred from a GTID conflict.
type pxcFormationCertification struct {
	Owner                 string                        `json:"owner"`
	Sequence              uint64                        `json:"sequence"`
	Epoch                 uint64                        `json:"epoch"`
	Group                 string                        `json:"group"`
	Checkpoint            backupCheckpoint              `json:"checkpoint"`
	EmptyMembers          map[string]bool               `json:"emptyMembers,omitempty"`
	Members               map[string]gtidMemberIdentity `json:"members,omitempty"`
	Blocked               string                        `json:"blocked,omitempty"`
	Legacy                bool                          `json:"legacy,omitempty"`
	JoinOnly              bool                          `json:"joinOnly,omitempty"`
	StartedAt             time.Time                     `json:"startedAt,omitempty"`
	LastProgressAt        time.Time                     `json:"lastProgressAt,omitempty"`
	JoinedCount           int                           `json:"joinedCount,omitempty"`
	ProgressPhase         string                        `json:"progressPhase,omitempty"`
	FencedMembers         map[string]bool               `json:"fencedMembers,omitempty"`
	PreviousMapping       *pxcFormationCertification    `json:"previousMapping,omitempty"`
	CompletingReplacement string                        `json:"completingReplacement,omitempty"`
	CompletedReplacement  string                        `json:"completedReplacement,omitempty"`
	HistoryDiagnostic     bool                          `json:"historyDiagnostic,omitempty"`
	// Retain evidence from obsolete post-admission certification without
	// retaining its lifecycle authority or client holds.
	RetiredCertification *pxcRestoreCertification `json:"retiredCertification,omitempty"`
}

type pxcMappingOwner struct {
	Operation *domain.Operation
	Formation *pxcFormationCertification
}

func (o pxcMappingOwner) persist(rc *engine.Context, s *state, cp *backupCheckpoint) error {
	if o.Formation == nil {
		return persistRestore(rc, o.Operation, *cp)
	}
	o.Formation.Checkpoint = *cp
	o.Formation.Epoch, o.Formation.Group = s.Formation.FormationEpoch, s.Formation.GroupName
	return save(rc.Cluster, s)
}
func (o pxcMappingOwner) acquire(ctx context.Context, rc *engine.Context, s *state, cp *backupCheckpoint) (bool, error) {
	if o.Formation != nil && o.Formation.StartedAt.IsZero() {
		o.Formation.StartedAt, o.Formation.LastProgressAt = now(rc), now(rc)
	}
	if !cp.GTIDCertification.FencesOwned {
		cp.GTIDCertification.FencesOwned = true
		if err := o.persist(rc, s, cp); err != nil {
			return false, err
		}
	}
	if o.Formation != nil && o.Formation.JoinOnly {
		if o.Formation.FencedMembers == nil {
			o.Formation.FencedMembers = map[string]bool{}
		}
		changed := false
		for _, m := range s.data() {
			if o.Formation.EmptyMembers[m.Name] {
				o.Formation.FencedMembers[m.Name] = true
			}
			if o.Formation.EmptyMembers[m.Name] && (!m.GaleraJoinFence || m.GaleraSSTDonor != s.Formation.Seed) {
				m.GaleraJoinFence, m.GaleraSSTDonor = true, s.Formation.Seed
				changed = true
			}
		}
		if changed {
			publishRevision(s)
		}
		return true, o.persist(rc, s, cp)
	}
	fenced, err := fencePXCRestoreClients(ctx, rc, s, o.Operation.ID)
	if err != nil || !fenced || o.Formation == nil {
		return fenced, err
	}
	changed := false
	for _, m := range s.data() {
		if o.Formation.EmptyMembers[m.Name] && m.Name != s.Formation.Seed && m.GaleraSSTDonor != s.Formation.Seed {
			m.GaleraSSTDonor = s.Formation.Seed
			changed = true
		}
	}
	if changed {
		publishRevision(s)
		if err := o.persist(rc, s, cp); err != nil {
			return false, err
		}
	}
	return true, nil
}
func (o pxcMappingOwner) blockedReason(s *state) string {
	if o.Formation != nil {
		return o.Formation.Blocked
	}
	return s.RestoreGTIDBlocked
}
func (o pxcMappingOwner) block(ctx context.Context, rc *engine.Context, s *state, cp *backupCheckpoint, reason string) error {
	if o.Formation == nil {
		return blockRestoreGTID(ctx, rc, s, o.Operation, cp, reason)
	}
	if o.Formation.HistoryDiagnostic {
		o.Formation.Blocked = "PXC joining-member history certification blocked: " + reason + "; the final-membership probe agrees on SID mapping, but the joining member remains unrouted; preserve its unique history and use explicit full acknowledgement, trusted-source restore or deletion; acknowledgement is not data repair"
	} else if o.Formation.JoinOnly {
		o.Formation.Blocked = "PXC joining-member GTID mapping certification blocked: " + reason + "; scale back to the admitted member count or cancel the pending replacement; existing data is preserved; otherwise restore from a trusted source; a restart or full-backup acknowledgement does not repair SID mapping"
	} else {
		o.Formation.Blocked = "PXC formation GTID mapping certification blocked: " + reason + "; preserve member GTIDs and delete/re-create; a restart or full-backup acknowledgement does not repair SID mapping"
	}
	cp.Step = "certify-cleanup"
	cp.GTIDCertification.Phase = "cleanup"
	cp.GTIDCertification.CleanupPending = true
	cp.GTIDCertification.CleanupComplete = false
	cp.GTIDCertification.Certified = false
	if err := o.persist(rc, s, cp); err != nil {
		return err
	}
	if err := o.cleanup(ctx, rc, s, cp); err != nil {
		return err
	}
	if o.Formation.HistoryDiagnostic {
		return nil
	}
	return engine.Invalid("%s", o.Formation.Blocked)
}
func (o pxcMappingOwner) cleanup(ctx context.Context, rc *engine.Context, s *state, cp *backupCheckpoint) error {
	if err := o.release(ctx, rc, s, true); err != nil {
		return err
	}
	if o.Formation != nil && o.Formation.HistoryDiagnostic {
		for _, m := range s.Members {
			if o.Formation.EmptyMembers[m.Name] {
				m.GaleraHistoryBlocked = true
			}
		}
		publishRevision(s)
	}
	c := cp.GTIDCertification
	c.CleanupPending, c.CleanupComplete, c.FencesOwned = false, true, false
	c.Phase, cp.Step = "blocked", "certify-blocked"
	return o.persist(rc, s, cp)
}

// Existing writers and ProxySQL remain running for empty scale/replace joins.
func (o pxcMappingOwner) release(ctx context.Context, rc *engine.Context, s *state, terminal bool) error {
	if o.Formation == nil || !o.Formation.JoinOnly {
		return releaseRestoreGTIDFence(ctx, rc, s, o.Operation.ID, terminal)
	}
	changed := false
	for _, m := range s.data() {
		if m.GaleraJoinFence {
			m.GaleraJoinFence, m.GaleraSSTDonor = false, ""
			changed = true
		}
	}
	if changed {
		publishRevision(s)
		if err := save(rc.Cluster, s); err != nil {
			return err
		}
	}
	var failures []error
	for _, m := range s.data() {
		if !o.Formation.FencedMembers[m.Name] {
			continue
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err == nil {
			err = a.Reload(ctx)
		}
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if m.Offline {
			continue
		}
		status, err := galeraStatus(ctx, rc, m.Name)
		if err != nil && terminal {
			metadata, metadataErr := a.Status(ctx)
			process, known := metadata.Processes["mysqld"]
			if metadataErr == nil && known && process.PID == 0 {
				continue
			}
		}
		if err == nil && status.OfflineMode {
			err = engine.ErrUnavailable
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (o pxcMappingOwner) requiresFence(name string) bool {
	return o.Formation == nil || !o.Formation.JoinOnly || o.Formation.EmptyMembers[name]
}

func (o pxcMappingOwner) probeMember(s *state) string {
	if o.Formation != nil && o.Formation.JoinOnly {
		for _, m := range s.data() {
			if o.Formation.EmptyMembers[m.Name] && !m.Offline {
				return m.Name
			}
		}
	}
	return s.Formation.Seed
}

// A live scale join need not sample identical wsrep counters. Each fenced
// candidate must have crossed the probe and be contained by the trusted seed's
// executed cut; its admission separately proves no unique data may be erased.
func (o pxcMappingOwner) mappingCutReady(s *state, observed map[string]myspec.GaleraStatusResponse) (bool, error) {
	seed := observed[s.Formation.Seed]
	for name, status := range observed {
		if o.Formation == nil || !o.Formation.JoinOnly {
			if status.LastCommitted != seed.LastCommitted {
				return false, nil
			}
			continue
		}
		if !o.Formation.EmptyMembers[name] {
			continue
		}
		contained, err := myspec.GTIDContains(seed.GTIDExecuted, status.GTIDExecuted)
		if err != nil || !contained {
			return false, err
		}
	}
	return true, nil
}
func (o pxcMappingOwner) remedyRefusal(s *state, target string, observed map[string]myspec.GaleraStatusResponse) string {
	if o.Formation == nil {
		return ""
	} // Explicit restore already owns follower replacement.
	if !o.Formation.EmptyMembers[target] {
		return "member " + target + " has no durable empty-join admission; automatic data replacement is not authorized"
	}
	seed := observed[s.Formation.Seed]
	candidate := observed[target]
	if seed.StateUUID != candidate.StateUUID || !o.Formation.JoinOnly && seed.LastCommitted != candidate.LastCommitted {
		return "member " + target + " lacks a common fully applied seed position"
	}
	contained, err := myspec.PXCFrozenHistoryContained(seed.GTIDExecuted, candidate.GTIDExecuted, seed.StateUUID)
	if err != nil || !contained {
		return "member " + target + " has unique or unproven frozen/local GTID history; automatic SST refused"
	}
	return ""
}

func pxcFormationMembersMatch(s *state, c *pxcFormationCertification) bool {
	if c.Epoch != s.Formation.FormationEpoch || c.Group != s.Formation.GroupName || len(c.Members) != len(s.data()) {
		return false
	}
	for _, m := range s.data() {
		old, ok := c.Members[m.Name]
		if !ok || old.ServiceID != m.ServiceID || old.VolumeID != m.VolumeID || old.ServerUUID != m.ServerUUID {
			return false
		}
	}
	return true
}

// Capture before ready()/bootstrap changes Initialized. A later already-formed
// cluster cannot retroactively manufacture this data-discard authorization.
func preparePXCFormationCertification(s *state) {
	old := s.Formation.GTIDCertification
	if old != nil && !old.Legacy && old.Checkpoint.GTIDCertification != nil && !old.Checkpoint.GTIDCertification.Certified && old.Blocked == "" {
		return
	}
	empty := map[string]bool{}
	for _, m := range s.data() {
		if freshPXCEmptyJoin(m) {
			empty[m.Name] = true
		}
	}
	if len(empty) == 0 {
		return
	}
	if old != nil {
		fresh := false
		for name := range empty {
			if !old.EmptyMembers[name] {
				fresh = true
				break
			}
		}
		if !fresh {
			return
		}
	}
	seq := uint64(1)
	if old != nil {
		seq = old.Sequence + 1
	}
	s.Formation.GTIDCertification = &pxcFormationCertification{
		Owner: fmt.Sprintf("formation-gtid/%s/%d/%d", s.UID, s.Formation.FormationEpoch, seq), Sequence: seq, Epoch: s.Formation.FormationEpoch, Group: s.Formation.GroupName,
		EmptyMembers: empty, Checkpoint: backupCheckpoint{Step: "certify"},
		FencedMembers:   maps.Clone(empty),
		JoinOnly:        s.Formation.State == "formed",
		PreviousMapping: previousPXCMapping(old),
	}
}

func freshPXCEmptyJoin(m *member) bool {
	return !m.Initialized && !m.Removing && m.Stage == "planned" && m.ServiceID == "" && m.VolumeID == "" && m.ImportBatch == nil && m.DeployOp == nil
}

func formationMappingOwner(c *pxcFormationCertification) pxcMappingOwner {
	return pxcMappingOwner{Operation: &domain.Operation{ID: c.Owner, Kind: "formation-certification"}, Formation: c}
}

// Acquiring the formation fence precedes native bootstrap/join. Provisioning
// continues while agent identities do not yet exist; legacy agents never hold.
func beginPXCFormationCertification(ctx context.Context, rc *engine.Context, s *state) error {
	if rc.Cluster.Spec.Provider != "pxc" {
		return nil
	}
	backup, err := loadBackupState(rc.Cluster)
	if err != nil {
		return err
	}
	if backup.RestoreOperation != nil {
		return nil
	}
	c := s.Formation.GTIDCertification
	if c == nil || c.Blocked != "" || c.Legacy || pxcServingRecertification(c) || c.Checkpoint.GTIDCertification != nil && c.Checkpoint.GTIDCertification.Certified {
		return nil
	}
	for _, m := range s.data() {
		if m.ServiceID == "" || m.Stage != "deployed" && m.Stage != "joined" {
			return nil
		}
	}
	ctx = freshMySQLReads(ctx)
	owner := formationMappingOwner(c)
	cp := &c.Checkpoint
	if err := negotiatePXCMapping(ctx, rc, s, cp, owner); err != nil {
		return err
	}
	if c.Legacy {
		return nil
	}
	_, err = owner.acquire(ctx, rc, s, cp)
	return err
}

func reconcilePXCFormationCertification(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if rc.Cluster.Spec.Provider != "pxc" {
		return false, nil
	}
	backup, err := loadBackupState(rc.Cluster)
	if err != nil {
		return true, err
	}
	if backup.RestoreOperation != nil {
		return false, nil
	}
	c := s.Formation.GTIDCertification
	if c == nil {
		// A history incident is not a birth/empty-join admission.
		return false, nil
	}
	if pxcServingRecertification(c) {
		return true, retirePXCServingCertification(ctx, rc, s, c)
	}
	// A timed-out join certificate must not starve the irreversible retirement
	// already owned by replacement. The unproven joiner stays unrouted; only
	// final membership can be certified after old-member cleanup finishes.
	if pxcReplacementRetirementPending(s, c) {
		if err := moveRetiringPXCSeed(freshMySQLReads(ctx), rc, s, s.Replacement.Old); err != nil {
			return true, err
		}
		if c.CompletingReplacement != s.Replacement.Operation {
			c.CompletingReplacement = s.Replacement.Operation
			return true, save(rc.Cluster, s)
		}
		return false, nil
	}
	if handled, err := resumePXCReplacementAdmission(ctx, rc, s, c); handled || err != nil {
		return handled, err
	}
	cp := &c.Checkpoint
	owner := formationMappingOwner(c)
	if handled, err := reconcilePXCFormationLifecycle(ctx, rc, s); handled || err != nil {
		return handled, err
	}
	if c.Legacy {
		return false, nil
	}
	if cp.GTIDCertification != nil && cp.GTIDCertification.Certified {
		if cp.GTIDCertification.FencesOwned || s.RestoreGTIDFence == c.Owner {
			return true, activatePXCFormationMapping(ctx, rc, s, c)
		}
		// Already admitted members never gain another formation owner merely
		// because their epoch, identity or history changed. New empty joins
		// acquire their own admission before native initialization.
		return false, nil
	}
	if c.Blocked != "" {
		if c.Owner == "" && cp.GTIDCertification == nil {
			return false, nil
		}
		if cp.GTIDCertification != nil && cp.GTIDCertification.CleanupPending {
			return true, owner.cleanup(freshMySQLReads(ctx), rc, s, cp)
		}
		if pxcFormationHistoryDiagnostic(s, c) {
			return false, nil
		}
		return true, engine.Invalid("%s", c.Blocked)
	}
	if pxcFormationAdmissionExpired(rc, s, c) {
		return true, owner.block(freshMySQLReads(ctx), rc, s, cp, "PXC join admission made no native Synced/certification progress within five minutes; recover the stalled joiner before retrying")
	}
	if err := owner.persist(rc, s, cp); err != nil {
		return true, err
	}
	if s.Formation.State != "formed" {
		return false, nil
	}
	active := cp.GTIDCertification != nil && cp.GTIDCertification.Phase != "negotiated" && cp.GTIDCertification.Phase != "fence" && cp.GTIDCertification.Phase != "observe"
	for _, m := range s.data() {
		if !active && (m.Stage != "joined" || m.Removing) {
			return false, nil
		}
	}
	// Replacement owns join/retirement; certify its final admitted membership,
	// not a temporary four-member component that still contains the old node.
	if s.Replacement != nil {
		return false, nil
	}
	ctx = freshMySQLReads(ctx)
	invalidateMySQLReadObservation(ctx, rc)
	defer invalidateMySQLReadObservation(ctx, rc)
	if err := beginPXCFormationCertification(ctx, rc, s); err != nil {
		return true, err
	}
	if c.Legacy {
		return false, nil
	}
	if err := certifyPXCMapping(ctx, rc, s, cp, owner); err != nil {
		return true, err
	}
	if cp.GTIDCertification.Certified {
		return true, activatePXCFormationMapping(ctx, rc, s, c)
	}
	return true, owner.persist(rc, s, cp)
}

func activatePXCFormationMapping(ctx context.Context, rc *engine.Context, s *state, c *pxcFormationCertification) error {
	owner := formationMappingOwner(c)
	cp := &c.Checkpoint
	// Retire empty-member erase authority before clients can write again.
	c.EmptyMembers = nil
	c.Members = gtidMemberIdentities(s, cp.GTIDCertification.Before)
	c.PreviousMapping = nil
	clearPXCBlockedHistoryRouting(s)
	if err := owner.persist(rc, s, cp); err != nil {
		return err
	}
	if err := owner.release(freshMySQLReads(ctx), rc, s, false); err != nil {
		return err
	}
	cp.GTIDCertification.FencesOwned = false
	return owner.persist(rc, s, cp)
}

func pxcFormationMappingPending(s *state) bool {
	c := s.Formation.GTIDCertification
	if c == nil || c.Legacy {
		return false
	}
	if !pxcFormationHistoryDiagnostic(s, c) && !(c.Owner == "" && c.Checkpoint.GTIDCertification == nil) && (c.Blocked != "" || c.Checkpoint.GTIDCertification == nil || !c.Checkpoint.GTIDCertification.Certified || c.Checkpoint.GTIDCertification.FencesOwned) {
		return true
	}
	// Planning a new empty member precedes persisting its certification owner.
	// Keep that admission pending without treating history/epoch changes in
	// already initialized members as another lifecycle.
	for _, m := range s.Members {
		if m.Role == "data" && freshPXCEmptyJoin(m) {
			return true
		}
	}
	return false
}

const pxcFormationAdmissionTimeout = 5 * time.Minute

func pxcFormationAdmissionExpired(rc *engine.Context, s *state, c *pxcFormationCertification) bool {
	if c.StartedAt.IsZero() {
		return false
	}
	joined := 0
	for _, m := range s.data() {
		if m.Stage == "joined" && !m.Removing {
			joined++
		}
	}
	phase := ""
	if c.Checkpoint.GTIDCertification != nil {
		phase = c.Checkpoint.GTIDCertification.Phase
	}
	if joined > c.JoinedCount || phase != c.ProgressPhase {
		c.JoinedCount, c.ProgressPhase, c.LastProgressAt = joined, phase, now(rc)
	}
	return now(rc).Sub(c.LastProgressAt) >= pxcFormationAdmissionTimeout
}

func recordRestoredPXCMapping(s *state, c *pxcRestoreCertification) {
	if c == nil || !c.Certified && !c.Legacy {
		return
	}
	if c.Certified {
		clearPXCBlockedHistoryRouting(s)
	}
	sequence := uint64(1)
	if previous := s.Formation.GTIDCertification; previous != nil {
		sequence = previous.Sequence + 1
	}
	s.Formation.GTIDCertification = &pxcFormationCertification{
		Owner: fmt.Sprintf("restored-mapping/%s/%d/%d", s.UID, s.Formation.FormationEpoch, sequence), Sequence: sequence, Epoch: s.Formation.FormationEpoch, Group: s.Formation.GroupName,
		Checkpoint: backupCheckpoint{Step: "activate", GTIDCertification: c}, Members: gtidMemberIdentities(s, c.Before),
		Legacy: c.Legacy,
	}
}

func pxcFormationMappingCondition(s *state) (domain.Condition, bool) {
	condition := domain.Condition{Type: "GTIDMappingCertified", Status: "Unknown", Reason: "CertificationUnavailable", Message: "This existing formation has no durable empty-join certification admission"}
	c := s.Formation.GTIDCertification
	if c == nil || c.Owner == "" && c.Checkpoint.GTIDCertification == nil {
		// The history condition carries the incident; an unavailable historical
		// mapping proof must not turn it into lifecycle ownership.
		return condition, true
	}
	if c.Legacy {
		condition.Message = "Agents do not all advertise PXC mapping certification; no new holds were acquired; the native GTID comparator remains enforced"
		return condition, true
	}
	if pxcFormationHistoryDiagnostic(s, c) {
		condition.Status, condition.Reason, condition.Message = "False", "GTIDMappingCertificationBlocked", c.Blocked
		return condition, false
	}
	if pxcFormationMappingPending(s) || s.RestoreGTIDFence == c.Owner {
		condition.Reason, condition.Message = "GTIDMappingCertificationInProgress", "Client SQL is fenced until the admitted PXC members prove one replicated GTID SID mapping"
		if c.JoinOnly {
			condition.Message = "Only joining members are fenced and unrouted; existing client writers remain available while replicated GTID mapping is certified"
		}
		if c.Blocked != "" {
			condition.Status, condition.Reason, condition.Message = "False", "GTIDMappingCertificationBlocked", c.Blocked
		}
		return condition, false
	}
	if pxcUnrepairedMixedMapping(s) {
		condition.Status, condition.Reason, condition.Message = "False", "ObservedMixedGTIDMapping", "A replicated probe observed mixed SID assignment; preserve history and replace from a trusted donor or restore before full acknowledgement; no client holds are acquired"
		return condition, false
	}
	if activeGTIDIncident(s) != nil {
		condition.Reason, condition.Message = "HistoryIncidentMappingUnproven", "The saved mapping certificate predates a GTID history incident; current compatible accounting is required for acknowledgement, without acquiring client holds"
		return condition, true
	}
	if !pxcFormationMembersMatch(s, c) {
		condition.Reason, condition.Message = "HistoricalMappingProof", "Previously admitted members have no probe certificate for the current epoch/incarnations; native GTID history comparison remains enforced without client holds"
		return condition, true
	}
	condition.Status, condition.Reason, condition.Message = "True", "ReplicatedGTIDMappingCertified", "The admitted PXC member incarnations passed replicated GTID mapping certification"
	return condition, true
}

func pxcFormationCleanupComplete(s *state, c *pxcFormationCertification) bool {
	if c.Owner != "" && s.RestoreGTIDFence == c.Owner {
		return false
	}
	proof := c.Checkpoint.GTIDCertification
	if proof == nil {
		return c.Owner == ""
	}
	return proof.CleanupComplete && !proof.CleanupPending && !proof.FencesOwned
}

func previousPXCMapping(c *pxcFormationCertification) *pxcFormationCertification {
	if c != nil && (c.Legacy || c.Checkpoint.GTIDCertification != nil && c.Checkpoint.GTIDCertification.Certified) {
		return c
	}
	return nil
}

// Earlier controllers could replace a serving certificate with a new owner on
// history/epoch changes alone. No new empty admission means no authority to
// recertify or retain client holds. Keep its native evidence, not its authority.
func pxcServingRecertification(c *pxcFormationCertification) bool {
	return c != nil && !c.JoinOnly && len(c.EmptyMembers) == 0 && len(c.FencedMembers) == 0 &&
		c.PreviousMapping != nil && (c.PreviousMapping.Legacy ||
		c.PreviousMapping.Checkpoint.GTIDCertification != nil && c.PreviousMapping.Checkpoint.GTIDCertification.Certified)
}

func retirePXCServingCertification(ctx context.Context, rc *engine.Context, s *state, c *pxcFormationCertification) error {
	// Release is replayable after a crash, including a previously completed
	// cleanup whose proxy start response was lost. Do not retire ownership until
	// every owned release has succeeded.
	if err := formationMappingOwner(c).release(freshMySQLReads(ctx), rc, s, true); err != nil {
		return err
	}
	restored := *c.PreviousMapping
	// Never reuse the retired owner's probe receipts for a later empty join.
	if restored.Sequence < c.Sequence {
		restored.Sequence = c.Sequence
	}
	if proof := c.Checkpoint.GTIDCertification; proof != nil {
		proof.FencesOwned, proof.CleanupPending, proof.CleanupComplete = false, false, true
		proof.Phase = "retired"
		restored.RetiredCertification = proof
	}
	restored.PreviousMapping = nil
	s.Formation.GTIDCertification = &restored
	invalidateMySQLReadObservation(ctx, rc)
	return save(rc.Cluster, s)
}

func pxcMixedMappingObserved(c *pxcRestoreCertification) bool {
	if c == nil || len(c.Evidence) == 0 {
		return false
	}
	var first string
	for _, sid := range c.Evidence[len(c.Evidence)-1].Mapping {
		if sid == "" {
			continue
		}
		if first != "" && sid != first {
			return true
		}
		first = sid
	}
	return false
}

func pxcUnrepairedMixedMapping(s *state) bool {
	c := s.Formation.GTIDCertification
	return c != nil && (pxcMixedMappingObserved(c.RetiredCertification) || pxcMixedMappingObserved(c.Checkpoint.GTIDCertification))
}

// After irreversible replacement, a successful native mapping probe can leave
// only a frozen-history discrepancy. Keep that certificate blocked and the
// joiner unrouted, but do not turn it into a new lock on documented recovery.
func pxcFormationHistoryDiagnostic(s *state, c *pxcFormationCertification) bool {
	return c != nil && c.JoinOnly && c.HistoryDiagnostic && c.Blocked != "" &&
		c.CompletedReplacement != "" && c.CompletedReplacement == s.LastReplacement &&
		pxcFormationCleanupComplete(s, c)
}

// Only a successful full-membership certificate (including explicit restore)
// supersedes a prior unique-history route hold. A full backup is not one.
func clearPXCBlockedHistoryRouting(s *state) {
	changed := false
	for _, m := range s.Members {
		if m.GaleraHistoryBlocked {
			m.GaleraHistoryBlocked = false
			changed = true
		}
	}
	if changed {
		publishRevision(s)
	}
}
