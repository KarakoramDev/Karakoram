// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type pxcJoinRetirementKey struct{}
type pxcJoinRetirementScope struct {
	owner  *pxcFormationCertification
	cohort map[string]bool
}

// Ordinary retirement still proves every survivor. Only this explicit exit may
// exclude other unadmitted cohort members, and only when its target is itself
// in the same recorded cohort. It cannot grant retirement of an old member.
func pxcRetirementSurvivor(ctx context.Context, s *state, target, peer string) bool {
	exit, _ := ctx.Value(pxcJoinRetirementKey{}).(*pxcJoinRetirementScope)
	if exit == nil || exit.owner != s.Formation.GTIDCertification || !exit.owner.JoinOnly || !exit.cohort[target] {
		return true
	}
	return !exit.cohort[peer]
}

// The caller invokes this before blocked/timeout admission can starve rollback.
// A scale-back request can delete only the recorded new cohort, never old data.
func reconcilePXCFormationLifecycle(ctx context.Context, rc *engine.Context, s *state) (handled bool, err error) {
	c := s.Formation.GTIDCertification
	if c == nil || !c.JoinOnly {
		return false, nil
	}
	if proof := c.Checkpoint.GTIDCertification; proof != nil && proof.Certified {
		// This cohort is admitted, even if activation acknowledgement is still
		// pending. An ordinary later scale-down must use normal data retirement.
		return false, nil
	}
	if !c.Legacy && c.Blocked == "" && (c.Checkpoint.GTIDCertification == nil || !c.Checkpoint.GTIDCertification.Certified) {
		deferPass, waitErr := pausePXCReplacementAdmissionForWindow(ctx, rc, s, c)
		if deferPass || waitErr != nil {
			return deferPass, waitErr
		}
	}
	cohort := make(map[string]bool, len(c.EmptyMembers)+len(c.FencedMembers))
	for name, admitted := range c.EmptyMembers {
		if admitted {
			cohort[name] = true
		}
	}
	for name, fenced := range c.FencedMembers {
		if fenced {
			cohort[name] = true
		}
	}
	if len(cohort) == 0 {
		return false, nil
	}
	present, admittedCount, exiting := 0, 0, false
	for _, m := range s.Members {
		if m.Role != "data" {
			continue
		}
		if !cohort[m.Name] {
			admittedCount++
			continue
		}
		present++
		exiting = exiting || m.Removing || m.Retirement != ""
		if !c.Legacy && m.Name == s.Formation.Seed {
			return true, engine.Invalid("joining-cohort cleanup cannot retire the admitted seed")
		}
		if previous := c.PreviousMapping; previous != nil {
			if _, old := previous.Members[m.Name]; old {
				return true, engine.Invalid("joining-cohort cleanup cannot retire a previously admitted member")
			}
		}
	}
	if c.Legacy && present > 0 {
		// Legacy members may already have been routed without this new
		// certificate. Their removal uses ordinary drain/membership proofs.
		return false, nil
	}
	requested := rc.Cluster.Spec.Components.Engine.Replicas == admittedCount
	if s.Replacement != nil {
		requested = s.Replacement.Recovery == "rollback"
	}
	if present > 0 && !requested && !exiting {
		return false, nil
	}
	if admittedCount < 3 || s.RestoreGTIDFence != "" {
		return true, engine.ErrUnavailable
	}
	handled = true
	defer func() { err = errors.Join(err, save(rc.Cluster, s)) }()
	p := Core{options: Options{Provider: "pxc"}}
	ctx = freshMySQLReads(ctx)
	if present == 0 {
		return true, retirePXCJoinAdmission(ctx, rc, s, c)
	}
	if busy, pendingErr := p.observePending(ctx, rc, s); busy || pendingErr != nil {
		return true, pendingErr
	}
	// Settle imported/deployed identities before touching their exact resources.
	for _, m := range s.Members {
		if !cohort[m.Name] {
			continue
		}
		if m.ImportBatch != nil {
			op, importErr := resumePlannedImport(ctx, rc, m)
			if importErr != nil {
				return true, importErr
			}
			if op != nil {
				p.track(s, m, "import", *op)
				return true, nil
			}
		}
		if m.Retirement == "" {
			// A completed import may have lost its response or the Pending
			// checkpoint. Recover backend-owned IDs before retiring the plan:
			// empty persisted IDs alone never prove resources are absent.
			if identityErr := verifyPXCJoinRetirementIdentity(ctx, rc, m); identityErr != nil {
				return true, identityErr
			}
			m.ImportBatch = nil
		}
		if m.DeployOp != nil {
			status, deployErr := rc.Backend.OpStatus(ctx, *m.DeployOp)
			if deployErr != nil || !status.Done && !status.Failed {
				return true, deployErr
			}
			m.DeployOp = nil
		}
	}
	// Persist the whole-cohort stop intent before any external stop. Retire
	// erase authorization now; the explicit user exit owns only these new IDs.
	authorityRetired := c.EmptyMembers != nil
	if authorityRetired {
		c.FencedMembers, c.EmptyMembers = cohort, nil
	}
	changed := false
	for _, m := range s.Members {
		if !cohort[m.Name] || m.Retirement != "" {
			continue
		}
		m.Removing, m.Offline = true, true
		m.Retirement = "runtime-stop"
		if m.Stage == "planned" && m.ServiceID == "" && m.VolumeID == "" {
			m.Retirement = "credentials"
		}
		changed = true
	}
	if changed {
		publishRevision(s)
	}
	if changed || authorityRetired {
		return true, nil
	}
	exit := &pxcJoinRetirementScope{owner: c, cohort: cohort}
	ctx = context.WithValue(ctx, pxcJoinRetirementKey{}, exit)
	stopping := false
	for _, m := range s.Members {
		if !cohort[m.Name] {
			continue
		}
		if m.Retirement == "runtime-stop" {
			if _, stopErr := p.retireMember(ctx, rc, s, m); stopErr != nil {
				return true, stopErr
			}
			stopping = true
		}
	}
	if stopping {
		return true, nil
	}
	// Every new runtime must have terminal stop evidence before any membership
	// proof/delete. Another stalled joiner is not a required healthy survivor.
	for _, m := range s.Members {
		if !cohort[m.Name] || m.Retirement != "stop" {
			continue
		}
		if m.RetirementOp == nil {
			return true, engine.Invalid("joining-cohort stop has no durable receipt")
		}
		status, stopErr := rc.Backend.OpStatus(ctx, *m.RetirementOp)
		if stopErr != nil {
			return true, stopErr
		}
		if status.Failed {
			m.Retirement = "runtime-stop"
			return true, engine.ErrUnavailable
		}
		if !status.Done {
			return true, nil
		}
		runtime, observeErr := engine.ObserveRetiringService(ctx, rc.Backend, m.Name, m.ServiceID, m.Volume, m.VolumeID, false)
		if observeErr != nil {
			return true, observeErr
		}
		if runtime != nil && runtime.Status != infra.StatusStopped {
			m.Retirement = "runtime-stop"
			return true, nil
		}
	}
	for _, m := range s.Members {
		if cohort[m.Name] {
			_, retireErr := p.retireMember(ctx, rc, s, m)
			return true, retireErr
		}
	}
	return true, nil
}

func retirePXCJoinAdmission(ctx context.Context, rc *engine.Context, s *state, c *pxcFormationCertification) error {
	c.EmptyMembers = nil
	owner := formationMappingOwner(c)
	if err := owner.persist(rc, s, &c.Checkpoint); err != nil {
		return err
	}
	if err := owner.release(ctx, rc, s, false); err != nil {
		return err
	}
	previous := c.PreviousMapping
	if previous == nil {
		s.Formation.GTIDCertification = nil
	} else {
		// This is the old proof, not a new certificate. Its original identity
		// and incident bindings remain checked by the ordinary controller.
		restored := *previous
		restored.EmptyMembers, restored.PreviousMapping = nil, nil
		s.Formation.GTIDCertification = &restored
	}
	invalidateMySQLReadObservation(ctx, rc)
	return nil
}

// Native Synced progress can legitimately wait hours for an operator's window.
// Do not confuse that wait with a joiner that stopped making native progress.
func pausePXCReplacementAdmissionForWindow(ctx context.Context, rc *engine.Context, s *state, c *pxcFormationCertification) (bool, error) {
	replacement := s.Replacement
	if replacement == nil || replacement.Recovery != "" || replacement.Stage != "join" {
		return false, nil
	}
	next := s.find(replacement.New)
	if next == nil || next.Stage != "joined" {
		return false, nil
	}
	op := rc.Operation
	if op == nil || op.ID != replacement.Operation {
		if rc.Store == nil {
			return false, nil
		}
		var err error
		op, err = rc.Store.GetOperation(ctx, replacement.Operation)
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return true, err
		}
	}
	if op.Actor == "system" || op.Phase != domain.PhaseRunning {
		return false, nil
	}
	policy := *rc
	policy.Operation = op
	if allowed, _ := policy.DisruptionAllowed(); allowed {
		return false, nil
	}
	status, err := live(normalMySQLReads(ctx), rc, next.Name)
	if errors.Is(err, engine.ErrObservationBudgetExhausted) {
		return true, nil // Retry a new pass; do not renew the old progress clock.
	}
	if err == nil && status.MemberState == "ONLINE" {
		c.LastProgressAt = now(rc)
	}
	return false, nil
}

// Verify the existing ownership contract before recovering IDs. A planned
// identity can lack credentials if cancellation won before credential creation;
// only backend-confirmed absence permits retiring that identity without them.
func verifyPXCJoinRetirementIdentity(ctx context.Context, rc *engine.Context, m *member) error {
	err := verifyMember(ctx, rc, m)
	if err == nil || !missing(err) || m.ServiceID != "" || m.VolumeID != "" {
		return err
	}
	for _, name := range []string{m.Name, m.Volume} {
		if name == "" {
			continue
		}
		service, observeErr := rc.Backend.GetService(ctx, name)
		if observeErr != nil {
			if service == nil && errors.Is(observeErr, infra.ErrNotFound) {
				continue
			}
			return observeErr
		}
		if service != nil {
			return engine.UnverifiedOwnership(service)
		}
	}
	return nil
}

func pxcReplacementRetirementPending(s *state, c *pxcFormationCertification) bool {
	replacement := s.Replacement
	if !c.JoinOnly || c.Legacy || replacement == nil || replacement.Recovery == "rollback" ||
		(replacement.Stage != "retire" && replacement.Stage != "done") ||
		!c.EmptyMembers[replacement.New] || c.EmptyMembers[replacement.Old] {
		return false
	}
	old := s.find(replacement.Old)
	return old == nil || old.Removing
}

func resumePXCReplacementAdmission(ctx context.Context, rc *engine.Context, s *state, c *pxcFormationCertification) (bool, error) {
	if c.CompletingReplacement == "" || s.Replacement != nil || s.LastReplacement != c.CompletingReplacement {
		return false, nil
	}
	proof := c.Checkpoint.GTIDCertification
	if proof == nil {
		return true, engine.Invalid("completed PXC replacement lacks its empty-member certification checkpoint")
	}
	if proof.CleanupPending {
		return true, formationMappingOwner(c).cleanup(freshMySQLReads(ctx), rc, s, &c.Checkpoint)
	}
	// Membership retirement is concrete lifecycle progress, not permission to
	// route the joiner or acknowledge history. Reuse its durable admission and
	// bounded remedy counters, retaining every previous probe round.
	if proof.Probe != nil && (len(proof.Evidence) == 0 || proof.Evidence[len(proof.Evidence)-1].Round != proof.Round) {
		proof.Evidence = append(proof.Evidence, pxcRestoreGTIDEvidence{Round: proof.Round, Epoch: c.Epoch, Before: proof.Before, Probe: *proof.Probe})
	}
	proof.Phase, c.Checkpoint.Step = "fence", "certify"
	proof.Certified, proof.CleanupComplete = false, false
	c.CompletedReplacement = c.CompletingReplacement
	c.HistoryDiagnostic = false
	c.Blocked, c.CompletingReplacement = "", ""
	c.LastProgressAt, c.ProgressPhase = now(rc), ""
	invalidateMySQLReadObservation(ctx, rc)
	return true, save(rc.Cluster, s)
}
