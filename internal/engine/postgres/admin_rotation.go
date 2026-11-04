// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

const adminRotationKey = "admin/rotation"

type adminRotationSyncKey struct{}

func administratorRotationOperation(op *domain.Operation) bool {
	if op.Kind != domain.OperationRotateSystemUser {
		return false
	}
	var payload struct {
		User string `json:"user"`
	}
	return json.Unmarshal(op.Payload, &payload) == nil && payload.User == "postgres"
}

// This entire journal is sealed, including the stable SCRAM verifier. Neither
// operation checkpoints nor native receipts contain credential material.
type adminRotation struct {
	Operation   string    `json:"operation"`
	Stage       string    `json:"stage"`
	Previous    string    `json:"previous,omitempty"`
	Password    string    `json:"password,omitempty"`
	Verifier    string    `json:"verifier,omitempty"`
	Groups      []int     `json:"groups"`
	Completed   int       `json:"completed"`
	InFlight    bool      `json:"inFlight,omitempty"`
	AgentMember string    `json:"agentMember,omitempty"`
	AgentDeploy *infra.Op `json:"agentDeploy,omitempty"`
	// AgentSelected marks a candidate not yet crossing the dispatch boundary.
	// Absent on older journals: conservatively treat their member as in flight.
	AgentSelected bool `json:"agentSelected,omitempty"`
	Attempts      int  `json:"attempts,omitempty"`
}

func readAdminRotation(ctx context.Context, rc *engine.Context) (*adminRotation, error) {
	b, err := secret(ctx, rc, adminRotationKey)
	if missing(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var j adminRotation
	if err = json.Unmarshal(b, &j); err != nil {
		return nil, err
	}
	return &j, nil
}
func saveAdminRotation(ctx context.Context, rc *engine.Context, j *adminRotation) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return putSecret(ctx, rc, adminRotationKey, b)
}

// AdmitAdminRotation requires transition-aware agents on every data member,
// including replicas that may become leaders during forward recovery.
func (p *Core) AdmitAdminRotation(ctx context.Context, rc *engine.Context) error {
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	if len(s.data()) == 0 {
		return domain.Conflict("operation", domain.OperationRotateSystemUser, domain.ReasonUnsupported, engine.ErrConflict, "Administrator rotation requires running agents; run upgrade-agents first")
	}
	if s.Pending != nil || s.Rolling != nil || s.Upgrade != nil || s.Replacement != nil || s.CitusScale != nil || s.RestoreRecovery != nil || s.Rebalance != nil || s.CitusOperation != "" {
		return domain.Conflict("operation", domain.OperationRotateSystemUser, domain.ReasonUnsupported, engine.ErrConflict, "Structural or rolling maintenance must complete before administrator rotation")
	}
	for _, m := range s.data() {
		status, err := live(ctx, rc, m.Name)
		if err != nil || !status.AdminRotation {
			return domain.Conflict("operation", domain.OperationRotateSystemUser, domain.ReasonUnsupported, engine.ErrConflict, "Member %s does not advertise PostgreSQL administrator rotation; run upgrade-agents first", m.Name)
		}
	}
	return nil
}

func (p *Core) rotateAdministrator(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	result := func(stage, message string, done bool) (engine.OpResult, error) {
		cp, err := json.Marshal(struct {
			Stage string `json:"stage"`
		}{stage})
		return engine.OpResult{Step: stage, Message: message, Done: done, Checkpoint: cp, RequeueAfter: time.Second}, err
	}
	var checkpoint struct {
		Stage string `json:"stage"`
	}
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, &checkpoint); err != nil {
			return engine.OpResult{}, err
		}
	}
	if checkpoint.Stage == "rotated" {
		return result("rotated", "Administrator credential rotation complete", true)
	}
	j, err := readAdminRotation(ctx, rc)
	if err != nil {
		return engine.OpResult{}, err
	}
	if j == nil || j.Operation != op.ID {
		if j != nil && j.Stage != "rotated" && j.Stage != "rejected" {
			return engine.OpResult{}, fmt.Errorf("%w: another administrator rotation requires recovery", engine.ErrConflict)
		}
		if err = p.AdmitAdminRotation(ctx, rc); err != nil {
			return engine.OpResult{}, err
		}
		previous, err := secret(ctx, rc, "users/postgres")
		if err != nil {
			return engine.OpResult{}, err
		}
		password, err := secrets.Password()
		if err != nil {
			return engine.OpResult{}, err
		}
		verifier, err := secrets.SCRAMVerifier(password)
		if err != nil {
			return engine.OpResult{}, err
		}
		s, err := load(rc.Cluster)
		if err != nil {
			return engine.OpResult{}, err
		}
		groups := map[int]bool{}
		for _, m := range s.data() {
			groups[m.Group] = true
		}
		j = &adminRotation{Operation: op.ID, Stage: "transition", Previous: string(previous), Password: password, Verifier: verifier}
		for group := range groups {
			j.Groups = append(j.Groups, group)
		}
		sort.Ints(j.Groups)
		// Workers first limits the coordinator/proxy password publication
		// window. Persisted journals retain their original order and cursor.
		if s.Topology == "distributed" && len(j.Groups) > 1 && j.Groups[0] == 0 {
			j.Groups = append(j.Groups[1:], 0)
		}
		if err = saveAdminRotation(ctx, rc, j); err != nil {
			return engine.OpResult{}, err
		}
		return result(j.Stage, "Administrator credential sealed; staging transition-aware member configuration", false)
	}
	if j.Stage == "rejected" {
		return engine.OpResult{}, engine.Invalid("Administrator password transaction rejected before any primary committed; original credential retained")
	}
	if j.Stage == "rotated" {
		return result("rotated", "Administrator credential rotation complete", true)
	}
	if j.Stage == "transition" || j.Stage == "publish" || j.Stage == "cleanup" || j.Stage == "rollback" {
		if j.Stage == "publish" {
			if err = putSecret(ctx, rc, "users/postgres", []byte(j.Password)); err != nil {
				return engine.OpResult{}, err
			}
		}
		// Repeat these idempotent writes after a crash between journal and revision.
		if err = putSecret(ctx, rc, "access/revision", []byte(op.ID+"/"+j.Stage)); err != nil {
			return engine.OpResult{}, err
		}
		pending, err := p.adminConfigurationPending(ctx, rc)
		if err != nil {
			return result(j.Stage, "Forward recovery: waiting for member/proxy configuration observation", false)
		}
		if pending != "" {
			return result(j.Stage, "Waiting for "+pending, false)
		}
		if j.Stage == "transition" {
			j.Stage = "native"
		} else if j.Stage == "publish" {
			if f, ok := p.options.Flavour.(interface {
				AdminConsumers(context.Context, *engine.Context) (string, error)
			}); ok {
				pending, e := f.AdminConsumers(ctx, rc)
				if e != nil || pending != "" {
					return result(j.Stage, "Forward recovery: waiting for Supabase "+pending+" credential acknowledgement", false)
				}
			}
			j.Stage = "cleanup"
		} else if j.Stage == "rollback" {
			j.Stage = "rejected"
		} else {
			j.Stage = "rotated"
			j.Previous = ""
			j.Password = ""
			j.Verifier = ""
		}
		if err = saveAdminRotation(ctx, rc, j); err != nil {
			return engine.OpResult{}, err
		}
		if j.Stage == "rejected" {
			return engine.OpResult{}, engine.Invalid("Administrator password transaction rejected; original credential retained")
		}
		return result(j.Stage, "Administrator rotation advancing consumer convergence", j.Stage == "rotated")
	}
	if j.Stage != "native" {
		return engine.OpResult{}, engine.Invalid("invalid administrator rotation journal")
	}
	if j.Completed == len(j.Groups) {
		j.Stage = "publish"
		if err = saveAdminRotation(ctx, rc, j); err != nil {
			return engine.OpResult{}, err
		}
		return result(j.Stage, "All group primaries accepted the new credential; publishing consumers", false)
	}
	group := j.Groups[j.Completed]
	// Intent is durable before dispatch. Lost responses replay the same receipt.
	j.InFlight = true
	if err = saveAdminRotation(ctx, rc, j); err != nil {
		return engine.OpResult{}, err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	// A saved runtime deployment may have stopped the group's only agent.
	// Recover that durable intent before requiring a live SQL primary.
	if s.Topology == "distributed" && (j.AgentMember != "" || j.AgentDeploy != nil) {
		pending, recoveryErr := p.adminRotationAgent(ctx, rc, s, j)
		if recoveryErr != nil {
			return engine.OpResult{}, recoveryErr
		}
		if pending != nil {
			return *pending, nil
		}
	}
	reason := "no running writable primary; restore member health"
	for _, m := range s.group(group).data() {
		status, e := live(ctx, rc, m.Name)
		if e != nil || status.Role != "primary" && status.Role != "standalone" {
			continue
		}
		if s.Topology == "distributed" && !status.AdminRotationLocalReceipts {
			// Legacy readiness can fail on cross-group authentication even
			// while its owner-local peer connection and Patroni role work.
			if !status.Running && !status.AdminRotation {
				continue
			}
			// Persist the selected group/member independently of the runtime
			// receipt so terminal retries survive loss of its live agent.
			j.AgentMember = m.Name
			j.AgentSelected = true
			if err = saveAdminRotation(ctx, rc, j); err != nil {
				return engine.OpResult{}, err
			}
			pending, recoveryErr := p.adminRotationAgent(ctx, rc, s, j)
			if recoveryErr != nil {
				return engine.OpResult{}, recoveryErr
			}
			if pending != nil {
				return *pending, nil
			}
			return result(j.Stage, "Local-receipt agent converged; rediscovering writable primary", false)
		}
		if !status.Running {
			continue
		}
		a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			reason = "agent connection unavailable; restore agent connectivity"
			continue
		}
		var receipt pgspec.AccessResult
		e = a.Engine(ctx, "postgres", "roles", pgspec.AccessRequest{Operation: "admin-rotation", RequestID: op.ID, Role: &pgspec.RoleWrite{Role: pgspec.Role{Name: "postgres"}, Password: &j.Verifier}}, &receipt)
		a.Close()
		if e != nil {
			reason = "agent request unavailable; restore agent connectivity and local peer authentication"
			break
		}
		if receipt.Outcome == "unknown" {
			reason = receipt.Code + "; inspect node-local receipt transaction and local peer authentication"
			break
		}
		if receipt.Outcome == "rejected" {
			if j.Completed == 0 && (receipt.Code == "transaction_rejected" || receipt.Code == "role_or_membership_missing") {
				j.Stage = "rollback"
				j.Previous = ""
				j.Password = ""
				j.Verifier = ""
				if err = saveAdminRotation(ctx, rc, j); err != nil {
					return engine.OpResult{}, err
				}
				return result(j.Stage, "Native transaction rejected; retiring pending configuration with original credential unchanged", false)
			}
			reason = receipt.Code + "; resolve the native role/receipt conflict without replacing the sealed intent"
			break
		}
		if receipt.Outcome != "applied" {
			break
		}
		j.Completed++
		j.InFlight = false
		j.Attempts = 0
		if err = saveAdminRotation(ctx, rc, j); err != nil {
			return engine.OpResult{}, err
		}
		return result(j.Stage, fmt.Sprintf("Group %d administrator transaction acknowledged", group), false)
	}
	j.Attempts++
	if err = saveAdminRotation(ctx, rc, j); err != nil {
		return engine.OpResult{}, err
	}
	if j.Attempts >= 30 {
		return engine.OpResult{}, engine.Invalid("Administrator forward recovery blocked at group %d after %d attempts: %s; restore the group's owner-local peer access and native role/receipt identity, then allow the same sealed rotation to resume; do not start another rotation or reset credentials", group, j.Attempts, reason)
	}
	return result(j.Stage, fmt.Sprintf("Forward recovery: waiting for group %d writable primary credential receipt (%s)", group, reason), false)
}

// adminRotationAgent reconciles the durable runtime intent without requiring
// the runtime it is repairing to accept agent or database connections. A nil
// result means the caller may rediscover a writable primary for native SQL.
func (p *Core) adminRotationAgent(ctx context.Context, rc *engine.Context, s *state, j *adminRotation) (*engine.OpResult, error) {
	wait := func(message string) (*engine.OpResult, error) {
		return &engine.OpResult{Step: j.Stage, Message: message, RequeueAfter: time.Second}, nil
	}
	target := s.find(j.AgentMember)
	if target == nil || target.Role != "data" || target.Group != j.Groups[j.Completed] {
		return nil, engine.Invalid("Administrator recovery deployment member does not match the pending sealed group")
	}
	var deployment infra.OpState
	var deployErr error
	exhausted := false
	if j.AgentDeploy != nil {
		deployment, deployErr = rc.Backend.OpStatus(ctx, *j.AgentDeploy)
		if deployment.Status == "DeploymentRetriesExhausted" && deployErr == nil {
			deployErr = &infra.DeploymentExhaustedError{Service: target.Name, LastError: deployment.Message}
		}
		exhausted = errors.Is(deployErr, infra.ErrDeploymentRetriesExhausted)
		if deployErr != nil && !exhausted {
			return nil, fmt.Errorf("administrator recovery agent deployment for %s: %w", target.Name, deployErr)
		}
		if !deployment.Done && !deployment.Failed && !exhausted {
			return wait("Forward recovery: waiting for local-receipt agent deployment on " + target.Name)
		}
	}
	// Observations may retire a terminal/obsolete intent, but are never a
	// prerequisite for polling or retrying the runtime's saved deployment.
	primaryCapable, targetCapable := false, false
	for _, m := range s.group(target.Group).data() {
		status, err := live(ctx, rc, m.Name)
		if err != nil || !status.Running || !status.AdminRotationLocalReceipts {
			continue
		}
		if m.Name == target.Name {
			targetCapable = true
		}
		if status.Role == "primary" || status.Role == "standalone" {
			primaryCapable = true
		}
	}
	if primaryCapable || j.AgentDeploy != nil && !deployment.Failed && !exhausted && targetCapable {
		j.AgentDeploy, j.AgentMember = nil, ""
		j.AgentSelected = false
		return nil, saveAdminRotation(ctx, rc, j)
	}
	if j.AgentDeploy != nil {
		if !deployment.Failed && !exhausted {
			return nil, engine.Invalid("Administrator recovery deployment for %s completed without owner-local receipt capability; verify the matching v0.4.5-or-newer agent artifact and local peer authentication, then resume the same sealed rotation", target.Name)
		}
		// Retire only the terminal receipt. Keep the selected member so a
		// failed singleton can be redeployed without a live-primary probe.
		j.AgentDeploy = nil
		if err := saveAdminRotation(ctx, rc, j); err != nil {
			return nil, err
		}
		if exhausted {
			spec, err := p.agentUpgradeRuntimeSpec(rc, target)
			if err != nil {
				return nil, err
			}
			return nil, engine.RecordDeploymentExhausted(ctx, rc, target.Name, spec, deployErr)
		}
		return wait("Forward recovery: retrying local-receipt agent deployment on " + target.Name)
	}
	if rc.Store == nil {
		return nil, engine.Invalid("Administrator recovery requires its retained operation record before deploying an agent")
	}
	owner, err := rc.Store.GetOperation(ctx, j.Operation)
	if err != nil {
		return nil, fmt.Errorf("administrator recovery cannot load its retained operation authorization: %w", err)
	}
	if owner == nil || owner.ID != j.Operation || !administratorRotationOperation(owner) || owner.Cluster != rc.Cluster.Name || domain.NormalizedNamespace(owner.Namespace) != domain.NormalizedNamespace(rc.Cluster.Namespace) {
		return nil, engine.Invalid("Administrator recovery retained operation does not match its sealed cluster owner")
	}
	caller := rc.Operation
	rc.Operation = owner
	defer func() { rc.Operation = caller }()
	if window := rc.WaitForWindow(); window.RequeueAfter > 0 {
		return &engine.OpResult{Step: j.Stage, Message: "Administrator recovery requires a local-receipt agent upgrade; waiting for maintenance window", RequeueAfter: window.RequeueAfter}, nil
	}
	if j.AgentSelected {
		// Selection before a window wait is not authority to disrupt a
		// former primary. Re-observe this group at the first dispatch boundary.
		var primary *member
		for _, m := range s.group(target.Group).data() {
			status, statusErr := live(ctx, rc, m.Name)
			if statusErr != nil || !status.Running && !status.AdminRotation || status.Role != "primary" && status.Role != "standalone" {
				continue
			}
			if status.AdminRotationLocalReceipts {
				j.AgentDeploy, j.AgentMember, j.AgentSelected = nil, "", false
				return nil, saveAdminRotation(ctx, rc, j)
			}
			primary = m
			break
		}
		if primary == nil {
			return wait("Forward recovery: waiting for the pending group's current primary before initial agent deployment")
		}
		if primary.Name != target.Name {
			target = primary
			j.AgentMember = primary.Name
			if err := saveAdminRotation(ctx, rc, j); err != nil {
				return nil, err
			}
		}
	}
	spec, err := p.agentUpgradeRuntimeSpec(rc, target)
	if err != nil {
		return nil, err
	}
	if j.AgentSelected {
		// Persist intent before submission: a lost response may already have
		// disrupted this runtime, so later recovery must not require it live.
		j.AgentSelected = false
		if err := saveAdminRotation(ctx, rc, j); err != nil {
			return nil, err
		}
	}
	submitted, err := engine.Deploy(ctx, rc, target.Name, spec)
	if err != nil {
		return nil, fmt.Errorf("administrator recovery agent upgrade for %s: %w", target.Name, err)
	}
	j.AgentDeploy = &submitted
	if err := saveAdminRotation(ctx, rc, j); err != nil {
		return nil, err
	}
	return wait("Forward recovery: installing local-receipt agent on " + target.Name)
}

func (p *Core) adminConfigurationPending(ctx context.Context, rc *engine.Context) (string, error) {
	if _, err := p.Sync(context.WithValue(ctx, adminRotationSyncKey{}, true), rc); err != nil {
		return "", err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return "", err
	}
	for _, m := range s.Members {
		if m.ConfigVersion != s.ConfigVersion {
			return "member " + m.Name + " assigned configuration", nil
		}
		ok, err := readyDesired(ctx, rc, m)
		if err != nil {
			return "", err
		}
		if !ok {
			return "member " + m.Name + " configuration acknowledgement/readiness", nil
		}
	}
	status, err := p.Status(ctx, rc)
	if err != nil {
		return "", err
	}
	for _, c := range status.Conditions {
		if c.Type == "CredentialsPublished" && c.Status != "True" {
			return "ProxySQL CredentialsPublished", nil
		}
	}
	return "", nil
}
