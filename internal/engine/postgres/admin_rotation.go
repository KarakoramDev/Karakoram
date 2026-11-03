// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
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
	Operation string `json:"operation"`
	Stage     string `json:"stage"`
	Previous  string `json:"previous,omitempty"`
	Password  string `json:"password,omitempty"`
	Verifier  string `json:"verifier,omitempty"`
	Groups    []int  `json:"groups"`
	Completed int    `json:"completed"`
	InFlight  bool   `json:"inFlight,omitempty"`
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
	for _, m := range s.group(group).data() {
		status, e := live(ctx, rc, m.Name)
		if e != nil || !status.Running || status.Role != "primary" && status.Role != "standalone" {
			continue
		}
		a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			continue
		}
		var receipt pgspec.AccessResult
		e = a.Engine(ctx, "postgres", "roles", pgspec.AccessRequest{Operation: "admin-rotation", RequestID: op.ID, Role: &pgspec.RoleWrite{Role: pgspec.Role{Name: "postgres"}, Password: &j.Verifier}}, &receipt)
		a.Close()
		if e != nil || receipt.Outcome == "unknown" {
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
			break
		}
		if receipt.Outcome != "applied" {
			break
		}
		j.Completed++
		j.InFlight = false
		if err = saveAdminRotation(ctx, rc, j); err != nil {
			return engine.OpResult{}, err
		}
		return result(j.Stage, fmt.Sprintf("Group %d administrator transaction acknowledged", group), false)
	}
	return result(j.Stage, fmt.Sprintf("Forward recovery: waiting for group %d writable primary credential receipt", group), false)
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
