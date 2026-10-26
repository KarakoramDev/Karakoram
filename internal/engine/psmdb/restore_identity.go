// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
	"github.com/KarakoramDev/Karakoram/internal/redact"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Kept outside provider-owned state so operation hooks and the provider's outer
// save cannot overwrite one another. Only the encrypted secret store holds pwd.
type restorePBMIdentity struct {
	User          string `json:"user"`
	Secret        string `json:"secret"`
	Stage         string `json:"stage"`
	ConfigVersion int64  `json:"configVersion"`
}

func loadRestorePBMIdentity(raw []byte) (*restorePBMIdentity, error) {
	var state struct {
		Identity *restorePBMIdentity `json:"pbmRestoreIdentity"`
	}
	err := decodeJSON(raw, &state)
	return state.Identity, err
}

func (h hooks) identityState() (*internalState, error) {
	if h.state != nil {
		return h.state, nil
	}
	return load(h.rc.Cluster)
}

func (h hooks) publishRestoreIdentity(s *internalState, identity *restorePBMIdentity, stage string) error {
	for _, m := range s.Members {
		s.ConfigVersion = max(s.ConfigVersion, m.ConfigVersion)
	}
	s.ConfigVersion++
	for _, m := range s.Members {
		if m.Role != agentapi.RoleRouter && !m.Removing {
			m.ConfigVersion = s.ConfigVersion
		}
	}
	identity.Stage, identity.ConfigVersion = stage, s.ConfigVersion
	if err := setInternal(h.rc.Cluster, "pbmRestoreIdentity", identity); err != nil {
		return err
	}
	// This changes desired in-memory state only. The caller returns incomplete
	// so the reconciler atomically commits state/checkpoint before side effects.
	return save(h.rc.Cluster, s)
}

func (h hooks) restoreIdentityPublished(ctx context.Context, identity *restorePBMIdentity) (bool, error) {
	cluster, err := h.rc.Store.GetCluster(ctx, h.rc.Cluster.Ref())
	if err != nil {
		return false, err
	}
	published, err := loadRestorePBMIdentity(cluster.Internal)
	if err != nil {
		return false, err
	}
	return published != nil && *published == *identity, nil
}

func (h hooks) PreparePITRIdentity(ctx context.Context, operation string) (bool, error) {
	rc := h.rc
	s, err := h.identityState()
	if err != nil {
		return false, err
	}
	// Never submit a restore to an old agent that ignores the alternate username.
	for _, m := range s.Members {
		if m.Role == agentapi.RoleRouter || m.Removing {
			continue
		}
		c, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return false, err
		}
		status, err := engine.ObserveAgentStatus(ctx, c, "PSMDB", m.Name)
		c.Close()
		if err != nil {
			return false, err
		}
		if !status.PBMRestoreIdentity {
			return false, fmt.Errorf("member %s requires an agent upgrade before PITR: restore-scoped PBM authentication is unavailable: %w", m.Name, engine.ErrNotSupported)
		}
	}
	identity, err := loadRestorePBMIdentity(rc.Cluster.Internal)
	if err != nil {
		return false, err
	}
	if identity == nil {
		identity = &restorePBMIdentity{User: "restore_" + secrets.Hash(operation)[:24], Secret: "restores/" + operation + "/pbm"}
		return false, h.publishRestoreIdentity(s, identity, "creating")
	}
	if identity.User != "restore_"+secrets.Hash(operation)[:24] {
		return false, errors.New("another restore owns the PBM authentication identity; operator recovery is required")
	}
	published, err := h.restoreIdentityPublished(ctx, identity)
	if err != nil || !published {
		return false, err
	}
	if identity.Stage == "creating" {
		if err := ensureSecret(ctx, rc, identity.Secret, secrets.Password); err != nil {
			return false, err
		}
		password, err := secret(ctx, rc, identity.Secret)
		if err != nil {
			return false, err
		}
		var roles []domain.RoleRef
		for _, def := range users.SystemUserDefs() {
			if def.Name == "backup" {
				roles = def.Roles
			}
		}
		for set, state := range s.Sets {
			if state.Stage != "ready" {
				continue
			}
			c, err := h.p.dial(ctx, rc, s, set, "userAdmin")
			if err != nil {
				return false, err
			}
			user := domain.DatabaseUser{Name: identity.User, Database: "admin", Roles: roles}
			err = users.UpdateUser(ctx, c, user, string(password))
			if errors.Is(err, engine.ErrNotFound) {
				err = users.CreateUser(ctx, c, user, string(password))
			}
			_ = c.Disconnect(ctx)
			if err != nil {
				return false, err
			}
		}
		return false, h.publishRestoreIdentity(s, identity, "active")
	}
	if identity.Stage != "active" {
		return false, errors.New("PITR authentication identity is already being released")
	}
	return h.reloadPBMIdentity(ctx, s, identity.ConfigVersion)
}

func (h hooks) reloadPBMIdentity(ctx context.Context, s *internalState, version int64) (bool, error) {
	for _, m := range s.Members {
		if m.Role == agentapi.RoleRouter || m.Removing {
			continue
		}
		c, err := h.rc.Agents.Dial(ctx, h.rc.Cluster.Ref(), m.Name)
		if err != nil {
			return false, err
		}
		err = c.Reload(ctx)
		if err == nil {
			status, statusErr := engine.ObserveAgentStatus(ctx, c, "PSMDB", m.Name)
			if statusErr != nil || status.ConfigVersion < version {
				c.Close()
				return false, statusErr
			}
		}
		if err == nil {
			var response agentapi.PBMResponse
			response, err = c.PBM(ctx, agentapi.PBMRequest{Command: "status"})
			if err == nil && response.ExitCode != 0 {
				err = fmt.Errorf("PBM identity verification failed: %s %s", redact.NativeError(response.Stderr), redact.NativeError(string(response.Output)))
			}
		}
		c.Close()
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (h hooks) restorePBMClient(ctx context.Context, s *internalState, set string) (*mongo.Client, error) {
	identity, err := loadRestorePBMIdentity(h.rc.Cluster.Internal)
	if err != nil {
		return nil, err
	}
	if identity == nil || identity.Stage == "creating" {
		return h.p.dial(ctx, h.rc, s, set, "backup")
	}
	password, err := secret(ctx, h.rc, identity.Secret)
	if err != nil {
		return nil, err
	}
	ca, err := secret(ctx, h.rc, "tls/ca.crt")
	if err != nil {
		return nil, err
	}
	return mongoadmin.Dial(ctx, mongoadmin.DialOptions{Hosts: hosts(s.setMembers(set)), Dialer: h.rc.Dialer, ReplicaSet: set, Username: identity.User, Password: string(password), CAPEM: ca, TLS: s.TLSMode != "disabled"})
}

func (h hooks) FinishPITRIdentity(ctx context.Context, operation string) (bool, error) {
	identity, err := loadRestorePBMIdentity(h.rc.Cluster.Internal)
	if err != nil || identity == nil {
		return identity == nil, err
	}
	if identity.User != "restore_"+secrets.Hash(operation)[:24] {
		return false, errors.New("another restore owns the PBM authentication identity; operator recovery is required")
	}
	s, err := h.identityState()
	if err != nil {
		return false, err
	}
	if identity.Stage != "releasing" {
		return false, h.publishRestoreIdentity(s, identity, "releasing")
	}
	published, err := h.restoreIdentityPublished(ctx, identity)
	if err != nil || !published {
		return false, err
	}
	if done, err := h.reloadPBMIdentity(ctx, s, identity.ConfigVersion); !done || err != nil {
		return false, err
	}
	for set, state := range s.Sets {
		if state.Stage != "ready" {
			continue
		}
		c, err := h.p.dial(ctx, h.rc, s, set, "userAdmin")
		if err != nil {
			return false, err
		}
		err = users.DeleteUser(ctx, c, "admin", identity.User)
		_ = c.Disconnect(ctx)
		if err != nil && !errors.Is(err, engine.ErrNotFound) {
			return false, err
		}
	}
	if err := h.rc.Secrets.DeleteClusterSecret(ctx, h.rc.Cluster.Ref(), identity.Secret); err != nil && !missing(err) {
		return false, err
	}
	if err := setInternal(h.rc.Cluster, "pbmRestoreIdentity", nil); err != nil {
		return false, err
	}
	return true, nil
}
