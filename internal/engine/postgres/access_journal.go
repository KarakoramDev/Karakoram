// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// Both the plaintext and verifier are sealed before SQL can commit. Replaying
// uses exactly the same verifier, password, managed-role index and revision.
// Nothing credential-bearing is placed in Cluster.Internal or operation status.
type accessIntent struct {
	Request   pgspec.AccessRequest `json:"request"`
	Password  *string              `json:"password,omitempty"`
	Names     []string             `json:"names"`
	Revision  string               `json:"revision"`
	Generated bool                 `json:"generated,omitempty"`
	Previous  map[string][]byte    `json:"previous,omitempty"`
}

type accessRejection struct {
	result pgspec.AccessResult
	role   string
}

func (e *accessRejection) Error() string {
	return "credential change rejected (" + e.result.Code + "): " + e.result.Message
}
func (e *accessRejection) Unwrap() error {
	switch e.result.Status {
	case http.StatusNotFound:
		return &domain.ResourceError{Resource: "postgres-role", Name: e.role, Reason: domain.ReasonNotFound, State: e.result.Code, Message: e.result.Message, Err: engine.ErrNotFound}
	case http.StatusConflict:
		return domain.Conflict("postgres-role", e.role, e.result.Code, engine.ErrConflict, "%s", e.result.Message)
	case http.StatusServiceUnavailable:
		return engine.ErrUnavailable
	default:
		return engine.Invalid("%s", e.result.Message)
	}
}
func definitiveAccessRejection(err error) bool {
	var rejected *accessRejection
	return errors.As(err, &rejected)
}

func loadAccessIntent(ctx context.Context, rc *engine.Context) (*accessIntent, error) {
	raw, err := secret(ctx, rc, "access/pending")
	if missing(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var intent accessIntent
	if err = json.Unmarshal(raw, &intent); err != nil {
		return nil, fmt.Errorf("decode pending credential intent: %w", err)
	}
	if intent.Request.Role == nil || intent.Revision == "" {
		return nil, engine.Invalid("invalid pending credential intent; preserve the sealed intent and recover its metadata")
	}
	return &intent, nil
}

func accessIntentPending(ctx context.Context, rc *engine.Context) (bool, error) {
	intent, err := loadAccessIntent(ctx, rc)
	return intent != nil, err
}

func accessSnapshot(ctx context.Context, rc *engine.Context, name string) (map[string][]byte, error) {
	previous := make(map[string][]byte, 3)
	for _, key := range []string{rolePasswordKey(name), "access/roles", "access/revision"} {
		value, err := secret(ctx, rc, key)
		if missing(err) {
			previous[key] = nil
		} else if err != nil {
			return nil, err
		} else {
			previous[key] = value
		}
	}
	return previous, nil
}

func clearAccessIntent(ctx context.Context, rc *engine.Context) error {
	if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "access/pending"); err != nil && !missing(err) {
		return err
	}
	return nil
}

func (p *Core) publishAccessIntent(ctx context.Context, rc *engine.Context, intent *accessIntent) error {
	if intent.Request.Binding && intent.Request.Operation == "delete" {
		for _, key := range []string{rolePasswordKey(intent.Request.Role.Name), "bindings/roles/" + intent.Request.Role.Name} {
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
				return err
			}
		}
	}
	if intent.Password != nil {
		if err := putSecret(ctx, rc, rolePasswordKey(intent.Request.Role.Name), []byte(*intent.Password)); err != nil {
			return fmt.Errorf("sealing selected role password: %w", err)
		}
	}
	names, err := json.Marshal(intent.Names)
	if err != nil {
		return err
	}
	if err = putSecret(ctx, rc, "access/roles", names); err != nil {
		return fmt.Errorf("sealing managed role index: %w", err)
	}
	return putSecret(ctx, rc, "access/revision", []byte(intent.Revision))
}

func (p *Core) retireRejectedAccess(ctx context.Context, rc *engine.Context, intent *accessIntent, result pgspec.AccessResult) error {
	// A role can have been replaced after SQL committed and only part of the
	// proxy metadata was published. Restore the pre-intent sealed snapshot too.
	for key, value := range intent.Previous {
		var err error
		if value == nil {
			err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key)
			if missing(err) {
				err = nil
			}
		} else {
			err = putSecret(ctx, rc, key, value)
		}
		if err != nil {
			return fmt.Errorf("rolling back rejected credential metadata: %w", err)
		}
	}
	if err := clearAccessIntent(ctx, rc); err != nil {
		return err
	}
	return &accessRejection{result: result, role: intent.Request.Role.Name}
}

func (p *Core) completeAccessIntent(ctx context.Context, rc *engine.Context, intent *accessIntent) error {
	request := intent.Request
	if request.RequestID == "" {
		supported, err := p.credentialReceipts(ctx, rc)
		if err != nil {
			return err
		}
		if !supported {
			// Recover pre-receipt intents on old members without introducing
			// name-only CREATE adoption. A duplicate stays a conflict.
			if err = p.accessAction(ctx, rc, "roles", request, nil); err != nil {
				if errors.Is(err, engine.ErrConflict) || errors.Is(err, engine.ErrNotFound) || errors.Is(err, engine.ErrInvalid) {
					status := http.StatusBadRequest
					if errors.Is(err, engine.ErrConflict) {
						status = http.StatusConflict
					}
					if errors.Is(err, engine.ErrNotFound) {
						status = http.StatusNotFound
					}
					return p.retireRejectedAccess(ctx, rc, intent, pgspec.AccessResult{Outcome: "rejected", Code: "legacy_transaction_rejected", Message: "Legacy PostgreSQL rejected credential recovery; no existing role was adopted", Status: status})
				}
				return err
			}
			if err = p.publishAccessIntent(ctx, rc, intent); err != nil {
				return err
			}
			return clearAccessIntent(ctx, rc)
		}
	}
	// Revision also gives pre-receipt sealed intents a stable operation identity.
	// Never recover an old CREATE by converting it to name-only UPDATE.
	if request.RequestID == "" {
		request.RequestID = intent.Revision
	}
	request.RoleOperation, request.Operation = request.Operation, "journal"
	var result pgspec.AccessResult
	if err := p.accessAction(ctx, rc, "roles", request, &result); err != nil {
		return err // No legacy fallback after a durable journal owns the write.
	}
	switch result.Outcome {
	case "rejected":
		return p.retireRejectedAccess(ctx, rc, intent, result)
	case "applied":
		if result.RoleOID == 0 {
			return fmt.Errorf("credential receipt has no native role identity: %w", engine.ErrUnavailable)
		}
	case "unknown", "":
		return fmt.Errorf("credential transaction outcome is unknown; replaying the same sealed intent: %w", engine.ErrUnavailable)
	default:
		return fmt.Errorf("unsupported credential transaction outcome: %w", engine.ErrUnavailable)
	}
	if intent.Request.Binding && intent.Request.Operation == "create" {
		raw, err := json.Marshal(intent.Request)
		if err != nil {
			return err
		}
		if err := putSecret(ctx, rc, "bindings/roles/"+intent.Request.Role.Name, raw); err != nil {
			return err
		}
	}
	if err := p.publishAccessIntent(ctx, rc, intent); err != nil {
		return err
	}
	return clearAccessIntent(ctx, rc)
}

func (p *Core) reconcileAccessIntent(ctx context.Context, rc *engine.Context) (bool, error) {
	p.accessMu.Lock()
	defer p.accessMu.Unlock()
	intent, err := loadAccessIntent(ctx, rc)
	if err != nil || intent == nil {
		return intent != nil, err
	}
	if err = p.completeAccessIntent(ctx, rc, intent); err != nil {
		if definitiveAccessRejection(err) {
			return true, err
		}
		return true, fmt.Errorf("managed credential recovery pending; restore PostgreSQL and secret-store availability: %w", err)
	}
	return true, nil
}

func (p *Core) finishPendingAccess(ctx context.Context, rc *engine.Context) error {
	intent, err := loadAccessIntent(ctx, rc)
	if err != nil || intent == nil {
		return err
	}
	err = p.completeAccessIntent(ctx, rc, intent)
	if definitiveAccessRejection(err) {
		return nil
	}
	return err
}

func (p *Core) credentialReceipts(ctx context.Context, rc *engine.Context) (bool, error) {
	var capabilities pgspec.AccessCapabilities
	err := p.accessAction(ctx, rc, "roles", pgspec.AccessRequest{Operation: "capabilities"}, &capabilities)
	// Previous releases validate this unknown operation before reaching SQL.
	if errors.Is(err, engine.ErrInvalid) {
		return false, nil
	}
	return capabilities.CredentialReceipts, err
}
