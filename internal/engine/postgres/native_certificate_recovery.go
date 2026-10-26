// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

// Recover reloadable native identities independently of proxy writer health:
// verified proxy monitor connections need these identities to become healthy.
// This path publishes only the member's already applied configuration and a new
// certificate, never a desired GUC/resource/version change or database restart.
func (p *Core) recoverNativeCertificates(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Suspended || s.Pending != nil || s.Upgrade != nil || s.Rolling != nil || s.Replacement != nil || s.RestoreRecovery != nil {
		return false, nil
	}
	var target *member
	for _, m := range s.Members {
		if m.Offline || m.Removing || m.Retirement != "" {
			return false, nil
		}
		if m.Stage != "joined" && m.Stage != "deployed" && m.Stage != "deploying" {
			continue
		}
		if m.TLSReloadPending {
			target = m
			break
		}
		if !m.TLSPending && m.TLSReloadBlocked == "" {
			continue
		}
		active, err := secret(ctx, rc, "tls/servers/"+m.Name+".crt")
		if err != nil {
			return true, err
		}
		unusable, err := pki.NeedsRenewal(active, 0, now(rc))
		if err != nil {
			return true, err
		}
		if unusable {
			block, _ := pem.Decode(active)
			if block == nil {
				return true, fmt.Errorf("member %s TLS certificate is not PEM", m.Name)
			}
			leaf, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return true, err
			}
			if now(rc).Before(leaf.NotBefore) {
				rc.SyncWait = fmt.Sprintf("member %s TLS certificate not valid until %s; reissuing", m.Name, leaf.NotBefore.UTC().Format(time.RFC3339))
			} else {
				rc.SyncWait = fmt.Sprintf("member %s TLS certificate expired at %s; reissuing", m.Name, leaf.NotAfter.UTC().Format(time.RFC3339))
			}
			target = m
			break
		}
		// A validity-based hold is obsolete once the active identity is valid,
		// even if no replacement receipt was ever created.
		m.TLSReloadBlocked = ""
	}
	if target == nil {
		return false, nil
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil || bs.ManagedRolesHeld || bs.Restore != nil {
		return false, err
	}
	// Check cluster-wide local controls, not only the receiving member. Never
	// use identity recovery to bypass an operator hold.
	targetReloadOnly := false
	for _, m := range s.Members {
		if m.Offline || m.Removing || m.Retirement != "" {
			return true, nil
		}
		if m.Stage != "joined" && m.Stage != "deployed" && m.Stage != "deploying" {
			continue
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return true, err
		}
		status, err := engine.ObserveAgentStatus(ctx, a, "PostgreSQL", m.Name)
		a.Close()
		if err != nil {
			return true, err
		}
		if status.Maintenance {
			return true, nil
		}
		if m == target {
			targetReloadOnly = status.CertificateReloadOnly && m.Role == "data"
		}
		// New agents have an explicit identity-only path. Older HA agents use
		// Patroni reload without making a postmaster restart decision; older
		// single agents require the safety check below.
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), target.Name)
	if err != nil {
		return true, err
	}
	defer a.Close()
	if target.TLSReloadPending {
		status, err := engine.ObserveAgentStatus(ctx, a, "PostgreSQL", target.Name)
		if err != nil {
			return true, err
		}
		if !target.TLSPending && status.ConfigVersion >= target.TLSReloadVersion {
			// The receipt revision was published and accepted. Keep it as the
			// member's publication floor when the temporary override is cleared.
			// Applied options/version/resources still describe the frozen payload.
			target.ConfigVersion = max(target.ConfigVersion, target.TLSReloadVersion)
			target.TLSReloadPending = false
			target.TLSReloadVersion = 0
			target.TLSReloadOnly = false
			target.TLSReloadBlocked = ""
			// Do not mark desired options/version as applied by an identity-only
			// pull. A strictly newer revision resumes the ordinary rollout.
			s.ConfigVersion++
			return true, nil
		}
	} else {
		// Persist the reload receipt before activation so a lost publication or
		// reload response is retried without depending on proxy health.
		target.TLSReloadBlocked = ""
		if target.Role == "data" && target.Topology == "single" && !targetReloadOnly {
			st, err := live(ctx, rc, target.Name)
			if err != nil || !st.Running || st.PendingRestart {
				target.TLSReloadBlocked = fmt.Sprintf("member %s requires a reload-only capable agent for TLS recovery while native restart requirements are unavailable or a postmaster restart is pending", target.Name)
				rc.SyncWait = target.TLSReloadBlocked
				return true, nil
			}
		}
		s.ConfigVersion++
		target.TLSReloadVersion = s.ConfigVersion
		target.TLSReloadPending = true
		target.TLSReloadOnly = targetReloadOnly
		if err := save(rc.Cluster, s); err != nil {
			return true, err
		}
	}
	if err := activateCertificates(ctx, rc, target); err != nil {
		return true, err
	}
	if err := save(rc.Cluster, s); err != nil {
		return true, err
	}
	return true, a.Reload(ctx)
}

// Report the stored identity, not a failed network probe, as the recovery cause.
func memberCertificateCondition(ctx context.Context, rc *engine.Context, s *state) (*domain.Condition, error) {
	for _, m := range s.Members {
		if m.TLSReloadBlocked != "" {
			return &domain.Condition{Type: "MemberTLSReady", Status: "False", Reason: "CertificateRecoveryBlocked", Message: m.TLSReloadBlocked, LastTransitionTime: now(rc)}, nil
		}
		for _, kind := range []string{"agents", "servers"} {
			active, err := secret(ctx, rc, "tls/"+kind+"/"+m.Name+".crt")
			if missing(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			block, _ := pem.Decode(active)
			if block == nil {
				return nil, fmt.Errorf("member %s TLS certificate is not PEM", m.Name)
			}
			leaf, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, err
			}
			condition := &domain.Condition{Type: "MemberTLSReady", Status: "False", LastTransitionTime: now(rc)}
			if now(rc).Before(leaf.NotBefore) {
				condition.Reason = "CertificateNotYetValid"
				condition.Message = fmt.Sprintf("member %s %s TLS certificate not valid until %s; reissuing", m.Name, kind, leaf.NotBefore.UTC().Format(time.RFC3339))
				return condition, nil
			}
			if !now(rc).Before(leaf.NotAfter) {
				condition.Reason = "CertificateExpired"
				condition.Message = fmt.Sprintf("member %s %s TLS certificate expired at %s; reissuing", m.Name, kind, leaf.NotAfter.UTC().Format(time.RFC3339))
				return condition, nil
			}
		}
		if m.TLSReloadPending {
			return &domain.Condition{Type: "MemberTLSReady", Status: "False", Reason: "CertificateReloadPending", Message: fmt.Sprintf("member %s replacement TLS certificate published; waiting for agent acknowledgement", m.Name), LastTransitionTime: now(rc)}, nil
		}
	}
	return nil, nil
}
