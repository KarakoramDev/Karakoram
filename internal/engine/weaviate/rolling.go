// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"reflect"
	"slices"
	"sort"
)

type rollingTarget struct {
	Config    agentapi.MemberConfig `json:"config"`
	Resources domain.Resources      `json:"resources"`
}

func (p *Provider) startRolling(ctx context.Context, rc *engine.Context, s *state, reason, operationID, only string) error {
	if s.Rolling != nil {
		return nil
	}
	members := ordered(s)
	sort.SliceStable(members, func(i, j int) bool {
		return !slices.Contains(s.Voters, members[i].Name) && slices.Contains(s.Voters, members[j].Name)
	})
	r := &rolling{Reason: reason, OperationID: operationID, Step: "gate"}
	for _, m := range members {
		if only != "" && m.Name != only {
			continue
		}
		cfg, e := p.desiredConfig(ctx, rc, s, m)
		if e != nil {
			return e
		}
		if reason == "config" && configHash(cfg) == m.ConfigHash && reflect.DeepEqual(m.Resources, rc.Cluster.Spec.Components.Engine.Resources) {
			continue
		}
		raw, e := json.Marshal(rollingTarget{Config: cfg, Resources: rc.Cluster.Spec.Components.Engine.Resources})
		if e != nil {
			return e
		}
		if e = putSecret(ctx, rc, "target/"+m.Name, raw); e != nil {
			return e
		}
		r.Order = append(r.Order, m.Name)
	}
	s.Rolling = r
	s.Phase = "rolling"
	return nil
}

// rollingClusterReady relaxes time verification only for active leaves that
// this same durable roll will replace with valid leaves. This includes peers
// still waiting their turn, so simultaneous expiry can recover sequentially.
func (p *Provider) rollingClusterReady(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Rolling == nil {
		return p.clusterReady(ctx, rc, s)
	}
	expired, err := expiredActiveLeaves(ctx, rc, s)
	if err != nil {
		return false, err
	}
	for name, activeDER := range expired {
		if !slices.Contains(s.Rolling.Order[s.Rolling.Position:], name) {
			return false, fmt.Errorf("native certificate expired for member %s outside replacement roll", name)
		}
		raw, err := secret(ctx, rc, "target/"+name)
		if err != nil {
			return false, err
		}
		var target rollingTarget
		if err = json.Unmarshal(raw, &target); err != nil {
			return false, err
		}
		block, rest := pem.Decode([]byte(target.Config.TLS.CertPEM))
		if block == nil || target.Config.TLS.Mode == "disabled" {
			return false, fmt.Errorf("native certificate expired for member %s without replacement leaf", name)
		}
		leaf, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return false, err
		}
		if bytes.Equal(leaf.Raw, activeDER) {
			return false, fmt.Errorf("native certificate expired for member %s without replacement leaf", name)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(target.Config.TLS.CAPEM)) {
			return false, fmt.Errorf("invalid replacement CA for member %s", name)
		}
		intermediates := x509.NewCertPool()
		intermediates.AppendCertsFromPEM(rest)
		if _, err = leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, DNSName: name, CurrentTime: now(rc)}); err != nil {
			return false, fmt.Errorf("invalid replacement leaf for member %s: %w", name, err)
		}
	}
	return p.observeClusterReady(ctx, rc, s, expired)
}

func (p *Provider) rollingStep(ctx context.Context, rc *engine.Context, s *state) error {
	r := s.Rolling
	if r == nil {
		return nil
	}
	if r.Position == len(r.Order) {
		s.Rolling = nil
		return nil
	}
	m := s.find(r.Order[r.Position])
	if m == nil {
		return fmt.Errorf("rolling member is missing")
	}
	raw, e := secret(ctx, rc, "target/"+m.Name)
	if e != nil {
		return e
	}
	var target rollingTarget
	if e = json.Unmarshal(raw, &target); e != nil {
		return e
	}
	switch r.Step {
	case "gate":
		ready, e := p.rollingClusterReady(ctx, rc, s)
		if e != nil || !ready {
			return e
		}
		r.Step = "activate"
		return nil
	case "activate":
		ready, e := p.rollingClusterReady(ctx, rc, s)
		if e != nil || !ready {
			return e
		}
		if e = restartConfig(&target.Config, s.ConfigVersion+1); e != nil {
			return e
		}
		if e = activateConfig(ctx, rc, s, m, target.Config); e != nil {
			return e
		}
		r.Step = "resources"
		return nil
	case "resources":
		if !reflect.DeepEqual(m.Resources, target.Resources) {
			diskChanged := m.Resources.MinDisk != target.Resources.MinDisk || m.Resources.MaxDisk != target.Resources.MaxDisk
			if diskChanged {
				if e = engine.RequireDiskGrowth(rc.Backend); e != nil {
					return e
				}
				m.Resources = target.Resources
				s.Pending = &pending{Action: "volume-resources", Member: m.Name}
				r.Step = "runtime-resources"
				return nil
			}
			m.Resources = target.Resources
			s.Pending = &pending{Action: "resources", Member: m.Name}
			r.Step = "deploy"
			return nil
		}
		r.Step = "deploy"
		return nil
	case "runtime-resources":
		s.Pending = &pending{Action: "resources", Member: m.Name}
		r.Step = "deploy"
		return nil
	case "deploy":
		// A new binary must be installed before the agent pulls its new configuration.
		s.Pending = &pending{Action: "deploy", Member: m.Name}
		r.Step = "ack"
		return nil
	case "ack":
		if _, _, e = p.memberReady(ctx, rc, m); e != nil {
			return nil
		}
		c, e := p.native(ctx, rc, s, m.Name)
		if e != nil {
			return e
		}
		meta, e := c.Meta(ctx)
		c.Close()
		if e != nil {
			return e
		}
		if meta.Version != m.DesiredVersion {
			return nil
		}
		ready, e := p.rollingClusterReady(ctx, rc, s)
		if e != nil || !ready {
			return e
		}
		m.Version = m.DesiredVersion
		m.Stage = "ready"
		r.Position++
		r.Step = "gate"
		return nil
	default:
		return fmt.Errorf("unknown rolling step %q", r.Step)
	}
}
func (*Provider) CanClaimOperation(c *domain.Cluster) (bool, error) {
	s, e := load(c)
	if e != nil {
		return false, e
	}
	if s.Pending != nil || s.Rolling != nil || s.ScaleDown != nil || s.Rotation != nil {
		return false, nil
	}
	for _, m := range s.Members {
		if m.ImportPlan != nil || m.RuntimeRecovery.Op != nil || m.RuntimeRecovery.ImportPlan != nil || m.UnplacedRetirement != nil {
			return false, nil
		}
	}
	return true, nil
}
func (*Provider) SyncDuringOperation(*domain.Operation) bool { return false }
