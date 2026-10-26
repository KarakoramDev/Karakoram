// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func recoverComponentRuntime(ctx context.Context, rc *engine.Context, m *componentMember) (bool, error) {
	tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return true, err
	}
	if tok == nil {
		return true, engine.ErrNotFound
	}
	out, err := engine.ReconcileRuntime(ctx, rc, engine.RuntimeMember{MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Resources: m.Resources, Containers: max(1, m.Replicas)}, ServiceID: &m.ServiceID, Recovery: &m.RuntimeRecovery})
	if out.Imported {
		m.Stage = engine.ComponentImported
		m.Public = false
	}
	return out.Busy, err
}
