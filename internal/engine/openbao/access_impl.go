// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

var _ Access = (*Provider)(nil)

func (*Provider) RevealUnsealKeys(ctx context.Context, rc *engine.Context) (UnsealKeys, error) {
	v, err := readUnseal(ctx, rc)
	if missing(err) {
		return UnsealKeys{}, engine.ErrUnavailable
	}
	if err != nil {
		return UnsealKeys{}, err
	}
	return UnsealKeys{SealType: v.SealType, RecoveryShares: v.Recovery, Threshold: v.Threshold, Shares: v.Shares, InitialRootToken: v.RootToken, InitializedAt: v.InitializedAt}, nil
}
func (*Provider) ListDatabaseConnections(ctx context.Context, rc *engine.Context) ([]DatabaseConnection, error) {
	b, err := secret(ctx, rc, SecretConnections)
	if missing(err) {
		return []DatabaseConnection{}, nil
	}
	if err != nil {
		return nil, err
	}
	var out []DatabaseConnection
	if json.Unmarshal(b, &out) != nil {
		return nil, engine.ErrUnavailable
	}
	return out, nil
}
func (p *Provider) ConnectionTargets(ctx context.Context, rc *engine.Context) ([]domain.ClusterRef, error) {
	connections, err := p.ListDatabaseConnections(ctx, rc)
	if err != nil {
		return nil, err
	}
	var out []domain.ClusterRef
	seen := map[string]bool{}
	for _, c := range connections {
		if !seen[c.TargetCluster] {
			seen[c.TargetCluster] = true
			out = append(out, domain.ClusterRef{Namespace: rc.Cluster.Ref().Namespace, Name: c.TargetCluster})
		}
	}
	return out, nil
}
