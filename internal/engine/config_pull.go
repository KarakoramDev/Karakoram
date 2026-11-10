// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// ConfigPullRevision binds a configuration to its converged, current baseline.
// The caller must independently certify that baseline before using the revision.
func ConfigPullRevision(rc *Context, member string, capabilities []string, secrets [32]byte) string {
	target, ok := rc.Backend.(infra.PlatformSnapshotter)
	if !ok {
		return ""
	}
	epoch, drift, valid := target.PlatformSnapshot().ConfigGeneration()
	if !valid {
		return ""
	}
	return configRevision(rc, member, capabilities, secrets, epoch, drift, true)
}

// ConfigChangeRevision is a notification stamp, never permission to serve 304.
// Loss of a steady baseline is itself a change requiring a prompt fresh pull.
func ConfigChangeRevision(rc *Context, member string, secrets [32]byte, steady bool) string {
	target, ok := rc.Backend.(infra.PlatformSnapshotter)
	if !ok {
		return ""
	}
	epoch, drift, _ := target.PlatformSnapshot().ConfigGeneration()
	return configRevision(rc, member, nil, secrets, epoch, drift, steady)
}

func configRevision(rc *Context, member string, capabilities []string, secrets [32]byte, epoch, drift uint64, steady bool) string {
	inputs := struct {
		Epoch, Member   string
		Cluster         domain.ClusterRef
		CreatedAt       time.Time
		Generation      int64
		Invalidation    uint64
		Mutation, Drift uint64
		Capabilities    []string
		Secrets         [32]byte
		Spec            domain.ClusterSpec
		Internal        json.RawMessage
		Layout          infra.Layout
		Phase           domain.Phase
		Steady          bool
	}{
		Epoch: rc.ObservationEpoch, Member: member, Cluster: rc.Cluster.Ref(),
		CreatedAt: rc.Cluster.CreatedAt, Generation: rc.Cluster.Generation,
		Invalidation: rc.ConfigInvalidationGeneration,
		Mutation:     epoch, Drift: drift, Capabilities: capabilities, Secrets: secrets,
		Spec: rc.Cluster.Spec, Internal: rc.Cluster.Internal,
		Layout: rc.Backend.Layout(), Phase: rc.Cluster.Status.Phase, Steady: steady,
	}
	data, err := json.Marshal(inputs)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// PullMemberConfig skips rendering only for a matching certified revision.
// Missing baselines and legacy agents always take the original fresh path.
func PullMemberConfig(requested, current string, render func() (agentapi.MemberConfig, error)) (agentapi.MemberConfig, bool, error) {
	if requested != "" && current != "" && requested == current {
		return agentapi.MemberConfig{}, true, nil
	}
	cfg, err := render()
	if err == nil {
		cfg.PullRevision = current
	}
	return cfg, false, err
}
