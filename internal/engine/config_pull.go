// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// ConfigNoticeContentSource derives config-relevant content without platform or
// native-agent I/O. An empty result deliberately retains conservative notices.
// This is a notification hint, never permission to skip fresh config rendering.
type ConfigNoticeContentSource interface {
	MemberConfigNoticeContent(context.Context, *Context, string) (string, error)
}

// MemberConfigContentRevision hashes a canonical notification-only rendering.
func MemberConfigContentRevision(cfg agentapi.MemberConfig) (string, error) {
	cfg.PullRevision, cfg.ChangeRevision, cfg.ContentRevision = "", "", ""
	raw, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

var noticeCapabilities = [...]string{
	agentapi.ConfigCapabilityGaleraListenHost, agentapi.ConfigCapabilityMySQLListenHost,
	agentapi.ConfigCapabilityTemporalIPv6, agentapi.ConfigCapabilityPostgresLocalPeer,
}

// StoredConfigNoticeContent is for providers declaring a storage-only renderer.
// Providers with live rendering gates must supply their own notice-only renderer.
func StoredConfigNoticeContent(ctx context.Context, source AgentConfigSource, rc *Context, member string) (string, error) {
	if fresh, ok := source.(FreshMemberConfigSource); ok {
		required, err := fresh.RequiresFreshMemberConfig(ctx, rc, member)
		if err != nil || required {
			return "", err
		}
	}
	canonical := *rc
	canonical.AgentConfigCapabilities = noticeCapabilities[:]
	cfg, err := source.MemberConfig(ctx, &canonical, member)
	if err != nil {
		return "", err
	}
	return MemberConfigContentRevision(cfg)
}

// ConfigNoticeRevisions excludes observation journals and phase, retaining
// listing drift and explicit force intent without target mutation fan-out.
func ConfigNoticeRevisions(rc *Context, payload string, steady bool) (change, content string) {
	target, ok := rc.Backend.(infra.PlatformSnapshotter)
	if !ok || payload == "" {
		return "", ""
	}
	_, drift, _ := target.PlatformSnapshot().ConfigGeneration()
	hash := func(force uint64, eligible bool) string {
		raw, _ := json.Marshal(struct {
			Payload, Epoch string
			Drift, Force   uint64
			Steady         bool
		}{payload, rc.ObservationEpoch, drift, force, eligible})
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:])
	}
	return hash(rc.ConfigInvalidationGeneration, steady), hash(0, true)
}

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
	return configRevision(rc, member, capabilities, secrets, epoch, drift, true, rc.ConfigInvalidationGeneration)
}

// ConfigChangeRevision is a notification stamp, never permission to serve 304.
// Loss of a steady baseline changes the stamp, independently of notice scheduling.
func ConfigChangeRevision(rc *Context, member string, secrets [32]byte, steady bool) string {
	target, ok := rc.Backend.(infra.PlatformSnapshotter)
	if !ok {
		return ""
	}
	epoch, drift, _ := target.PlatformSnapshot().ConfigGeneration()
	return configRevision(rc, member, nil, secrets, epoch, drift, steady, rc.ConfigInvalidationGeneration)
}

// ConfigRevisions captures one target proof for the payload validator and its
// notification acknowledgements. Content excludes force/eligibility changes.
func ConfigRevisions(rc *Context, member string, capabilities []string, secrets [32]byte, steady bool) (inputs, change, content string) {
	target, ok := rc.Backend.(infra.PlatformSnapshotter)
	if !ok {
		return "", "", ""
	}
	epoch, drift, valid := target.PlatformSnapshot().ConfigGeneration()
	if valid {
		inputs = configRevision(rc, member, capabilities, secrets, epoch, drift, true, rc.ConfigInvalidationGeneration)
	}
	change = configRevision(rc, member, nil, secrets, epoch, drift, steady, rc.ConfigInvalidationGeneration)
	content = configRevision(rc, member, nil, secrets, epoch, drift, true, 0)
	return inputs, change, content
}

func configRevision(rc *Context, member string, capabilities []string, secrets [32]byte, epoch, drift uint64, steady bool, invalidation uint64) string {
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
		Invalidation: invalidation,
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
