// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/version"
)

// CurrentAgentBuild is the executable deployed by this control plane.
func CurrentAgentBuild() domain.AgentBuild {
	return domain.AgentBuild{Version: version.Version, Commit: version.Commit, Build: version.BuildIdentity()}
}

// AgentUpgradePayload freezes the enqueue target. Providers must copy this target
// into their durable rolling checkpoint before their first disruption, preserve the
// engine version/runtime/volume, and use normal Deploy admission and native gates.
type AgentUpgradePayload struct {
	IncludeCurrent bool   `json:"includeCurrent"`
	TargetBuild    string `json:"targetBuild"`
	TargetVersion  string `json:"targetVersion"`
}

// AgentUpgradeTarget decodes the immutable enqueue target; it never substitutes
// the current CP build after a restart. A different running build must not deploy
// its bytes under the frozen identity: providers should fail before disruption.
func AgentUpgradeTarget(op *domain.Operation) (AgentUpgradePayload, error) {
	var target AgentUpgradePayload
	if op == nil {
		return target, errors.New("agent upgrade operation is required")
	}
	if err := json.Unmarshal(op.Payload, &target); err != nil {
		return target, err
	}
	if target.TargetBuild == "" {
		return target, errors.New("agent upgrade target build is required")
	}
	return target, nil
}

// MemberNeedsAgentUpgrade selects a rollout candidate. Providers must journal
// completion per member: includeCurrent selects every member initially, but a
// checkpoint's completed members must never be selected again.
func MemberNeedsAgentUpgrade(member domain.MemberStatus, target AgentUpgradePayload) bool {
	return target.IncludeCurrent || !member.Agent.Healthy || member.Agent.Build == "" || member.Agent.Build != target.TargetBuild
}

// ObserveAgentVersions adds informational fleet inventory, never changing phase,
// member health, or readiness conditions. Unreachable/stopped and legacy agents
// are unknown; display versions do not replace executable identity comparisons.
func ObserveAgentVersions(status *domain.ClusterStatus, target domain.AgentBuild, at time.Time) {
	inventory := &domain.AgentRolloutStatus{TargetBuild: target.Build, TargetVersion: target.Version, Outdated: []string{}, Unknown: []string{}}
	for _, member := range status.Members {
		if !member.Agent.Healthy || member.Agent.Build == "" {
			inventory.Unknown = append(inventory.Unknown, member.Name)
		} else if member.Agent.Build != target.Build {
			inventory.Outdated = append(inventory.Outdated, member.Name)
		}
	}
	slices.Sort(inventory.Outdated)
	slices.Sort(inventory.Unknown)
	status.Agents = inventory
	condition := domain.Condition{Type: "AgentsCurrent", Status: "True", Reason: "Current", Message: "All member agents report the current control-plane build", LastTransitionTime: at}
	if len(inventory.Outdated) > 0 {
		condition.Status, condition.Reason, condition.Message = "False", "Outdated", "Member agents differ from the current control-plane build"
	} else if len(inventory.Unknown) > 0 {
		condition.Status, condition.Reason, condition.Message = "Unknown", "Unreported", "Some member agents have no reachable build observation"
	}
	for i, old := range status.Conditions {
		if old.Type == condition.Type {
			if old.Status == condition.Status && old.Reason == condition.Reason {
				condition.LastTransitionTime = old.LastTransitionTime
			}
			status.Conditions[i] = condition
			return
		}
	}
	status.Conditions = append(status.Conditions, condition)
}
