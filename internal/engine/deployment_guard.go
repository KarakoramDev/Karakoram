// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func deploymentHash(spec infra.RuntimeSpec) string {
	raw, _ := json.Marshal(spec) // RuntimeSpec contains only JSON-compatible values.
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// DeploymentBlocked fences the same desired runtime until an operator advances
// the durable cluster epoch. A different desired runtime has its own budget.
func DeploymentBlocked(rc *Context, name string, spec infra.RuntimeSpec) bool {
	f, ok := rc.Cluster.Status.DeploymentFailures[name]
	return ok && rc.Cluster.Spec.RetryEpoch <= f.RetryEpoch && f.DesiredHash == deploymentHash(spec) && (rc.Artifact == nil || f.ArtifactIdentity == artifactIdentity(rc.Artifact))
}

func deploymentNow(rc *Context) time.Time {
	if rc.Clock != nil {
		return rc.Clock.Now()
	}
	return time.Now()
}

// RecordDeploymentExhausted retains terminal evidence and emits one event per
// member, desired runtime and epoch. Callers must preserve this state on errors.
func RecordDeploymentExhausted(ctx context.Context, rc *Context, name string, spec infra.RuntimeSpec, err error) error {
	if !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
		return err
	}
	if DeploymentBlocked(rc, name, spec) {
		return err
	}
	f := domain.DeploymentFailure{RetryEpoch: rc.Cluster.Spec.RetryEpoch, DesiredHash: deploymentHash(spec), ArtifactIdentity: artifactIdentity(rc.Artifact)}
	var exhausted *infra.DeploymentExhaustedError
	if errors.As(err, &exhausted) {
		f.Version, f.Attempts, f.LastError = exhausted.Version, exhausted.Attempts, exhausted.LastError
	}
	f.Message = fmt.Sprintf("DeploymentRetriesExhausted: member/component %s, app version %s, %d attempts; last platform error: %s; run karactl clusters retry-deployments %s --namespace %s", name, f.Version, f.Attempts, f.LastError, rc.Cluster.Name, domain.NormalizedNamespace(rc.Cluster.Namespace))
	if rc.Cluster.Status.DeploymentFailures == nil {
		rc.Cluster.Status.DeploymentFailures = make(map[string]domain.DeploymentFailure)
	}
	rc.Cluster.Status.DeploymentFailures[name] = f
	ApplyDeploymentStatus(rc, &rc.Cluster.Status)
	if rc.Events != nil {
		if eventErr := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "DeploymentRetriesExhausted", Message: f.Message}); eventErr != nil {
			return errors.Join(err, eventErr)
		}
	}
	return err
}

// ApplyDeploymentStatus preserves durable fences across provider-owned health
// snapshots and exposes each terminal member without hiding other observations.
func ApplyDeploymentStatus(rc *Context, status *domain.ClusterStatus) {
	status.DeploymentFailures = rc.Cluster.Status.DeploymentFailures
	filtered := status.Conditions[:0]
	for _, c := range status.Conditions {
		if c.Reason != "DeploymentRetriesExhausted" && c.Type != "PlatformDeploymentWaiting" {
			filtered = append(filtered, c)
		}
	}
	status.Conditions = filtered
	if rc.DeploymentWait != "" {
		status.Conditions = append(status.Conditions, domain.Condition{Type: "PlatformDeploymentWaiting", Status: "True", Reason: rc.DeploymentWaitReason, Key: rc.DeploymentWaitKey, Message: rc.DeploymentWait, LastTransitionTime: deploymentNow(rc)})
		status.Message = rc.DeploymentWait
	}
	names := make([]string, 0, len(status.DeploymentFailures))
	for name, f := range status.DeploymentFailures {
		if rc.Cluster.Spec.RetryEpoch <= f.RetryEpoch {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		f := status.DeploymentFailures[name]
		status.Conditions = append(status.Conditions, domain.Condition{Type: "Deployment/" + name, Status: "False", Reason: "DeploymentRetriesExhausted", Message: f.Message, LastTransitionTime: deploymentNow(rc)})
	}
	if len(names) > 0 {
		if status.Phase == domain.PhaseReady {
			status.Phase = domain.PhaseDegraded
		}
		status.Message = status.DeploymentFailures[names[0]].Message
	}
}

func deploymentWaiting(err error) bool {
	var waiting interface{ DeploymentWaiting() bool }
	return errors.As(err, &waiting) && waiting.DeploymentWaiting()
}

func recordDeploymentWait(ctx context.Context, rc *Context, cause error, message string) error {
	rc.DeploymentWait = message
	rc.DeploymentWaitReason, rc.DeploymentWaitKey = "PlatformDeploymentWaiting", message
	var wait *infra.DeploymentWaitError
	if errors.As(cause, &wait) {
		rc.DeploymentWaitReason, rc.DeploymentWaitKey = wait.Reason, wait.Key
	}
	for _, c := range rc.Cluster.Status.Conditions {
		if c.Type == "PlatformDeploymentWaiting" && c.Reason == rc.DeploymentWaitReason && c.Key == rc.DeploymentWaitKey {
			return nil
		}
	}
	if rc.Events != nil {
		return rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Type: "Normal", Reason: "PlatformDeploymentWaiting", Message: message})
	}
	return nil
}
