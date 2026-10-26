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
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// RuntimeRecovery is a member's durable, bounded infrastructure repair journal.
// Engine operations and cleanup must exclude this reconciler while they own a stop.
type RuntimeRecovery struct {
	RetrySchedule
	Op          *infra.Op   `json:"op,omitempty"`
	Action      string      `json:"action,omitempty"`
	ImportPlan  *ImportPlan `json:"importPlan,omitempty"`
	RetryAt     time.Time   `json:"retryAt,omitempty"`
	Initialized bool        `json:"initialized,omitempty"`
	VolumeLost  bool        `json:"volumeLost,omitempty"`
}

// RuntimeMember supplies persisted identity, not hostname-based adoption.
type RuntimeMember struct {
	MemberService
	ServiceID *string
	VolumeID  string
	Recovery  *RuntimeRecovery
}

type RuntimeRecoveryResult struct {
	Busy       bool
	Imported   bool
	VolumeLost bool
}

// ObserveRuntimeRecovery drains accepted work before another lifecycle owner can
// act. In particular, absent inventory never proves an accepted import finished.
func ObserveRuntimeRecovery(ctx context.Context, rc *Context, m RuntimeMember) (RuntimeRecoveryResult, error) {
	q := m.Recovery
	if q == nil || q.Op == nil && q.ImportPlan == nil {
		return RuntimeRecoveryResult{}, nil
	}
	if rc.Cluster.Deleting && q.Op != nil && CleanupSupersedesDeployment(rc.Backend, *q.Op) {
		q.Op, q.Action = nil, ""
		return RuntimeRecoveryResult{}, nil
	}
	result := RuntimeRecoveryResult{Busy: true}
	if q.Op == nil {
		now := time.Now().UTC()
		if rc.Clock != nil {
			now = rc.Clock.Now()
		}
		if q.Waiting(now, q.RetryAt) {
			return result, nil
		}
		q.RetryAt = q.Schedule(now, now.Add(time.Minute))
		if rc.Events != nil {
			if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Resource: m.Member, Type: "Normal", Reason: "RuntimeRecovery", Message: fmt.Sprintf("Recovering desired-running member %s: import", m.Member), CreatedAt: now}); err != nil {
				return result, err
			}
		}
		op, err := rc.Backend.ImportServices(ctx, q.ImportPlan.Services(rc.Backend.Layout(), m.Cluster, m.Member, m.Token))
		if err != nil {
			if reset := infra.RetryAt(err); reset.After(q.RetryAt) {
				q.RetryAt = q.Schedule(now, reset)
			}
			return result, err
		}
		q.Op, q.Action = &op, "import"
		return result, nil
	}
	if q.Action == "import" && q.ImportPlan != nil {
		op, err := ResumeImportPlan(ctx, rc, q.ImportPlan, m.Member, m.Token)
		if err != nil {
			return result, err
		}
		if op != nil {
			q.Op = op
			return result, nil
		}
	}
	st, err := rc.Backend.OpStatus(ctx, *q.Op)
	if err != nil {
		return result, err
	}
	if !st.Done && !st.Failed {
		return result, nil
	}
	if st.Failed {
		action := q.Action
		q.Op, q.Action = nil, ""
		return result, fmt.Errorf("runtime recovery %s failed for %s: %s", action, m.Member, st.Message)
	}
	if q.Action == "import" {
		runtime, err := rc.Backend.GetService(ctx, m.Member)
		if err != nil || runtime == nil {
			return result, err
		}
		id := ImportedServiceID(*q.Op, st, runtime)
		if runtime.Kind != infra.KindRuntime || runtime.ID != id && !HasOwnership(runtime, m.Token, m.Cluster, m.Member) {
			return result, UnverifiedOwnership(runtime)
		}
		*m.ServiceID = runtime.ID
		result.Imported = true
		q.ImportPlan = nil
	}
	q.Op, q.Action = nil, ""
	return result, nil
}

// ReconcileRuntime restores running intent without creating storage. Call only
// for deployed, desired-running members, outside intentional stop/restore work.
// Derived-resource restoration belongs to the backend: transitional observations
// are left alone. Only confirmed logical absence permits a runtime-only import.
func ReconcileRuntime(ctx context.Context, rc *Context, m RuntimeMember) (RuntimeRecoveryResult, error) {
	var result RuntimeRecoveryResult
	if m.Recovery != nil && (m.Recovery.Op != nil || m.Recovery.ImportPlan != nil) {
		return ObserveRuntimeRecovery(ctx, rc, m)
	}
	if rc.Cluster.Deleting {
		return result, nil
	}
	now := time.Now().UTC()
	if rc.Clock != nil {
		now = rc.Clock.Now()
	}
	if m.Recovery == nil || m.ServiceID == nil {
		return result, fmt.Errorf("runtime recovery requires a persistent member journal")
	}
	q := m.Recovery
	runtime, err := rc.Backend.GetService(ctx, m.Member)
	if err != nil {
		if ConfirmedVolumeLoss(err, m.Volume, m.VolumeID) {
			q.VolumeLost = true
			result.VolumeLost = true
			return result, nil
		}
		return result, err
	}
	if runtime != nil && runtime.Kind != infra.KindRuntime {
		return result, fmt.Errorf("%w: %s is not a runtime", ErrConflict, m.Member)
	}
	if runtime != nil && runtime.ID != *m.ServiceID && !HasOwnership(runtime, m.Token, m.Cluster, m.Member) {
		return result, UnverifiedOwnership(runtime)
	}
	if runtime != nil {
		*m.ServiceID = runtime.ID
	}
	if m.Volume != "" {
		volume, err := rc.Backend.GetService(ctx, m.Volume)
		if err != nil {
			if ConfirmedVolumeLoss(err, m.Volume, m.VolumeID) {
				q.VolumeLost = true
				result.VolumeLost = true
				return result, nil
			}
			return result, err
		}
		if volume == nil {
			q.VolumeLost = true
			result.VolumeLost = true
			return result, nil
		}
		// A marker proves ownership, not the data incarnation of an old volume.
		if volume.Kind != infra.KindVolume || m.VolumeID != "" && volume.ID != m.VolumeID || m.VolumeID == "" && !HasOwnership(volume, m.Token, m.Cluster, m.Member) {
			q.VolumeLost = true
			return result, fmt.Errorf("%w: volume %s identity changed or is unverified; restore the original retained volume before recovering %s", ErrConflict, m.Volume, m.Member)
		}
		if volume.Status != infra.StatusActive && volume.Status != infra.StatusRunning {
			result.Busy = true
			return result, nil
		}
		if runtime != nil && runtime.Status != infra.StatusStopped && rc.Agents != nil {
			probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			client, dialErr := rc.Agents.Dial(probeCtx, rc.Cluster.Ref(), m.Member)
			if dialErr == nil {
				status, statusErr := client.Status(probeCtx)
				client.Close()
				cancel()
				if statusErr == nil && status.Volume != nil {
					identity := status.Volume.Identity
					if identity.Member != m.Member || identity.Volume != m.Volume || identity.VolumeID != m.VolumeID {
						return result, fmt.Errorf("%w: agent volume identity for %s does not match its retained volume", ErrConflict, m.Member)
					}
					if status.Volume.Lost || status.Volume.Verified {
						q.VolumeLost = status.Volume.Lost
					}
					if status.Volume.Lost {
						result.VolumeLost = true
						return result, nil
					}
					if status.Volume.Verified {
						for name, process := range status.Processes {
							if name != "pbm-agent" && process.State == "running" {
								q.Initialized = true
							}
						}
					}
				}
			} else {
				cancel()
			}
		}
	}
	if runtime != nil {
		*m.ServiceID = runtime.ID
		if runtime.Status != infra.StatusStopped && !runtime.Importing {
			return result, nil
		}
	}
	result.Busy = true
	if (runtime == nil || runtime.Importing) && q.ImportPlan == nil {
		q.ImportPlan = NewImportPlan([]infra.ServiceSpec{RuntimeServiceSpec(rc.Backend.Layout(), m.MemberService)})
		return result, nil // Commit the immutable batch before submission.
	}
	if q.Waiting(now, q.RetryAt) {
		return result, nil
	}
	// Persisted even on a failed/ambiguous submission; no tight restart loop.
	q.RetryAt = q.Schedule(now, now.Add(time.Minute))
	action := "start"
	if runtime == nil || runtime.Importing {
		action = "import"
	}
	if rc.Events != nil {
		if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Resource: m.Member, Type: "Normal", Reason: "RuntimeRecovery", Message: fmt.Sprintf("Recovering desired-running member %s: %s", m.Member, action), CreatedAt: now}); err != nil {
			return result, err
		}
	}
	var op infra.Op
	if action == "import" {
		op, err = rc.Backend.ImportServices(ctx, q.ImportPlan.Services(rc.Backend.Layout(), m.Cluster, m.Member, m.Token))
	} else {
		op, err = rc.Backend.Start(ctx, m.Member)
	}
	if err != nil {
		if reset := infra.RetryAt(err); reset.After(q.RetryAt) {
			q.RetryAt = q.Schedule(now, reset)
		}
		return result, fmt.Errorf("recover runtime %s (%s); retained storage was not recreated: %w", m.Member, action, err)
	}
	q.Op, q.Action = &op, action
	return result, nil
}

// RecoverLostVolume hands irreversible storage loss to the existing serialized,
// engine-native replacement workflow. A deterministic ID makes queueing crash-safe.
// The workflow retains its quorum, data-transfer and retirement safety checks.
func RecoverLostVolume(ctx context.Context, rc *Context, member, volume, volumeID string, replicated bool) error {
	if !replicated {
		return fmt.Errorf("%w: durable volume %s for member %s is missing; automatic empty-volume creation is refused; recover the original volume or restore a backup into a new cluster", ErrConflict, volume, member)
	}
	if rc.Store == nil {
		return fmt.Errorf("%w: volume %s is missing; queue replace-member for %s after verifying surviving replicas", ErrUnavailable, volume, member)
	}
	digest := sha256.Sum256([]byte(rc.Cluster.Ref().Namespace + "\x00" + rc.Cluster.Name + "\x00" + member + "\x00" + volumeID))
	id := "volume-recovery-" + hex.EncodeToString(digest[:16])
	existing, err := rc.Store.GetOperation(ctx, id)
	if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	if existing != nil {
		if existing.Phase == domain.PhaseFailed || existing.Phase == domain.PhaseSucceeded || existing.Phase == domain.PhaseCancelled {
			return fmt.Errorf("%w: volume %s remains missing after replacement operation %s (%s); inspect that operation and restore retained data or request replace-member for %s", ErrConflict, volume, id, existing.Phase, member)
		}
		return nil
	}
	now := time.Now().UTC()
	if rc.Clock != nil {
		now = rc.Clock.Now()
	}
	payload, _ := json.Marshal(map[string]string{"member": member})
	return rc.Store.EnqueueOperation(ctx, &domain.Operation{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, ID: id, Kind: domain.OperationReplaceMember, Phase: domain.PhasePending, Payload: payload, Actor: "system", Message: "Durable member volume is missing; recover through safe replica replacement", CreatedAt: now, UpdatedAt: now})
}

// RuntimeVolumeIdentity publishes the CP's initialized-data receipt. Empty
// legacy service IDs are resolved by ownership reconciliation before opting in.
func RuntimeVolumeIdentity(uid, member, volume, volumeID string, initialized bool) *agentapi.VolumeIdentity {
	if volume == "" || volumeID == "" {
		return nil
	}
	return &agentapi.VolumeIdentity{ClusterUID: uid, Member: member, Volume: volume, VolumeID: volumeID, Initialized: initialized}
}

// ApplyRuntimeRecoveryStatus keeps data loss visible while a safe replacement
// is queued or waiting for quorum. It cannot turn a cluster Ready.
func ApplyRuntimeRecoveryStatus(status *domain.ClusterStatus, member, volume string, q RuntimeRecovery, now time.Time) {
	if !q.VolumeLost {
		return
	}
	message := fmt.Sprintf("Member %s lost durable volume %s or its data identity; empty-volume initialization is refused. Safe replica replacement requires surviving healthy data; otherwise recover the original volume or restore a backup into a new cluster", member, volume)
	status.Conditions = append(status.Conditions, domain.Condition{Type: "VolumeLost", Status: "True", Reason: "DataIdentityMissing", Message: message, LastTransitionTime: now})
	status.Phase = domain.PhaseDegraded
	status.Message = message
	for i := range status.Members {
		if status.Members[i].Name == member {
			status.Members[i].Health = false
			status.Members[i].State = "VOLUME_LOST"
		}
	}
}

// ConfirmedVolumeLoss requires an owned logical incarnation. Generic conflicts,
// inaccessible storage and foreign identities never authorize replacement.
func ConfirmedVolumeLoss(err error, volume, serviceID string) bool {
	var lost *infra.VolumeLostError
	return serviceID != "" && errors.As(err, &lost) && lost.Volume == volume && lost.ServiceID == serviceID
}

// ObserveRetiringService preserves a confirmed original service observation
// despite physical storage loss. This is only for existing native retirement
// workflows: it does not prove a runtime stopped or permit fresh deployment.
func ObserveRetiringService(ctx context.Context, b infra.Backend, hostname, recordedID, volume, volumeID string, volumeLost bool) (*infra.Service, error) {
	service, err := b.GetService(ctx, hostname)
	if err == nil {
		return service, nil
	}
	kind := infra.KindRuntime
	if hostname == volume {
		kind = infra.KindVolume
	}
	if volumeLost && ConfirmedVolumeLoss(err, volume, volumeID) && service != nil && recordedID != "" && service.ID == recordedID && service.Hostname == hostname && service.Kind == kind {
		return service, nil
	}
	return service, err
}
