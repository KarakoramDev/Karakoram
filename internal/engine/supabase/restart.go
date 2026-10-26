// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

type restartCheckpoint struct {
	Stack     bool                                  `json:"stack"`
	Core      json.RawMessage                       `json:"core,omitempty"`
	CoreDone  bool                                  `json:"coreDone,omitempty"`
	Component int                                   `json:"component,omitempty"`
	Instances map[string]restartInstance            `json:"instances,omitempty"`
	Active    string                                `json:"active,omitempty"`
	Completed map[string]map[string]restartInstance `json:"completed,omitempty"`
}
type restartInstance struct {
	Before agentapi.ProcessStatus `json:"before"`
	Done   bool                   `json:"done,omitempty"`
}

// restartStack extends the database restart with individually addressed child
// restarts. Configuration or allocation identity alone cannot prove a restart.
func (p *Provider) restartStack(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	var cp restartCheckpoint
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return engine.OpResult{}, err
		}
		if !cp.Stack { // Adopt a database-only checkpoint accepted by an older control plane.
			cp = restartCheckpoint{Stack: true, Core: op.Checkpoint}
		}
	}
	cp.Stack = true
	result := func(done bool, message string) (engine.OpResult, error) {
		raw, err := json.Marshal(cp)
		return engine.OpResult{Done: done, Checkpoint: raw, Step: "restart", Message: message, RequeueAfter: 2 * time.Second}, err
	}
	wait := func(message string) (engine.OpResult, error) {
		out, err := result(false, message)
		if err != nil {
			return out, err
		}
		return out, &engine.PlatformRetryWait{RetryAt: clockNow(rc).Add(2 * time.Second), Message: message}
	}
	if !cp.CoreDone {
		coreOp := *op
		coreOp.Checkpoint = cp.Core
		out, err := p.Core.RunOperation(ctx, rc, &coreOp)
		if err != nil {
			return out, err
		}
		if len(out.Checkpoint) > 0 {
			cp.Core = out.Checkpoint
		}
		cp.CoreDone = out.Done
		return result(false, "Restarting PostgreSQL members before Supabase components")
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	if cp.Completed == nil {
		cp.Completed = map[string]map[string]restartInstance{}
	}
	for cp.Component < len(sbspec.Components) {
		component := sbspec.Components[cp.Component]
		desired := rc.Cluster.Spec.Components.Extra[component]
		if desired.Replicas == 0 {
			cp.Component++
			cp.Instances = nil
			cp.Active = ""
			continue
		}
		m := s.Members[component]
		if m == nil || m.Stage != engine.ComponentJoined || s.Pending != nil {
			return engine.OpResult{}, engine.Invalid("cannot restart Supabase component %s before deployment converges", component)
		}
		initializing := cp.Instances == nil
		if initializing {
			cp.Instances = map[string]restartInstance{}
		}
		visited := map[string]bool{}
		blocked := ""
		needsRestart := false
		all, count, err := engine.VisitLiveInstancesWithIdentity(ctx, rc, m.Name, func(client *agentapi.Client, id string) (bool, error) {
			if id == "" {
				return false, engine.Invalid("Supabase restart requires an instance identity for %s", m.Name)
			}
			visited[id] = true
			entry, known := cp.Instances[id]
			if !initializing && !known {
				return false, engine.Invalid("Supabase component %s instance %s inventory changed during restart; inspect readiness and retry", component, id)
			}
			if !initializing && !entry.Done && id != cp.Active {
				return false, nil
			}
			reason := func(message string) (bool, error) {
				if blocked == "" || id == cp.Active {
					blocked = fmt.Sprintf("Waiting for Supabase component %s instance %s: %s", component, id, message)
				}
				return false, nil
			}
			status, err := engine.ObserveAgentStatus(ctx, client, "Supabase", m.Name)
			if err != nil {
				return reason("agent status observation failed: " + err.Error())
			}
			process, ok := status.Processes[component]
			if !ok || process.State != "running" || process.PID <= 0 {
				return reason(fmt.Sprintf("supervised process is not running (state %q, PID %d)", process.State, process.PID))
			}
			if initializing {
				cp.Instances[id] = restartInstance{Before: process}
				return false, nil
			}
			var native sbspec.Status
			readCtx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
			err = client.Engine(readCtx, "supabase", "status", struct{}{}, &native)
			cancel()
			if err != nil {
				return reason("native status observation failed: " + err.Error())
			}
			if entry.Done || restartedProcess(entry.Before, process) {
				if status.ConfigVersion < m.ConfigVersion {
					return reason(fmt.Sprintf("configuration version %d observed, require %d", status.ConfigVersion, m.ConfigVersion))
				}
				if !native.Ready {
					message := "native application is not ready"
					if native.Message != "" {
						message += ": " + native.Message
					}
					return reason(message)
				}
				entry.Done = true
				cp.Instances[id] = entry
				return true, nil
			}
			needsRestart = true
			return false, nil
		})
		if err != nil {
			return engine.OpResult{}, fmt.Errorf("restarting Supabase component %s active instance %s: %w", component, cp.Active, err)
		}
		if count < desired.Replicas {
			return engine.OpResult{}, fmt.Errorf("restarting Supabase component %s active instance %s: observed %d live instances, need %d", component, cp.Active, count, desired.Replicas)
		}
		for id := range cp.Instances {
			if !visited[id] {
				return engine.OpResult{}, engine.Invalid("Supabase component %s instance %s disappeared during restart; inspect readiness and retry", component, id)
			}
		}
		if blocked != "" {
			// An incomplete initial inventory is discarded; no mutation occurred.
			if initializing {
				cp.Instances = nil
			}
			return wait(blocked)
		}
		if needsRestart {
			// The active identity was persisted before this call. Complete the
			// read-only sweep before mutating so a failed/unready completed peer
			// cannot be discovered only after the active child was restarted.
			found := false
			_, _, err := engine.VisitLiveInstancesWithIdentity(ctx, rc, m.Name, func(client *agentapi.Client, id string) (bool, error) {
				if id != cp.Active {
					return true, nil
				}
				found = true
				return false, client.Process(ctx, agentapi.ProcessAction{Process: component, Action: "restart", RequestID: op.ID + "/restart/" + m.Name + "/" + id})
			})
			if err != nil {
				return engine.OpResult{}, fmt.Errorf("restarting Supabase component %s active instance %s: %w", component, cp.Active, err)
			}
			if !found {
				return engine.OpResult{}, engine.Invalid("Supabase component %s active instance %s disappeared before restart submission", component, cp.Active)
			}
			return wait(fmt.Sprintf("Waiting for Supabase component %s instance %s: restart acknowledged; awaiting changed supervised process restart evidence", component, cp.Active))
		}
		if !initializing && all {
			cp.Completed[component] = cp.Instances
			cp.Component++
			cp.Instances = nil
			cp.Active = ""
			return result(false, "Verified restarted Supabase component "+component)
		}
		if cp.Active == "" || cp.Instances[cp.Active].Done {
			order := make([]string, 0, len(cp.Instances))
			for id, entry := range cp.Instances {
				if !entry.Done {
					order = append(order, id)
				}
			}
			sort.Strings(order)
			if len(order) > 0 {
				cp.Active = order[0]
			}
		}
		return result(false, "Preparing restart of Supabase component "+component+" instance "+cp.Active)
	}
	// Keep all completed identities until the terminal checkpoint. Health alone
	// cannot certify a new allocation which replaced an already-verified target.
	for _, component := range sbspec.Components {
		desired := rc.Cluster.Spec.Components.Extra[component]
		if desired.Replicas == 0 {
			continue
		}
		m := s.Members[component]
		proof := cp.Completed[component]
		if m == nil || len(proof) == 0 {
			return engine.OpResult{}, engine.Invalid("Supabase component %s has no completed restart evidence; retry restart", component)
		}
		visited := map[string]bool{}
		blocked := ""
		_, count, err := engine.VisitLiveInstancesWithIdentity(ctx, rc, m.Name, func(client *agentapi.Client, id string) (bool, error) {
			entry, ok := proof[id]
			if !ok || !entry.Done {
				return false, engine.Invalid("Supabase component %s instance %s inventory changed after restart verification; retry restart", component, id)
			}
			visited[id] = true
			status, err := engine.ObserveAgentStatus(ctx, client, "Supabase", m.Name)
			if err != nil {
				blocked = fmt.Sprintf("Waiting for verified Supabase component %s instance %s: agent status observation failed: %v", component, id, err)
				return false, nil
			}
			var native sbspec.Status
			readCtx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
			err = client.Engine(readCtx, "supabase", "status", struct{}{}, &native)
			cancel()
			if err != nil {
				blocked = fmt.Sprintf("Waiting for verified Supabase component %s instance %s: native status observation failed: %v", component, id, err)
				return false, nil
			}
			process := status.Processes[component]
			if process.State != "running" || process.PID <= 0 || !restartedProcess(entry.Before, process) {
				blocked = fmt.Sprintf("Waiting for verified Supabase component %s instance %s: running restarted process evidence unavailable", component, id)
			} else if status.ConfigVersion < m.ConfigVersion {
				blocked = fmt.Sprintf("Waiting for verified Supabase component %s instance %s: configuration version %d observed, require %d", component, id, status.ConfigVersion, m.ConfigVersion)
			} else if !native.Ready {
				blocked = fmt.Sprintf("Waiting for verified Supabase component %s instance %s: native application is not ready: %s", component, id, native.Message)
			}
			return true, nil
		})
		if err != nil {
			return engine.OpResult{}, err
		}
		for id := range proof {
			if !visited[id] {
				return engine.OpResult{}, engine.Invalid("Supabase component %s verified instance %s disappeared before restart completion; retry restart", component, id)
			}
		}
		if count < desired.Replicas {
			return engine.OpResult{}, engine.Invalid("Supabase component %s has %d verified live instances, need %d", component, count, desired.Replicas)
		}
		if blocked != "" {
			return wait(blocked)
		}
	}
	status, err := p.Status(ctx, rc)
	if err != nil {
		return engine.OpResult{}, err
	}
	if status.Phase != domain.PhaseReady {
		return wait("Waiting for Supabase stack readiness after component restarts: " + status.Message)
	}
	return result(true, "PostgreSQL members and all enabled Supabase component instances restarted and verified")
}

func restartedProcess(before, process agentapi.ProcessStatus) bool {
	return process.PID != before.PID || process.Restarts > before.Restarts ||
		(process.SupervisorSession != "" && before.SupervisorSession != "" && (process.SupervisorSession != before.SupervisorSession || process.Launch > before.Launch)) ||
		(process.StartedAt != nil && before.StartedAt != nil && !process.StartedAt.Equal(*before.StartedAt))
}
