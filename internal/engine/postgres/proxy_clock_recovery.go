// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

const proxyClockMargin = 5 * time.Minute
const proxyClockCooldown = 30 * time.Minute

// Keep the completed evidence across CP restarts: the same stale monitor rows
// must never cause another restart, even after the cooldown has elapsed.
type proxyClockRecovery struct {
	EvidenceMicros int64     `json:"evidenceMicros,omitempty"`
	AttemptedAt    time.Time `json:"attemptedAt,omitempty"`
	RequestID      string    `json:"requestId,omitempty"`
	Completed      bool      `json:"completed,omitempty"`
	Message        string    `json:"message,omitempty"`
}

func futureProxyMonitorEvidence(st pgspec.StatusResponse, at time.Time) int64 {
	if st.Running {
		return 0
	}
	for _, backend := range st.ProxyBackends {
		if backend.Hostgroup == 10 && backend.Status == "ONLINE" {
			return 0
		}
	}
	boundary := at.Add(proxyClockMargin).UnixMicro()
	var latest int64
	for _, backend := range st.ProxyBackends {
		if backend.Diagnostics == nil {
			continue
		}
		d := backend.Diagnostics
		for _, observation := range []*pgspec.ProxyMonitorError{d.Connect, d.Ping, d.ReadOnly} {
			if observation != nil && observation.TimeUnixMicro > boundary && observation.TimeUnixMicro > latest {
				latest = observation.TimeUnixMicro
			}
		}
	}
	return latest
}

func healthyProxyRecoveryPrimary(ctx context.Context, rc *engine.Context, s *state) bool {
	primaries := 0
	for _, m := range s.data() {
		if m.Offline || m.Removing || m.Retirement != "" {
			continue
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil || !st.Running || (st.Role != "primary" && st.Role != "standalone") || (m.Topology == "ha" && !st.EtcdHealthy) {
			continue
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			continue
		}
		status, err := engine.ObserveAgentStatus(ctx, a, "PostgreSQL", m.Name)
		a.Close()
		if err == nil && !status.Maintenance && status.Error == "" {
			primaries++
		}
	}
	return primaries == 1
}

// Reset only a non-serving stateless proxy with demonstrably future-dated
// native monitor state. Old agents already support this identified Process API;
// no native SQL, trust change, or database restart is involved.
func (p *Core) recoverProxyClock(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Suspended || rc.Cluster.Deleting || s.Pending != nil || s.Upgrade != nil || s.Rolling != nil || s.Replacement != nil || s.RestoreRecovery != nil {
		return false, nil
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil || bs.ManagedRolesHeld || bs.Restore != nil {
		return false, err
	}
	handled := false
	primaryChecked, primaryHealthy := false, false
	for _, m := range s.Members {
		if m.Role != "proxy" || m.Offline || m.Removing || m.Retirement != "" || desiredComponent(rc, m).Replicas == 0 || (m.Stage != "joined" && m.Stage != "deployed" && m.Stage != "deploying") {
			continue
		}
		seen := map[string]bool{}
		complete := true
		var mutationErr error
		_, _, err := engine.VisitLiveInstancesWithIdentity(ctx, rc, m.Name, func(a *agentapi.Client, instance string) (bool, error) {
			seen[instance] = true
			var native pgspec.StatusResponse
			readCtx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
			err := a.Engine(readCtx, "postgres", "status", struct{}{}, &native)
			cancel()
			if err != nil {
				complete = false
				return false, nil
			}
			evidence := futureProxyMonitorEvidence(native, now(rc))
			receipt := m.ProxyClockRecovery[instance]
			if evidence == 0 {
				if receipt != nil {
					receipt.Message = ""
				}
				return true, nil
			}
			if m.ProxyClockRecovery == nil {
				m.ProxyClockRecovery = map[string]*proxyClockRecovery{}
			}
			if receipt == nil {
				receipt = &proxyClockRecovery{}
				m.ProxyClockRecovery[instance] = receipt
			}
			prefix := fmt.Sprintf("proxy %s instance %s monitor clock state is ahead of the control-plane clock", m.Name, instance)
			status, err := engine.ObserveAgentStatus(ctx, a, "PostgreSQL", m.Name)
			if err != nil {
				receipt.Message = prefix + "; waiting for supervised process status"
				complete = false
				return false, nil
			}
			process := status.Processes["proxysql"]
			if status.Maintenance || status.Error != "" || process.State != "running" || process.PID == 0 {
				receipt.Message = prefix + "; recovery is held by maintenance, process controls, or unavailable supervision"
				return true, nil
			}
			if !primaryChecked {
				primaryHealthy = healthyProxyRecoveryPrimary(ctx, rc, s)
				primaryChecked = true
			}
			if !primaryHealthy {
				receipt.Message = prefix + "; waiting for a healthy primary before resetting the proxy"
				return true, nil
			}
			if receipt.Completed && (receipt.EvidenceMicros == evidence || now(rc).Before(receipt.AttemptedAt.Add(proxyClockCooldown))) {
				receipt.Message = prefix + "; proxy already restarted; waiting for changed monitor evidence and the recovery cooldown"
				return true, nil
			}
			handled = true
			if receipt.RequestID == "" || receipt.Completed {
				receipt.EvidenceMicros, receipt.AttemptedAt = evidence, now(rc)
				receipt.RequestID = fmt.Sprintf("proxy-clock/%s/%s/%d", s.UID, instance, evidence)
				receipt.Completed = false
				receipt.Message = prefix + "; restarting proxy to clear it"
				// End this pass before the mutation. The outer reconciler
				// durably stores this intent before the next Sync can submit it.
				if err := save(rc.Cluster, s); err != nil {
					mutationErr = err
					return false, err
				}
				if rc.Events != nil {
					if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "ProxyMonitorClockAhead", Message: receipt.Message}); err != nil {
						mutationErr = err
						return false, err
					}
				}
				return true, nil
			}
			if err := a.Process(ctx, agentapi.ProcessAction{Process: "proxysql", Action: "restart", RequestID: receipt.RequestID}); err != nil {
				mutationErr = err
				return false, err
			}
			receipt.Completed = true
			receipt.Message = prefix + "; proxy restarted; waiting for fresh monitor observations"
			return true, nil
		})
		if mutationErr != nil {
			return handled, mutationErr
		}
		// Clock diagnosis is optional. Failed/partial inventory or probes must
		// not preempt deployment observation or erase unobserved receipts.
		if err != nil || !complete {
			continue
		}
		for instance := range m.ProxyClockRecovery {
			if !seen[instance] {
				delete(m.ProxyClockRecovery, instance)
			}
		}
	}
	return handled, nil
}

func proxyClockCondition(rc *engine.Context, s *state) *domain.Condition {
	var messages []string
	for _, m := range s.Members {
		for _, receipt := range m.ProxyClockRecovery {
			if receipt.Message != "" {
				messages = append(messages, receipt.Message)
			}
		}
	}
	if len(messages) == 0 {
		return nil
	}
	sort.Strings(messages)
	return &domain.Condition{Type: "ProxyClockReady", Status: "False", Reason: "MonitorClockAhead", Message: strings.Join(messages, "; "), LastTransitionTime: now(rc)}
}
