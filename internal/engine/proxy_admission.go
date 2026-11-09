// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// ObserveProxyRollout consumes the existing receipt and startup budget before
// attempting admission. An unavailable CP or agent can only stall new tasks;
// running incumbents are never probed, stopped, or made dependent on this loop.
func ObserveProxyRollout(ctx context.Context, rc *Context, op infra.Op, member string, revision int64, proxy bool) (infra.OpState, error) {
	status, err := rc.Backend.OpStatus(ctx, op)
	if err != nil || status.Failed || !proxy {
		return status, err
	}
	// A successful platform receipt may still await native convergence while
	// Swarm has already replaced one of its tasks. Admission remains necessary.
	// Only platforms with an explicit serving-roll contract need this protocol.
	if _, ok := rc.Backend.(infra.ServingProxyUpdates); !ok {
		return status, nil
	}
	return status, AdmitStartingProxies(ctx, rc, member, revision)
}

// ReconcileProxyAdmission admits opted-in task replacements after the original
// rollout receipt has settled, including Swarm crash/node-loss recovery.
// Callers skip this path while their durable rollout receipt remains active.
func ReconcileProxyAdmission(ctx context.Context, rc *Context, member string, revision int64) error {
	if !rc.Backend.Capabilities().ProxyDeploymentPreservesServing {
		return nil
	}
	return AdmitStartingProxies(ctx, rc, member, revision)
}

// AdmitStartingProxies binds each native serving proof to fresh owned inventory
// and the CP's exact desired revision. It has no background work or cached task
// addresses; pinned transports independently reauthorize each connection.
func AdmitStartingProxies(ctx context.Context, rc *Context, member string, revision int64) error {
	containers, err := rc.Backend.Containers(infra.WithFreshTaskInventory(ctx), member)
	if err != nil {
		return err
	}
	var client *agentapi.Client
	for _, task := range containers {
		if task.Status != infra.ContainerStarting || !task.ProxyServingAdmission {
			continue
		}
		if task.ID == "" || revision <= 0 {
			return fmt.Errorf("proxy admission requires task identity and configuration revision")
		}
		if client == nil {
			if rc.Agents == nil {
				return ErrUnavailable
			}
			client, err = rc.Agents.Dial(ctx, rc.Cluster.Ref(), member)
			if err != nil {
				return err
			}
		}
		address := task.IPv6
		if rc.Backend.Layout().PrivateAddressFamily == infra.AddressFamilyIPv4 {
			address = task.IPv4
		}
		if address == "" {
			address = task.IPv4
			if address == "" {
				address = task.IPv6
			}
		}
		if address == "" {
			return fmt.Errorf("proxy task %s has no pre-health overlay address", task.ID)
		}
		pinned, err := client.ForInstance(address)
		if err != nil {
			return err
		}
		err = admitProxyTask(ctx, pinned, member, task.ID, revision)
		pinned.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func admitProxyTask(ctx context.Context, client *agentapi.Client, member, taskID string, revision int64) error {
	proof, err := client.ProxyServing(ctx)
	var remote *agentapi.HTTPError
	// Released agents do not implement CP admission and do not wait for it in
	// their healthcheck. Preserve their existing rollout behavior, not a bypass
	// for any other transport, authentication, or protocol failure.
	if errors.As(err, &remote) && remote.StatusCode == http.StatusNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if proof.TaskID != taskID || proof.Member != member {
		return fmt.Errorf("proxy task %s serving proof does not match task and member", taskID)
	}
	// A fresh agent can expose authenticated TLS before its first engine apply
	// succeeds. Its unapplied revision is a startup wait, not an observation
	// failure: error backoff can otherwise skip the bounded health-admission
	// window. Never admit until both native readiness and the exact revision agree.
	if !proof.Ready {
		return nil
	}
	if proof.ConfigVersion != revision {
		return fmt.Errorf("proxy task %s serving proof does not match desired configuration revision", taskID)
	}
	if proof.Admitted {
		return nil
	}
	return client.AdmitProxyServing(ctx, agentapi.ProxyServingAdmission{TaskID: taskID, ConfigVersion: revision})
}

// RequireProxyAdmissionProtocol prevents count/resource replacement of released
// marked images whose health-kill path can still report a successful exit.
// Agent upgrades intentionally do not use this preflight: their replacement
// image supplies the fixed protocol and is explicitly admitted by the watcher.
func RequireProxyAdmissionProtocol(ctx context.Context, rc *Context, member string) error {
	if !rc.Backend.Capabilities().ProxyDeploymentPreservesServing {
		return nil
	}
	backend, ok := rc.Backend.(infra.ServingProxyUpdates)
	if !ok {
		return nil
	}
	marked, err := backend.ProxyUpdatesPreserveServing(ctx, member)
	if err != nil || !marked {
		return err
	}
	all, count, err := VisitLiveInstancesWithIdentity(infra.WithFreshTaskInventory(ctx), rc, member, func(client *agentapi.Client, _ string) (bool, error) {
		proof, err := client.ProxyServing(ctx)
		var remote *agentapi.HTTPError
		if errors.As(err, &remote) && remote.StatusCode == http.StatusNotFound {
			return false, Invalid("proxy %s does not support control-plane serving admission; run `clusters upgrade-agents` first", member)
		}
		if err != nil {
			return false, err
		}
		if proof.Member != member {
			return false, Invalid("proxy %s serving proof has the wrong member identity", member)
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	if !all || count == 0 {
		return ErrUnavailable
	}
	return nil
}
