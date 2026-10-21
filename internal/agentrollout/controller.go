// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package agentrollout schedules opt-in provider-managed agent upgrades. Unknown
// agents alone never trigger automatic disruption; operators can roll them manually.
package agentrollout

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/events"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const SettingsKey = "agentRollout"

func Settings(ctx context.Context, st store.SettingsRepository) (domain.AgentRolloutSettings, error) {
	settings := domain.AgentRolloutSettings{MaxConcurrentClusters: 1}
	raw, err := st.GetSetting(ctx, SettingsKey)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return settings, err
	}
	if err == nil {
		if err = json.Unmarshal(raw, &settings); err != nil {
			return settings, err
		}
	}
	build := engine.CurrentAgentBuild()
	settings.CurrentBuild = &build
	return settings, nil
}

func Validate(settings domain.AgentRolloutSettings) error {
	if settings.MaxConcurrentClusters < 1 || settings.MaxConcurrentClusters > 5 {
		return errors.New("maxConcurrentClusters must be between 1 and 5")
	}
	return nil
}

type Trigger interface{ Trigger(domain.ClusterRef) }
type Options struct {
	Store    *store.Store
	Registry *engine.Registry
	Trigger  Trigger
	Hub      *events.Hub
	Clock    engine.Clock
	Interval time.Duration
	// Tick allows deterministic Run tests without wall-clock sleeps.
	Tick   <-chan time.Time
	Target func() domain.AgentBuild
	Logger *slog.Logger
}
type Controller struct{ opts Options }

func New(opts Options) *Controller {
	if opts.Clock == nil {
		opts.Clock = engine.RealClock{}
	}
	if opts.Interval <= 0 {
		opts.Interval = 60 * time.Second
	}
	if opts.Target == nil {
		opts.Target = engine.CurrentAgentBuild
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Controller{opts: opts}
}

func OperationID(c domain.Cluster, targetBuild string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d/%s", domain.NormalizedNamespace(c.Namespace), c.Name, c.CreatedAt.UnixNano(), targetBuild)))
	return "agents-" + hex.EncodeToString(digest[:])[:16]
}

func (c *Controller) Run(ctx context.Context) error {
	tick := c.opts.Tick
	if tick == nil {
		timer := time.NewTicker(c.opts.Interval)
		defer timer.Stop()
		tick = timer.C
	}
	for {
		if err := c.Reconcile(ctx); err != nil && ctx.Err() == nil {
			c.opts.Logger.Error("Agent rollout reconciliation failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-tick:
			if !ok {
				return nil
			}
		}
	}
}

// Reconcile evaluates one durable scheduling pass. Queue admission and event
// deduplication are transactional; a crash cannot lose a warning or rearm failure.
func (c *Controller) Reconcile(ctx context.Context) error {
	settings, err := Settings(ctx, c.opts.Store)
	if err != nil {
		return err
	}
	if err = Validate(settings); err != nil {
		return err
	}
	clusters, err := c.opts.Store.ListClusters(ctx, "")
	if err != nil {
		return err
	}
	slices.SortFunc(clusters, func(a, b domain.Cluster) int {
		if a.Ref().Key() < b.Ref().Key() {
			return -1
		}
		if a.Ref().Key() > b.Ref().Key() {
			return 1
		}
		return 0
	})
	target := c.opts.Target()
	if target.Build == "" {
		return errors.New("current agent build is unavailable")
	}
	var failures []error
	for _, cluster := range clusters {
		status := engine.FreshStatus(cluster.Status, c.opts.Clock.Now())
		engine.ObserveAgentVersions(&status, target, c.opts.Clock.Now())
		if len(status.Agents.Outdated) == 0 {
			continue
		}
		id := OperationID(cluster, target.Build)
		event := domain.Event{Namespace: cluster.Namespace, Cluster: cluster.Name, Type: "Warning", Reason: "AgentsOutdated", Message: fmt.Sprintf("Agents differ from target build %s: %v", target.Build, status.Agents.Outdated), Actor: "system:agent-rollout", CreatedAt: c.opts.Clock.Now()}
		inserted, err := c.opts.Store.AppendDeduplicatedEvent(ctx, "agentRollout/warnings/"+id, &event)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if inserted && c.opts.Hub != nil {
			c.opts.Hub.Publish(event)
		}
		if !settings.Automatic || cluster.ManagedBy != nil || cluster.Deleting || cluster.Status.Phase == domain.PhaseSuspended {
			continue
		}
		provider, err := c.opts.Registry.Get(cluster.Spec.Provider)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if !slices.Contains(provider.Describe().Operations, domain.OperationUpgradeAgents) {
			continue
		}
		payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: target.Build, TargetVersion: target.Version})
		op := domain.Operation{ID: id, Namespace: cluster.Namespace, Cluster: cluster.Name, Kind: domain.OperationUpgradeAgents, Payload: payload, Actor: "system:agent-rollout", CreatedAt: c.opts.Clock.Now()}
		inserted, err = c.opts.Store.EnqueueAgentRollout(store.WithClusterSnapshot(ctx, &cluster), &op, settings.MaxConcurrentClusters)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if inserted && c.opts.Trigger != nil {
			c.opts.Trigger.Trigger(cluster.Ref())
		}
	}
	return errors.Join(failures...)
}
