// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

var _ infra.PlatformSnapshotter = (*Backend)(nil)

func (b *Backend) PlatformSnapshot() *infra.TargetSnapshot { return b.c.snapshot }

func (c *client) setSnapshot(environmentID string) {
	c.snapshot = infra.PlatformSnapshot("dokploy|" + c.origin + "|" + environmentID)
	c.snapshot.SetDependencyListing(func(ctx context.Context) (string, error) {
		// Bypass both snapshot serving and the pass-local backend cache: this
		// request is the independent proof that cached platform facts still hold.
		var listing struct {
			Applications []struct {
				ID          string `json:"applicationId"`
				Name        string `json:"name"`
				AppName     string `json:"appName"`
				Status      string `json:"applicationStatus"`
				Description string `json:"description"`
				ServerID    string `json:"serverId"`
			} `json:"applications"`
		}
		if err := c.get(infra.WithoutSteadyObservation(ctx), "environment.one", map[string]string{"environmentId": environmentID}, &listing); err != nil {
			return "", err
		}
		// Keep ownership metadata, but not Dokploy's volatile timestamps or
		// counters. Application order is not meaningful in environment.one.
		rows := make([][6]string, 0, len(listing.Applications))
		dependencies := make(map[string]string, len(listing.Applications))
		for _, app := range listing.Applications {
			rows = append(rows, [6]string{app.ID, app.Name, app.AppName, app.Status, app.Description, app.ServerID})
			if m, ok := parseMetadata(app.Description); ok && m.Hostname == app.Name {
				raw, _ := json.Marshal([6]string{app.ID, app.Name, app.AppName, app.Status, app.Description, app.ServerID})
				dependencies[m.Hostname] = string(raw)
			}
		}
		slices.SortFunc(rows, func(a, b [6]string) int {
			for i := range a {
				if cmp := strings.Compare(a[i], b[i]); cmp != 0 {
					return cmp
				}
			}
			return 0
		})
		data, err := json.Marshal(rows)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(data)
		c.snapshot.SetDependencies(dependencies)
		return hex.EncodeToString(sum[:]), nil
	})
}

// RuntimeStates reads current application identities, owned Swarm services and
// live tasks. Only the fresh inventory is shared within this render; no prior
// observation cache, lifecycle polling or recovery is involved.
func (b *Backend) RuntimeStates(ctx context.Context, names []string) (map[string]infra.Service, error) {
	ctx = infra.WithoutSteadyObservation(infra.WithoutObservation(ctx))
	var listing struct {
		Applications []application `json:"applications"`
	}
	if err := b.c.get(ctx, "environment.one", map[string]string{"environmentId": b.target.ID}, &listing); err != nil {
		return nil, err
	}
	apps := make(map[string]application, len(names))
	for _, name := range names {
		apps[name] = application{}
	}
	for _, app := range listing.Applications {
		previous, requested := apps[app.Name]
		if !requested {
			continue
		}
		if previous.ID != "" {
			return nil, restoreConflict("ambiguous runtime application identity")
		}
		m, ok := parseMetadata(app.Description)
		if ok && m.Hostname == app.Name && (app.Environment == "" || app.Environment == b.target.ID) && (m.Environment == "" || m.Environment == b.target.ID) {
			apps[m.Hostname] = app
		}
	}
	result := make(map[string]infra.Service, len(names))
	var inventory []swarmSummary
	for _, name := range names {
		app, exists := apps[name]
		if !exists || app.ID == "" {
			continue
		}
		// Some Dokploy versions omit appName from environment summaries.
		// Do not guess or reuse an old locator: it can change even when the
		// application ID is unchanged.
		if app.AppName == "" {
			full, err := b.applicationRecord(ctx, app.ID)
			if errors.Is(err, infra.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			current, ok := parseMetadata(full.Description)
			if !ok || full.ID != app.ID || full.Name != name || current.Hostname != name ||
				full.Environment != b.target.ID || current.Environment != "" && current.Environment != b.target.ID {
				return nil, restoreConflict("runtime identity changed during routing observation")
			}
			app = full
		}
		m, _ := parseMetadata(app.Description)
		if app.AppName == "" || m.Kind != infra.KindRuntime || m.Importing || m.Desired != "running" || !m.Deployed || app.Status == "error" {
			continue
		}
		// A lifecycle receipt alone is not ownership evidence. Validate the
		// current Swarm service and its tasks even while a roll is pending.
		if pending := m.Pending; pending != nil && !terminalEndpointRepair(pending) &&
			(pending.Failed || pending.Uncertain || pending.Action != "deploy" && pending.Action != "restart" && pending.Action != "scale") {
			continue
		}
		if inventory == nil {
			var err error
			inventory, err = b.readServiceInventory(ctx)
			if err != nil {
				return nil, err
			}
		}
		var owned *swarmService
		for _, summary := range inventory {
			if summary.Name == app.AppName {
				var err error
				owned, err = b.readServiceSummary(ctx, summary)
				if err != nil {
					return nil, err
				}
				break
			}
		}
		if owned == nil {
			continue
		}
		if !b.ownsService(app, m, owned) {
			return nil, restoreConflict("cannot authorize routing without the owned Swarm service")
		}
		// Dokploy passes appName directly to docker service ps, which also
		// accepts IDs. Bind liveness to the inspected object, not its reusable name.
		ts, err := b.readTasks(ctx, owned.ID)
		if err != nil {
			return nil, err
		}
		service := observe(app, m)
		service.Status = infra.StatusStopped
		for _, task := range ts {
			if task.ServiceID != "" && task.ServiceID != owned.ID {
				return nil, restoreConflict("task service identity changed during routing observation")
			}
			// Docker task names are service.slot (historical rows may have \_).
			taskName := strings.TrimPrefix(strings.TrimSpace(task.Name), `\_ `)
			if taskName != "" && !strings.HasPrefix(taskName, owned.Spec.Name+".") {
				return nil, restoreConflict("task service name changed during routing observation")
			}
			if strings.EqualFold(task.State, "running") && strings.HasPrefix(strings.ToLower(task.CurrentState), "running") {
				service.Status = infra.StatusRunning
			}
		}
		// Keep an already deployed peer in proxy discovery through the task
		// gap of its own roll. Omission would DELETE it on the proxy and erase
		// the epoch observation, preventing local recovery until another pull.
		// Active is routing intent, not a claim that a task is currently live.
		// Once the receipt is cleared, fails or expires, or intent changes,
		// only actual running tasks admit the peer again.
		pending := m.Pending
		rollWindow := taskStartupWindow
		if pending != nil && pending.Action == "deploy" {
			rollWindow += buildProgressWindow
		}
		now := b.nowTime()
		if service.Status != infra.StatusRunning && pending != nil &&
			!pending.Failed && !pending.Uncertain &&
			!pending.Issued.IsZero() && !now.Before(pending.Issued) &&
			now.Before(pending.Issued.Add(rollWindow)) &&
			(pending.Action == "deploy" || pending.Action == "restart" || pending.Action == "scale") &&
			pending.AppID == app.ID && pending.Hostname == name &&
			pending.AppName == app.AppName &&
			(pending.ServiceIdentity == "" || pending.ServiceIdentity == owned.ID) &&
			owned.Spec.Mode.Replicated != nil && owned.Spec.Mode.Replicated.Replicas != nil &&
			*owned.Spec.Mode.Replicated.Replicas > 0 {
			service.Status = infra.StatusActive
		}
		result[name] = service
	}
	return result, nil
}
