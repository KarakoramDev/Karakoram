// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Builds are queued on the server, not on a Karakoram environment. Keep the
// reservation lock separate from environment mutation locks (always acquired first).
var deploymentServers sync.Map

type deploymentServer struct {
	mu           contextMutex
	reservations map[string]string // application -> deployment preceding submission
}

func (b *Backend) deploymentServer() *deploymentServer {
	s, _ := deploymentServers.LoadOrStore(b.target.Config["url"], &deploymentServer{reservations: map[string]string{}})
	return s.(*deploymentServer)
}

func deploymentLimit() (int, error) {
	raw := os.Getenv("KARAKORAM_DOKPLOY_MAX_INFLIGHT_DEPLOYS")
	if raw == "" {
		return 2, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, reject("KARAKORAM_DOKPLOY_MAX_INFLIGHT_DEPLOYS must be an integer >= 1")
	}
	return n, nil
}

func terminalDeployment(status string) bool {
	return status == "done" || status == "error" || status == "cancelled"
}

// deploymentSlot holds server exclusion through the submission, bridging the
// interval before Dokploy exposes a deployment row. Ambiguous deliveries retain
// their reservation until a newer row or exact application absence is observed.
func (b *Backend) deploymentSlot(ctx context.Context, appID string) (func(bool), error) {
	limit, err := deploymentLimit()
	if err != nil {
		return nil, err
	}
	s := b.deploymentServer()
	if err := s.mu.Lock(ctx); err != nil {
		return nil, err
	}
	fail := func(err error) (func(bool), error) { s.mu.Unlock(); return nil, err }
	var projects []struct {
		ID string `json:"projectId"`
	}
	if err := b.c.get(ctx, "project.all", nil, &projects); err != nil {
		return fail(err)
	}
	// Bound additional observations even on very large servers. Incomplete
	// inventory fails closed rather than admitting an uncounted build.
	reads := 0
	apps := map[string]*operation{}
	for _, p := range projects {
		reads++
		if reads > 128 {
			return fail(silentFailure("deployment inventory exceeds observation budget; retry after reducing active target inventory"))
		}
		var project struct {
			Environments []environment `json:"environments"`
		}
		if err := b.c.get(ctx, "project.one", map[string]string{"projectId": p.ID}, &project); err != nil {
			return fail(err)
		}
		for _, e := range project.Environments {
			for _, a := range e.Applications {
				if m, owned := parseMetadata(a.Description); owned {
					apps[a.ID] = m.Pending
					if m.Wiping {
						apps[a.ID] = &operation{Action: "deploy"}
						if m.WipePreviousDeployment != "" {
							apps[a.ID].PreviousDeployments = []string{m.WipePreviousDeployment}
						}
					}
				}
			}
		}
	}
	for id := range s.reservations {
		if _, ok := apps[id]; ok {
			continue
		}
		reads++
		if reads > 128 {
			return fail(silentFailure("deployment inventory exceeds observation budget; refusing an uncounted submission"))
		}
		// deployment.all returns [] for deleted applications on Dokploy v0.30.8.
		// Only an exact-ID application lookup can corroborate disappearance.
		var a application
		if err := b.c.get(ctx, "application.one", map[string]string{"applicationId": id}, &a); err != nil {
			if errors.Is(err, infra.ErrNotFound) {
				delete(s.reservations, id)
				continue
			}
			return fail(err)
		}
		m, _ := parseMetadata(a.Description)
		apps[id] = m.Pending
		if m.Wiping {
			apps[id] = &operation{Action: "deploy", PreviousDeployments: []string{m.WipePreviousDeployment}}
		}
	}
	if _, ok := apps[appID]; !ok {
		apps[appID] = nil
	}
	active := 0
	baseline := ""
	var blockers []string
	for id, pending := range apps {
		reads++
		if reads > 128 {
			return fail(silentFailure("deployment inventory exceeds observation budget; refusing an uncounted submission"))
		}
		ds, err := b.deployments(ctx, id)
		if err != nil {
			return fail(err)
		}
		previous, reserved := s.reservations[id]
		count := 0
		var appBlockers []string
		for _, d := range ds {
			if !terminalDeployment(d.Status) {
				count++
				appBlockers = append(appBlockers, id+"/"+d.ID+":"+d.Status)
			}
		}
		// The newest row must differ from the baseline; historical rows alone
		// cannot acknowledge an uncertain submission.
		newer := len(ds) > 0 && ds[0].ID != previous
		if reserved && newer {
			delete(s.reservations, id)
			reserved = false
		}
		if pending != nil && pending.Action == "deploy" && (!pending.Failed || pending.Uncertain) &&
			(len(ds) == 0 || contains(pending.PreviousDeployments, ds[0].ID)) {
			reserved = true
		}
		if reserved && count == 0 {
			count = 1
			appBlockers = append(appBlockers, id+":receipt")
		}
		if id == appID {
			if len(ds) > 0 {
				baseline = ds[0].ID
			}
			if count > 0 {
				return fail(deploymentWait("PreviousDeployment", appBlockers, "waiting for previous platform deployment to become terminal"))
			}
		}
		active += count
		blockers = append(blockers, appBlockers...)
	}
	if active >= limit {
		return fail(deploymentWait("DeploymentSlot", blockers, fmt.Sprintf("waiting for a platform deployment slot: %d/%d in flight (Dokploy server)", active, limit)))
	}
	s.reservations[appID] = baseline
	return func(submitted bool) {
		if !submitted {
			delete(s.reservations, appID)
		}
		s.mu.Unlock()
	}, nil
}

func deploymentWait(reason string, blockers []string, detail string) error {
	sort.Strings(blockers)
	return &infra.DeploymentWaitError{Reason: reason, Key: strings.Join(blockers, ","), Detail: detail, RetryAt: time.Now().Add(15 * time.Second)}
}

// retireDeployment is called only after authoritative exact-ID deletion/absence.
// Submission paths already hold server exclusion and use release(false) instead.
func (b *Backend) retireDeployment(ctx context.Context, id string) error {
	s := b.deploymentServer()
	if err := s.mu.Lock(ctx); err != nil {
		return err
	}
	delete(s.reservations, id)
	s.mu.Unlock()
	return nil
}

func enqueueRefused(err error) bool {
	var api *APIError
	return errors.As(err, &api) && api.StatusCode >= 400 && api.StatusCode < 500 && api.StatusCode != 408
}
