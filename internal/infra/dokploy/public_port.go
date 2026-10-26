// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type swarmPort struct {
	PublishedPort int    `json:"PublishedPort"`
	TargetPort    int    `json:"TargetPort"`
	Protocol      string `json:"Protocol"`
	PublishMode   string `json:"PublishMode"`
}

// Keep the inverse before staging settings: a reload refusal must not leave
// desired public settings that a later lifecycle operation could apply.
type portChange struct {
	Ports           []port             `json:"ports,omitempty"`
	PublicPorts     []infra.PublicPort `json:"publicPorts,omitempty"`
	UpdateConfig    json.RawMessage    `json:"updateConfig"`
	Pending         *operation         `json:"pending,omitempty"`
	PortsPending    bool               `json:"portsPending,omitempty"`
	Status          string             `json:"status"`
	ReloadPending   bool               `json:"reloadPending,omitempty"`
	ReloadCompleted bool               `json:"reloadCompleted,omitempty"`
	RollbackPort    int                `json:"rollbackPort,omitempty"`
	Unconfirmed     bool               `json:"unconfirmed,omitempty"`
}

func (p *portChange) failure() error {
	if p.Unconfirmed {
		return &infra.PublicPortPublishError{Port: p.RollbackPort}
	}
	return &infra.PublicPortConflictError{Port: p.RollbackPort}
}

func (b *Backend) checkPublicPort(ctx context.Context, appName string, p infra.PublicPort) error {
	services, err := b.serviceInventory(ctx)
	if err != nil {
		return err
	}
	for _, summary := range services {
		if summary.Name == appName {
			continue
		}
		service, err := b.inspectServiceSummary(ctx, summary)
		if err != nil {
			return err
		}
		for _, ports := range [][]swarmPort{service.Spec.EndpointSpec.Ports, service.Endpoint.Ports} {
			for _, old := range ports {
				if old.PublishedPort == p.Port && old.Protocol == p.Protocol && (old.PublishMode == "ingress" || old.PublishMode == "") {
					return &infra.PublicPortConflictError{Port: p.Port}
				}
			}
		}
	}
	return nil
}

func beginPortChange(a *application, m *metadata) error {
	if m.PortChange != nil {
		return nil
	}
	config, err := json.Marshal(a.UpdateConfig)
	if err != nil {
		return err
	}
	m.PortChange = &portChange{Ports: slices.Clone(a.Ports), PublicPorts: slices.Clone(m.PublicPorts), UpdateConfig: config, Pending: m.Pending, PortsPending: m.PortsPending, Status: a.Status}
	return nil
}

func samePortRow(a, b port) bool {
	return a.PublishedPort == b.PublishedPort && a.TargetPort == b.TargetPort && a.Protocol == b.Protocol && a.PublishMode == b.PublishMode
}

func (b *Backend) rollbackPortChange(ctx context.Context, a *application, m *metadata) error {
	previous := m.PortChange
	if previous == nil {
		return nil
	}
	service, err := b.inspectService(ctx, a.AppName)
	if err != nil {
		return err
	}
	if service != nil && !b.ownsService(*a, *m, service) {
		return restoreConflict("Swarm service ownership changed; refusing port rollback")
	}
	// Read fresh rows: create may have committed despite losing its response.
	var current application
	if err := b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, &current); err != nil {
		return err
	}
	for _, row := range current.Ports {
		if !slices.ContainsFunc(previous.Ports, func(old port) bool { return samePortRow(old, row) }) {
			if err := b.c.post(ctx, "port.delete", map[string]any{"portId": row.ID}, nil); err != nil && !errors.Is(err, infra.ErrNotFound) {
				return err
			}
		}
	}
	for _, row := range previous.Ports {
		if !slices.ContainsFunc(current.Ports, func(old port) bool { return samePortRow(old, row) }) {
			if err := b.c.post(ctx, "port.create", map[string]any{"applicationId": a.ID, "publishedPort": row.PublishedPort, "targetPort": row.TargetPort, "protocol": row.Protocol, "publishMode": row.PublishMode}, nil); err != nil {
				return err
			}
		}
	}
	restored := *m
	restored.PublicPorts, restored.PortsPending, restored.Pending = previous.PublicPorts, previous.PortsPending, previous.Pending
	restored.PortChange = nil
	// No reload: Docker rejected the conflicting update, so its healthy tasks,
	// private DNSRR endpoint and private connections have never changed.
	if err := b.update(ctx, &current, restored, map[string]any{"updateConfigSwarm": previous.UpdateConfig, "applicationStatus": previous.Status}); err != nil {
		return err
	}
	*m = restored
	return nil
}

func explicitPortConflict(err error, p infra.PublicPort) bool {
	var api *APIError
	if !errors.As(err, &api) || api.StatusCode < 400 {
		return false
	}
	// Request-value sanitization replaces the publishMode value "ingress".
	ingress := strings.Contains(api.Message, "as an ingress port") || strings.Contains(api.Message, "as an [redacted] port")
	return ingress && strings.Contains(api.Message, "port '"+strconv.Itoa(p.Port)+"' is already in use by service")
}

func (b *Backend) publicPortFailure(ctx context.Context, a *application, m metadata, p infra.PublicPort, cause error) error {
	conflict := explicitPortConflict(cause, p)
	var known *infra.PublicPortConflictError
	if errors.As(cause, &known) {
		conflict = true
	}
	var api *APIError
	completedReloadRefusal := errors.As(cause, &api) && api.StatusCode == 500 && api.Message == "Error reloading application"
	if !conflict && !completedReloadRefusal {
		// A competing owner cannot establish the outcome of an outstanding
		// request. It might release the port before that request reaches Docker.
		return cause
	}
	if completedReloadRefusal && m.PortChange != nil {
		// Keep the completed handler response across reconstruction without
		// overwriting the original reload journal written before delivery.
		var current application
		if err := b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, &current); err != nil {
			return err
		}
		updated, ok := parseMetadata(current.Description)
		if !ok || updated.PortChange == nil || updated.Hostname != m.Hostname {
			return restoreConflict("application metadata changed during public-port response recording")
		}
		updated.PortChange.ReloadCompleted = true
		if err := b.update(ctx, &current, updated, nil); err != nil {
			return err
		}
		m, *a = updated, current
	}
	if !conflict {
		// Dokploy v0.30.8 hides Docker's InvalidArgument behind a generic
		// INTERNAL_SERVER_ERROR at reload, not at port.create. Re-observe to
		// distinguish a concurrent winner from an unrelated/uncertain failure.
		var inUse *infra.PublicPortConflictError
		conflict = errors.As(b.checkPublicPort(ctx, a.AppName, p), &inUse)
	}
	if !conflict {
		// The inventory intentionally omits dokploy-* services. A generic
		// reload rejection may therefore hide its owner. Only retire that
		// request after proving the original owned private DNSRR service is
		// still applied; never claim an unobserved conflict or lost delivery.
		// Only the explicit completed handler failure reaches this path.
		if m.PortChange == nil || len(m.PortChange.Ports) != 0 || len(m.PortChange.PublicPorts) != 0 ||
			!errors.As(cause, &api) || api.StatusCode != 500 || api.Message != "Error reloading application" {
			return cause
		}
		service, err := b.inspectService(ctx, a.AppName)
		if err != nil || service == nil || !b.ownsService(*a, m, service) ||
			service.Spec.EndpointSpec.Mode != "dnsrr" || len(service.Spec.EndpointSpec.Ports) != 0 || len(service.Endpoint.Ports) != 0 {
			return cause
		}
		// No owner is observable, but the completed request left the original
		// private service unchanged; report the hidden reason truthfully.
	}
	if m.PortChange == nil {
		return cause
	}
	return b.refusePortChange(ctx, a, &m, p, conflict)
}

func (b *Backend) refusePortChange(ctx context.Context, a *application, m *metadata, p infra.PublicPort, confirmed bool) error {
	m.PortChange.Unconfirmed = !confirmed
	m.PortChange.RollbackPort = p.Port
	failure := m.PortChange.failure()
	if err := b.update(ctx, a, *m, nil); err != nil {
		return err
	}
	if err := b.rollbackPortChange(ctx, a, m); err != nil {
		return err
	}
	return failure
}

// A lost response is not a refusal. Only a fresh applied specification and a
// completed replacement-task journal can retire this delivery fence.
func (b *Backend) resolvePortDelivery(ctx context.Context, a *application, m *metadata) error {
	waiting := silentFailure("public-port reload delivery is uncertain; preserving its pending operation until the applied outcome is established")
	service, err := b.inspectService(ctx, a.AppName)
	if err != nil {
		return err
	}
	if service != nil && b.ownsService(*a, *m, service) && m.PortChange.ReloadCompleted &&
		len(m.PortChange.Ports) == 0 && len(m.PortChange.PublicPorts) == 0 && len(m.PublicPorts) == 1 &&
		service.Spec.EndpointSpec.Mode == "dnsrr" && len(service.Spec.EndpointSpec.Ports) == 0 && len(service.Endpoint.Ports) == 0 {
		// A completed handler cannot publish later. Resume the refusal branch
		// even if the process stopped or inspection failed between recording
		// that response and recording the rollback decision.
		desired := m.PublicPorts[0]
		ownerErr := b.checkPublicPort(ctx, a.AppName, desired)
		var conflict *infra.PublicPortConflictError
		confirmed := errors.As(ownerErr, &conflict)
		if ownerErr != nil && !confirmed {
			return ownerErr
		}
		return b.refusePortChange(ctx, a, m, desired, confirmed)
	}
	if service == nil || !b.ownsService(*a, *m, service) || (a.Status != "done" && !(a.Status == "error" && m.PortChange.ReloadCompleted)) || len(service.Spec.EndpointSpec.Ports) != len(m.PublicPorts) {
		return waiting
	}
	for _, desired := range m.PublicPorts {
		if !slices.ContainsFunc(service.Spec.EndpointSpec.Ports, func(p swarmPort) bool {
			return p.PublishedPort == desired.Port && p.TargetPort == desired.TargetPort && p.Protocol == desired.Protocol && (p.PublishMode == "ingress" || p.PublishMode == "")
		}) {
			return waiting
		}
	}
	if m.Pending != nil {
		state, err := b.pollOperation(ctx, encodeOp(*m.Pending))
		if err != nil {
			return err
		}
		if !state.Done || state.Failed || state.Expired {
			return waiting
		}
		if err := b.c.get(ctx, "application.one", map[string]string{"applicationId": a.ID}, a); err != nil {
			return err
		}
		updated, ok := parseMetadata(a.Description)
		if !ok {
			return restoreConflict("application ownership changed during public-port delivery observation")
		}
		*m = updated
	}
	var settings map[string]any
	if a.Status == "error" && m.PortChange.ReloadCompleted {
		// The handler returned after Docker applied the matching specification.
		// Correlated replacement completion proves this is a status-only repair.
		settings = map[string]any{"applicationStatus": "done"}
	}
	updated := *m
	updated.PortChange, updated.PortsPending = nil, false
	if err := b.update(ctx, a, updated, settings); err != nil {
		return err
	}
	*m = updated
	if settings != nil {
		a.Status = "done"
	}
	return nil
}
