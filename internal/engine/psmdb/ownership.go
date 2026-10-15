// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func verifyServiceOwnership(ctx context.Context, rc *engine.Context, m *member, service *infra.Service, volume bool) error {
	recorded := m.ServiceID
	if volume {
		recorded = m.VolumeID
	}
	if recorded != "" && service.ID == recorded {
		return nil
	}
	if volume && recorded != "" {
		return fmt.Errorf("%w: service %s no longer matches its recorded volume identity", engine.ErrConflict, service.Hostname)
	}
	if volume {
		retained, err := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
		if err != nil {
			return err
		}
		for _, v := range retained {
			if v.Member == m.Name && v.Hostname == service.Hostname && v.ServiceID != "" && v.ServiceID == service.ID {
				return nil
			}
		}
	}
	tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return err
	}
	if tok != nil && tok.Token != "" {
		if engine.HasOwnership(service, tok.Token, rc.Cluster.Name, m.Name) {
			return nil
		}
		if volume {
			runtime, err := rc.Backend.GetService(ctx, m.Name)
			if err != nil {
				return err
			}
			// A partial import may leave Local Storage without environment.
			// Only an evidence-verified runtime's exact mount proves ownership;
			// a recorded runtime ID alone cannot authorize a new volume.
			if runtime != nil && engine.HasOwnership(runtime, tok.Token, rc.Cluster.Name, m.Name) && runtime.Ownership["KARAKORAM_DATA_DIR"] == "/mnt/"+service.Hostname {
				return nil
			}
		}
	}
	return engine.UnverifiedOwnership(service)
}

// verifyMemberServices verifies and records both member services, then writes
// the non-sensitive ownership marker where only other evidence proved them.
// Markers are written only after every service passed, so a marker backfilled
// from a recorded runtime ID never becomes mount evidence for its volume.
func verifyMemberServices(ctx context.Context, rc *engine.Context, m *member) error {
	var verified []*infra.Service
	for _, volume := range []bool{false, true} {
		name := m.Name
		if volume {
			if m.VolumeOrphaned {
				continue
			}
			name = m.Volume
		}
		if name == "" {
			continue
		}
		service, err := rc.Backend.GetService(ctx, name)
		if err != nil {
			return err
		}
		if service == nil {
			continue
		}
		if err := verifyServiceOwnership(ctx, rc, m, service, volume); err != nil {
			return err
		}
		if volume {
			m.VolumeID = service.ID
		} else {
			m.ServiceID = service.ID
		}
		verified = append(verified, service)
	}
	return ensureOwnerMarkers(ctx, rc, m, verified)
}

// ensureOwnerMarkers backfills markers on verified services. Services of a
// cluster being deleted are left alone, and a member without a stored agent
// token has nothing to derive a marker from; its recorded IDs still apply.
func ensureOwnerMarkers(ctx context.Context, rc *engine.Context, m *member, services []*infra.Service) error {
	if len(services) == 0 || rc.Cluster.Deleting {
		return nil
	}
	tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if missing(err) {
		return nil
	}
	if err != nil || tok == nil {
		return err
	}
	for _, service := range services {
		if err := engine.EnsureOwnerMarker(ctx, rc.Backend, service, tok.Token, rc.Cluster.Name, m.Name); err != nil {
			return err
		}
	}
	return nil
}

// adoptImported records the IDs of services Karakoram's own import created
// and reports whether every member service now exists. An expired import
// leaves no process evidence, so existing services then need a marker.
func adoptImported(ctx context.Context, rc *engine.Context, m *member, op infra.Op, st infra.OpState) (bool, error) {
	complete := true
	for _, volume := range []bool{false, true} {
		name, recorded := m.Name, &m.ServiceID
		if volume {
			if m.Volume == "" || m.VolumeOrphaned {
				continue
			}
			name, recorded = m.Volume, &m.VolumeID
		}
		service, err := rc.Backend.GetService(ctx, name)
		if err != nil {
			return false, err
		}
		if service == nil {
			complete = false
			continue
		}
		if *recorded == "" {
			*recorded = engine.ImportedServiceID(op, st, service)
		}
	}
	return complete, nil
}
