// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"crypto/subtle"
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
		owned := func(s *infra.Service) bool {
			o := s.Ownership
			return o["KARAKORAM_CLUSTER"] == rc.Cluster.Name && o["KARAKORAM_MEMBER"] == m.Name && subtle.ConstantTimeCompare([]byte(o["KARAKORAM_AGENT_TOKEN"]), []byte(tok.Token)) == 1
		}
		if owned(service) {
			return nil
		}
		if volume {
			runtime, err := rc.Backend.GetService(ctx, m.Name)
			if err != nil {
				return err
			}
			// Local Storage has no ownership environment. A token-verified
			// runtime's exact mount binding also proves ownership after a
			// partial import, before the member leaves the planned stage.
			if runtime != nil && owned(runtime) && runtime.Ownership["KARAKORAM_DATA_DIR"] == "/mnt/"+service.Hostname {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: service %s has no verified cluster ownership", engine.ErrConflict, service.Hostname)
}
func verifyMemberServices(ctx context.Context, rc *engine.Context, m *member) error {
	for _, volume := range []bool{false, true} {
		name := m.Name
		if volume {
			if m.VolumeOrphaned {
				continue
			}
			name = m.Volume
		}
		service, err := rc.Backend.GetService(ctx, name)
		if err != nil {
			return err
		}
		if service == nil {
			continue
		}
		if err = verifyServiceOwnership(ctx, rc, m, service, volume); err != nil {
			return err
		}
		if volume {
			m.VolumeID = service.ID
		} else {
			m.ServiceID = service.ID
		}
	}
	return nil
}
