// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Managed frontend credentials are always read from sealed storage, never SQL hashes.
func (p *Core) proxyUsers(ctx context.Context, rc *engine.Context) ([]myspec.ProxyUser, error) {
	credentials, credentialErr := secret(ctx, rc, accessCredentialsKey)
	if credentialErr != nil && !missing(credentialErr) {
		return nil, credentialErr
	}
	if len(credentials) > 0 {
		var managed map[string]accessCredential
		if err := json.Unmarshal(credentials, &managed); err != nil {
			return nil, err
		}
		for _, credential := range managed {
			if credential.Secondary != "" {
				return nil, engine.Invalid("discard retained MySQL passwords before deploying ProxySQL")
			}
		}
	}
	password, err := secret(ctx, rc, "users/admin")
	if err != nil {
		return nil, err
	}
	users := []myspec.ProxyUser{{Username: "admin", Password: string(password), RequireTLS: true}}
	raw, err := secret(ctx, rc, "mysql/access/proxy-users")
	if missing(err) {
		return users, nil
	}
	if err != nil {
		return nil, err
	}
	var managed []myspec.ProxyUser
	if err = json.Unmarshal(raw, &managed); err != nil {
		return nil, err
	}
	return append(users, managed...), nil
}
func proxyWriterHealthy(st myspec.ProxyStatusResponse) bool {
	for _, b := range st.Backends {
		if b.Hostgroup == myspec.ProxyWriterGroup && b.Status == "ONLINE" {
			return true
		}
	}
	return false
}

func proxyApplied(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	all, count, err := engine.VisitLiveInstancesConcurrent(ctx, rc, m.Name, func(a *agentapi.Client) (bool, error) {
		probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
		defer cancel()
		status, err := a.Status(probe)
		if err != nil {
			return false, err
		}
		if status.ConfigVersion < m.ConfigVersion {
			return false, a.Reload(probe)
		}
		var native myspec.ProxyStatusResponse
		if err = a.Engine(probe, "mysql", myspec.ActionProxyStatus, struct{}{}, &native); err != nil {
			return false, err
		}
		return proxyWriterHealthy(native), nil
	})
	return all && count >= m.Replicas, err
}
