// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

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

var errProxyConfigPending = errors.New("proxy configuration has not been applied")

func proxyApplied(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	ready, _, err := mysqlReadValue(ctx, rc, m.Name, "proxy-applied", func(probe context.Context) (bool, error) {
		all, count, err := engine.VisitLiveInstancesConcurrent(probe, rc, m.Name, func(a *agentapi.Client) (bool, error) {
			var metadata agentapi.Status
			var native myspec.ProxyStatusResponse
			var metadataErr, nativeErr error
			var batch sync.WaitGroup
			batch.Add(2)
			go func() {
				defer batch.Done()
				metadata, _, metadataErr = mysqlReadClientStatus(probe, rc, m.Name, a)
			}()
			go func() {
				defer batch.Done()
				nativeErr = mysqlReadClientAction(probe, rc, m.Name, a, myspec.ActionProxyStatus, struct{}{}, &native)
			}()
			batch.Wait()
			if err := errors.Join(metadataErr, nativeErr); err != nil {
				return false, err
			}
			if metadata.ConfigVersion < m.ConfigVersion {
				return false, errProxyConfigPending
			}
			return proxyWriterHealthy(native), nil
		})
		return all && count >= m.Replicas, err
	})
	return ready, err
}

// proxyTargetOnline uses only the original proxy-status response fields. Native
// membership and config versions cannot stand in for monitor-owned admission.
func proxyTargetOnline(st myspec.ProxyStatusResponse, target string, groups ...int) bool {
	for _, b := range st.Backends {
		if b.Host != target || b.Port != myspec.PortMySQL || b.Status != "ONLINE" {
			continue
		}
		for _, group := range groups {
			if b.Hostgroup == group {
				return true
			}
		}
	}
	return false
}

func hasServingProxy(s *state) bool {
	for _, m := range s.Members {
		if m.Role == "proxy" && !m.Removing && !m.Offline && m.Replicas > 0 {
			return true
		}
	}
	return false
}

func proxyAdmission(ctx context.Context, rc *engine.Context, s *state, target string, groups ...int) error {
	for _, proxy := range s.Members {
		if proxy.Role != "proxy" || proxy.Removing || proxy.Offline || proxy.Replicas == 0 {
			continue
		}
		all, count, err := engine.VisitLiveInstancesConcurrent(ctx, rc, proxy.Name, func(a *agentapi.Client) (bool, error) {
			var status myspec.ProxyStatusResponse
			if err := mysqlReadClientAction(ctx, rc, proxy.Name, a, myspec.ActionProxyStatus, struct{}{}, &status); err != nil {
				return false, err
			}
			return proxyTargetOnline(status, target, groups...), nil
		})
		if err != nil || !all || count < proxy.Replicas {
			s.ProxyAdmissionWait = fmt.Sprintf("Waiting for %s ONLINE in proxy hostgroups %v on every instance of %s", target, groups, proxy.Name)
			return fmt.Errorf("%s: %w", s.ProxyAdmissionWait, engine.ErrUnavailable)
		}
	}
	s.ProxyAdmissionWait = ""
	return nil
}

func proxyRejoinReady(ctx context.Context, rc *engine.Context, s *state, name string) error {
	if !clustered(s.Topology) || !hasServingProxy(s) {
		return nil
	}
	st, err := live(ctx, rc, name)
	if err != nil {
		return err
	}
	groups := []int{myspec.ProxyReaderGroup}
	if s.Topology == "cluster" {
		groups = []int{myspec.ProxyWriterGroup, myspec.ProxyBackupWriterGroup, myspec.ProxyReaderGroup}
	} else if onlinePrimary(st) {
		groups = []int{myspec.ProxyWriterGroup}
	}
	if err := proxyAdmission(ctx, rc, s, name, groups...); err != nil {
		return err
	}
	if s.Topology == "cluster" {
		return nil
	}
	target := ""
	return proxyHandoffReady(ctx, rc, s, "", &target)
}

// Pin the observed native writer when resuming an older checkpoint or a
// mixed-major election. Never accept an unrelated HG10 row for this identity.
func proxyHandoffReady(ctx context.Context, rc *engine.Context, s *state, old string, target *string) error {
	if !clustered(s.Topology) || !hasServingProxy(s) {
		return nil
	}
	if s.Topology == "cluster" {
		for _, peer := range s.data() {
			if peer.Name == old || peer.Removing || peer.Offline || peer.Retirement != "" {
				continue
			}
			st, err := live(ctx, rc, peer.Name)
			if err == nil && st.MemberState == "ONLINE" {
				if err := proxyAdmission(ctx, rc, s, peer.Name, myspec.ProxyWriterGroup, myspec.ProxyBackupWriterGroup, myspec.ProxyReaderGroup); err == nil {
					return nil
				}
			}
		}
		return engine.ErrUnavailable
	}
	for _, peer := range s.data() {
		if peer.Name == old || peer.Removing {
			continue
		}
		st, err := live(ctx, rc, peer.Name)
		if err != nil || !onlinePrimary(st) {
			continue
		}
		if *target == "" {
			*target = peer.Name
		}
		if *target != peer.Name {
			return engine.ErrUnavailable
		}
		return proxyAdmission(ctx, rc, s, *target, myspec.ProxyWriterGroup)
	}
	return engine.ErrUnavailable
}
