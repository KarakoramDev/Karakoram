// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"sync"
)

type memberObservation struct {
	status myspec.StatusResponse
	agent  agentapi.Status
	err    error
}

func live(ctx context.Context, rc *engine.Context, name string) (myspec.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	var out myspec.StatusResponse
	if rc.Agents == nil {
		return out, engine.ErrUnavailable
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return out, err
	}
	err = a.Engine(ctx, "mysql", myspec.ActionStatus, struct{}{}, &out)
	if err == nil && rc.Cluster.Spec.Provider == "pxc" {
		err = observeGalera(ctx, rc, name, &out)
	}
	return out, err
}
func onlinePrimary(st myspec.StatusResponse) bool {
	return st.MemberState == "ONLINE" && st.MemberRole == "PRIMARY" && !st.SuperReadOnly && !st.ReadOnly
}
func observeStatusMembers(ctx context.Context, rc *engine.Context, members []*member) []memberObservation {
	out := make([]memberObservation, len(members))
	var wg sync.WaitGroup
	limit := make(chan struct{}, 8)
	for i, m := range members {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case limit <- struct{}{}:
			case <-ctx.Done():
				out[i].err = ctx.Err()
				return
			}
			defer func() { <-limit }()
			probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
			defer cancel()
			if rc.Agents == nil {
				out[i].err = engine.ErrUnavailable
				return
			}
			a, err := rc.Agents.Dial(probe, rc.Cluster.Ref(), m.Name)
			if err != nil {
				out[i].err = err
				return
			}
			out[i].agent, err = a.Status(probe)
			if err != nil {
				out[i].err = err
				return
			}
			if m.Role == "proxy" {
				var healthy bool
				healthy, err = proxyApplied(probe, rc, m)
				if err == nil && !healthy {
					err = engine.ErrUnavailable
				}
			} else {
				err = a.Engine(probe, "mysql", myspec.ActionStatus, struct{}{}, &out[i].status)
				if err == nil && rc.Cluster.Spec.Provider == "pxc" {
					err = observeGalera(probe, rc, m.Name, &out[i].status)
				}
			}
			out[i].err = err
		}()
	}
	wg.Wait()
	return out
}
func ready(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	if m.Role == "proxy" {
		return proxyApplied(ctx, rc, m)
	}
	if rc.Agents == nil {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return false, nil
	}
	st, err := a.Status(ctx)
	if err != nil {
		return false, nil
	}
	if st.ConfigVersion < m.ConfigVersion {
		return false, a.Reload(ctx)
	}
	native, err := live(ctx, rc, m.Name)
	if err != nil {
		return false, nil
	}
	if native.ServerUUID != "" {
		m.ServerUUID = native.ServerUUID
	}
	if native.MemberState != "ONLINE" {
		return false, nil
	}
	if native.DataIdentity.Initialized {
		m.Initialized = true
	}
	return true, nil
}

func configApplied(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return false, err
	}
	status, err := a.Status(ctx)
	if err != nil {
		return false, err
	}
	if status.ConfigVersion < m.ConfigVersion {
		return false, a.Reload(ctx)
	}
	return true, nil
}
