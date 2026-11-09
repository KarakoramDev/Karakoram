// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type memberObservation struct {
	status                 myspec.StatusResponse
	agent                  agentapi.Status
	agentAt                time.Time
	agentObserved          bool
	err                    error
	metadataErr            error
	nativeErr              error
	nativeObserved         bool
	infrastructure         memberInfrastructure
	infrastructureErr      error
	infrastructureObserved bool
}

type memberInfrastructure struct {
	service    *infra.Service
	containers []infra.Container
}

func live(ctx context.Context, rc *engine.Context, name string) (myspec.StatusResponse, error) {
	var out myspec.StatusResponse
	err := mysqlAction(ctx, rc, name, myspec.ActionStatus, struct{}{}, &out)
	if err == nil && rc.Cluster.Spec.Provider == "pxc" {
		err = observeGalera(ctx, rc, name, &out)
	}
	return out, err
}
func onlinePrimary(st myspec.StatusResponse) bool {
	return st.WriterEligible(false)
}
func observeStatusMembers(ctx context.Context, rc *engine.Context, members []*member) []memberObservation {
	out := make([]memberObservation, len(members))
	var wg sync.WaitGroup
	limit := make(chan struct{}, infra.ObservationConcurrency)
	for i, m := range members {
		if m.ServiceID == "" || m.Stage == "planned" || m.Stage == "imported" {
			out[i].err = engine.ErrUnavailable
			continue
		}
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
			var batch sync.WaitGroup
			var metadataErr, nativeErr error
			batch.Add(3)
			go func() {
				defer batch.Done()
				out[i].agent, out[i].agentAt, metadataErr = mysqlAgentStatus(ctx, rc, m.Name)
				out[i].agentObserved = metadataErr == nil
			}()
			go func() {
				defer batch.Done()
				if m.Role == "proxy" {
					var healthy bool
					healthy, nativeErr = proxyApplied(ctx, rc, m)
					if nativeErr == nil && !healthy {
						nativeErr = engine.ErrUnavailable
					}
				} else {
					out[i].status, nativeErr = live(ctx, rc, m.Name)
				}
				out[i].nativeObserved = nativeErr == nil
			}()
			go func() {
				defer batch.Done()
				out[i].infrastructure, _, out[i].infrastructureErr = mysqlReadValue(ctx, rc, m.Name, "platform-status", func(probe context.Context) (memberInfrastructure, error) {
					var value memberInfrastructure
					// Service/storage validation and task inventory are independent
					// reads; overlap them within the shared absolute pass deadline.
					var serviceErr, containersErr error
					var reads sync.WaitGroup
					reads.Add(2)
					go func() {
						defer reads.Done()
						value.service, serviceErr = mysqlReadService(probe, rc, m.Name)
					}()
					go func() {
						defer reads.Done()
						value.containers, containersErr = rc.Backend.Containers(probe, m.Name)
					}()
					reads.Wait()
					return value, errors.Join(serviceErr, containersErr)
				})
				out[i].infrastructureObserved = out[i].infrastructureErr == nil
			}()
			batch.Wait()
			out[i].metadataErr, out[i].nativeErr = metadataErr, nativeErr
			out[i].err = errors.Join(metadataErr, nativeErr)
		}()
	}
	wg.Wait()
	return out
}
func ready(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	if m.Role == "proxy" {
		applied, err := proxyApplied(ctx, rc, m)
		if errors.Is(err, errProxyConfigPending) {
			_, _, err = engine.VisitLiveInstancesConcurrent(ctx, rc, m.Name, func(a *agentapi.Client) (bool, error) {
				status, _, e := mysqlReadClientStatus(ctx, rc, m.Name, a)
				if e != nil {
					return false, e
				}
				if status.ConfigVersion < m.ConfigVersion {
					invalidateMySQLReadObservation(ctx, rc)
					return false, a.Reload(ctx)
				}
				return true, nil
			})
			return false, err
		}
		return applied, err
	}
	st, _, err := mysqlAgentStatus(ctx, rc, m.Name)
	if err != nil {
		return false, nil
	}
	if st.ConfigVersion < m.ConfigVersion {
		invalidateMySQLReadObservation(ctx, rc)
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return false, err
		}
		return false, a.Reload(ctx)
	}
	native, err := live(ctx, rc, m.Name)
	if err != nil || native.MemberState != "ONLINE" {
		return false, nil
	}
	if native.ServerUUID != "" {
		m.ServerUUID = native.ServerUUID
	}
	if native.DataIdentity.Initialized {
		m.Initialized = true
	}
	return true, nil
}

func configApplied(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	status, _, err := mysqlAgentStatus(ctx, rc, m.Name)
	if err != nil {
		return false, err
	}
	if status.ConfigVersion < m.ConfigVersion {
		invalidateMySQLReadObservation(ctx, rc)
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return false, err
		}
		return false, a.Reload(ctx)
	}
	return true, nil
}
