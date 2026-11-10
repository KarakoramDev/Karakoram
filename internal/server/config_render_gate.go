// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

const configRenderWaitLimit = 30 * time.Second

type configRenderGate struct {
	held  chan struct{}
	users int // holders and waiters; guarded by configPullMu
}

// acquireConfigRender serializes the actual provider reads and publication for
// one member/capability. Active gates cannot be evicted; other keys stay parallel.
func (s *Server) acquireConfigRender(ctx context.Context, key configPullKey) (*configRenderGate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.configPullMu.Lock()
	if s.configRenderGates == nil {
		s.configRenderGates = make(map[configPullKey]*configRenderGate)
	}
	gate := s.configRenderGates[key]
	if gate == nil {
		if len(s.configRenderGates) >= maxConfigPullCertificates {
			for previous, candidate := range s.configRenderGates {
				if candidate.users == 0 {
					delete(s.configRenderGates, previous)
					break
				}
			}
		}
		if len(s.configRenderGates) >= maxConfigPullCertificates {
			s.configPullMu.Unlock()
			return nil, engine.ErrUnavailable
		}
		gate = &configRenderGate{held: make(chan struct{}, 1)}
		s.configRenderGates[key] = gate
	}
	gate.users++
	s.configPullMu.Unlock()
	acquired := func() (*configRenderGate, error) {
		if err := ctx.Err(); err != nil {
			s.releaseConfigRender(gate)
			return nil, err
		}
		return gate, nil
	}
	select {
	case gate.held <- struct{}{}:
		return acquired()
	default:
	}
	timer := time.NewTimer(configRenderWaitLimit)
	defer timer.Stop()
	select {
	case gate.held <- struct{}{}:
		return acquired()
	case <-ctx.Done():
		s.unrefConfigRender(gate)
		return nil, ctx.Err()
	case <-timer.C:
		s.unrefConfigRender(gate)
		return nil, context.DeadlineExceeded
	}
}

func (s *Server) unrefConfigRender(gate *configRenderGate) {
	s.configPullMu.Lock()
	gate.users--
	s.configPullMu.Unlock()
}

func (s *Server) releaseConfigRender(gate *configRenderGate) {
	<-gate.held
	s.unrefConfigRender(gate)
}
