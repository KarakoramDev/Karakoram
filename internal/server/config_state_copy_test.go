// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestServerCopiesShareConfigSynchronizationState(t *testing.T) {
	s, _, _ := testServer(t)
	local := *s
	if s.configPullState == nil || local.configPullState != s.configPullState {
		t.Fatal("constructor/request copy did not share config state")
	}
	key := configPullKey{cluster: domain.ClusterRef{Name: "cluster"}, member: "member"}
	cfg := agentapi.MemberConfig{Cluster: "cluster", Member: "member"}
	if err := s.issueConfigRevision(key, "inputs", "process", &cfg); err != nil {
		t.Fatal(err)
	}
	if local.conditionalConfigRevision(key, "inputs", true) != cfg.PullRevision {
		t.Fatal("request copy lost the shared certificate")
	}
	holder, err := s.acquireConfigRender(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer s.releaseConfigRender(holder)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	admission := &configGateAdmissionContext{Context: ctx, server: &local, key: key, waiting: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		gate, err := local.acquireConfigRender(admission, key)
		if gate != nil {
			local.releaseConfigRender(gate)
		}
		result <- err
	}()
	<-admission.waiting
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal("copied server bypassed the original render gate", err)
	}
	s.configPullMu.Lock()
	shared := local.configRenderGates[key] == holder && holder.users == 1
	s.configPullMu.Unlock()
	if !shared {
		t.Fatal("request copy used a different gate/refcount domain")
	}
}
