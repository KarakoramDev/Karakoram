// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"testing"
)

func TestComponentRuntimeRecovery(t *testing.T) {
	for _, fault := range []string{"stop", "runtime-delete"} {
		t.Run(fault, func(t *testing.T) {
			_, rc, _ := setupProvider(t)
			rc.Agents = nil
			b := fake.New(fake.Options{})
			rc.Backend = b
			m := &componentMember{Name: "demoauth", Component: "auth", Stage: engine.ComponentJoined, Replicas: 1}
			token := &domain.AgentToken{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Token: "token"}
			if err := rc.Secrets.SetAgentToken(t.Context(), token); err != nil {
				t.Fatal(err)
			}
			op, err := b.ImportServices(t.Context(), []infra.ServiceSpec{engine.RuntimeServiceSpec(b.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: token.Token, Containers: 1})})
			if err != nil {
				t.Fatal(err)
			}
			m.ServiceID = op.Services[m.Name]
			if fault == "stop" {
				b.Stop(t.Context(), m.Name)
			} else {
				b.DeleteService(t.Context(), m.Name)
			}
			busy, err := recoverComponentRuntime(t.Context(), rc, m)
			if err != nil || !busy {
				t.Fatal("component runtime repair not started", busy, err)
			}
			if fault == "runtime-delete" {
				plan := m.RuntimeRecovery.ImportPlan
				service, _ := b.GetService(t.Context(), m.Name)
				if plan == nil || len(plan.Specs) != 1 || plan.Specs[0].Hostname != m.Name || plan.Specs[0].Kind != infra.KindRuntime || m.RuntimeRecovery.Op != nil || service != nil {
					t.Fatal("component runtime-only intent was not persisted before submission")
				}
			}
			if _, err = recoverComponentRuntime(t.Context(), rc, m); err != nil {
				t.Fatal(err)
			}
			if fault == "runtime-delete" {
				if m.RuntimeRecovery.Op == nil || m.RuntimeRecovery.ImportPlan == nil {
					t.Fatal("component runtime import lost its plan or accepted handle")
				}
				if _, err = recoverComponentRuntime(t.Context(), rc, m); err != nil {
					t.Fatal(err)
				}
			}
			service, _ := b.GetService(t.Context(), m.Name)
			if service == nil {
				t.Fatal("runtime not recreated")
			}
			if fault == "runtime-delete" && m.Stage != engine.ComponentImported {
				t.Fatal("missing runtime not handed to normal deploy", m.Stage)
			}
			if fault == "stop" && service.Status != infra.StatusActive {
				t.Fatal("stopped runtime not started")
			}
			services, _ := b.ListServices(t.Context(), "")
			for _, service := range services {
				if service.Kind == infra.KindVolume {
					t.Fatal("stateless service created storage")
				}
			}
		})
	}
}
