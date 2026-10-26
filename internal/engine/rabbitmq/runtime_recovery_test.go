// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"testing"
)

func TestMemberRuntimeRecovery(t *testing.T) {
	for _, fault := range []string{"stop", "runtime-delete", "volume-delete"} {
		t.Run(fault, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			rc.Agents = nil
			rc.Cluster.Spec.Components.Engine.Replicas = 1
			b := fake.New(fake.Options{})
			rc.Backend = b
			m := &member{Name: "demom0", Volume: "demom0v", Stage: "ready"}
			s := &internalState{UID: "identity", Members: []*member{m}}
			token := &domain.AgentToken{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Token: "token"}
			if err := rc.Secrets.SetAgentToken(t.Context(), token); err != nil {
				t.Fatal(err)
			}
			op, err := b.ImportServices(t.Context(), []infra.ServiceSpec{engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, token.Token, domain.Resources{}), engine.RuntimeServiceSpec(b.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: token.Token, Volume: m.Volume, Containers: 1})})
			if err != nil {
				t.Fatal(err)
			}
			m.ServiceID = op.Services[m.Name]
			m.VolumeID = op.Services[m.Volume]
			volumeID := m.VolumeID
			switch fault {
			case "stop":
				b.Stop(t.Context(), m.Name)
			case "runtime-delete":
				b.DeleteService(t.Context(), m.Name)
			case "volume-delete":
				b.DeleteService(t.Context(), m.Name)
				b.DeleteService(t.Context(), m.Volume)
			}
			busy, err := p.recoverRuntimes(t.Context(), rc, s)
			if fault == "volume-delete" {
				if !busy || !errors.Is(err, engine.ErrConflict) || !m.RuntimeRecovery.VolumeLost {
					t.Fatal("singleton data loss not refused", busy, err)
				}
				return
			}
			if err != nil || !busy {
				t.Fatal("runtime repair not started", busy, err)
			}
			if _, err = p.recoverRuntimes(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if _, err = p.recoverRuntimes(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			service, _ := b.GetService(t.Context(), m.Name)
			volume, _ := b.GetService(t.Context(), m.Volume)
			if service == nil || volume == nil || volume.ID != volumeID {
				t.Fatal("identity-preserving recovery failed")
			}
			if fault == "runtime-delete" && m.Stage != "imported" {
				t.Fatal("missing runtime not handed to normal deploy", m.Stage)
			}
			if fault == "stop" && service.Status != infra.StatusActive {
				t.Fatal("stopped runtime not started")
			}
		})
	}
}

func TestRuntimeRecoveryReplicatedVolumeAdmission(t *testing.T) {
	for _, healthySource := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_surviving_majority", true: "retained_majority"}[healthySource], func(t *testing.T) {
			p, rc, backend, agents := setup(t)
			ready(t, p, rc)
			s := state(t, rc)
			lost := s.Members[0]
			delete(backend.services, lost.Name)
			delete(backend.services, lost.Volume)
			agents.broker(lost.Name).down = true
			if !healthySource {
				for _, m := range s.Members {
					agents.broker(m.Name).down = true
				}
			}
			busy, err := p.recoverRuntimes(t.Context(), rc, s)
			if !busy || (err == nil) != healthySource || !lost.RuntimeRecovery.VolumeLost {
				t.Fatal("wrong native recovery admission", busy, err)
			}
			ops, err := rc.Store.(*store.Store).ListOperations(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, op := range ops {
				if op.Kind == domain.OperationReplaceMember {
					count++
				}
			}
			if (count == 1) != healthySource {
				t.Fatal("replacement queue ignored surviving-data evidence", count)
			}
			if backend.services[lost.Volume] != nil {
				t.Fatal("lost volume recreated")
			}
		})
	}
}
