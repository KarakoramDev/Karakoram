// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

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
	for _, kind := range []string{kindNode, kindKibana} {
		for _, fault := range []string{"stop", "runtime-delete", "volume-delete"} {
			if kind == kindKibana && fault == "volume-delete" {
				continue
			}
			t.Run(kind+"/"+fault, func(t *testing.T) {
				p, rc, _, _ := coreSetup(t, 1)
				if kind != kindNode {
					component := rc.Cluster.Spec.Components.Extra[kind]
					component.Replicas = 1
					rc.Cluster.Spec.Components.Extra[kind] = component
				}
				rc.Agents = nil
				b := fake.New(fake.Options{})
				rc.Backend = b
				m := &member{Name: "demon0", Kind: kind, Stage: stageJoined, Containers: 1}
				if kind == kindNode {
					m.Volume = "demon0v"
				}
				s := &state{UID: "identity", Members: []*member{m}}
				token := &domain.AgentToken{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Token: "token"}
				if err := rc.Secrets.SetAgentToken(t.Context(), token); err != nil {
					t.Fatal(err)
				}
				specs := []infra.ServiceSpec{engine.RuntimeServiceSpec(b.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: token.Token, Volume: m.Volume, Containers: 1})}
				if m.Volume != "" {
					specs = append(specs, engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, token.Token, domain.Resources{}))
				}
				op, err := b.ImportServices(t.Context(), specs)
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
				if service == nil {
					t.Fatal("runtime not recreated")
				}
				if m.Volume != "" {
					volume, _ := b.GetService(t.Context(), m.Volume)
					if volume == nil || volume.ID != volumeID {
						t.Fatal("volume replaced")
					}
				}
				if fault == "runtime-delete" && m.Stage != stageImported {
					t.Fatal("missing runtime not handed to normal deploy", m.Stage)
				}
				if fault == "stop" && service.Status != infra.StatusActive {
					t.Fatal("stopped runtime not started")
				}
			})
		}
	}
}

func TestRuntimeRecoveryReplicatedVolumeAdmission(t *testing.T) {
	for _, healthySource := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing_primary_copy", true: "retained_primaries"}[healthySource], func(t *testing.T) {
			p, rc, backend, es := coreSetup(t, 3)
			s := coreConverge(t, p, rc, backend)
			lost := s.esNodes()[0]
			delete(backend.services, lost.Name)
			delete(backend.services, lost.Volume)
			delete(backend.nodes, lost.Name)
			if !healthySource {
				es.health = "red"
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
