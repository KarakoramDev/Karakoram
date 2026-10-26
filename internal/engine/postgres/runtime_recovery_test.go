// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"testing"
)

func TestMemberRuntimeRecovery(t *testing.T) {
	for _, fault := range []string{"stop", "runtime-delete", "volume-delete"} {
		t.Run(fault, func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			rc.Agents = nil
			b := fake.New(fake.Options{})
			rc.Backend = b
			m := &member{Name: "demop0", Volume: "demop0v", Stage: "joined", Role: "data", Replicas: 1}
			s := &state{UID: "identity", Topology: "single", Members: []*member{m}}
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
			if fault == "runtime-delete" {
				plan := m.RuntimeRecovery.ImportPlan
				service, _ := b.GetService(t.Context(), m.Name)
				if plan == nil || len(plan.Specs) != 1 || plan.Specs[0].Hostname != m.Name || plan.Specs[0].Kind != infra.KindRuntime || m.RuntimeRecovery.Op != nil || service != nil {
					t.Fatal("runtime-only intent was not persisted before submission")
				}
			}
			if _, err = p.recoverRuntimes(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if fault == "runtime-delete" {
				if m.RuntimeRecovery.Op == nil || m.RuntimeRecovery.ImportPlan == nil {
					t.Fatal("runtime-only import lost its plan or accepted handle")
				}
				if _, err = p.recoverRuntimes(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
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

type runtimeLossBackend struct {
	infra.Backend
	hostname string
	loss     *infra.VolumeLostError
	foreign  bool
}

func (b runtimeLossBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	service, err := b.Backend.GetService(ctx, name)
	if err != nil || name != b.hostname {
		return service, err
	}
	if b.foreign && service != nil {
		service.ID = "foreign"
		service.Ownership = nil
	}
	return service, b.loss
}
func TestRuntimeRecoveryOwnershipClassifiesPhysicalLoss(t *testing.T) {
	for _, volumeObservation := range []bool{false, true} {
		for _, foreign := range []bool{false, true} {
			t.Run(map[bool]string{false: "runtime", true: "volume"}[volumeObservation]+"/"+map[bool]string{false: "owned", true: "foreign"}[foreign], func(t *testing.T) {
				_, rc, _ := setup(t, "single")
				b := fake.New(fake.Options{})
				m := &member{Name: "demop0", Volume: "demop0v", RuntimeRecovery: engine.RuntimeRecovery{Initialized: true}}
				token := &domain.AgentToken{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Token: "token"}
				if err := rc.Secrets.SetAgentToken(t.Context(), token); err != nil {
					t.Fatal(err)
				}
				op, err := b.ImportServices(t.Context(), []infra.ServiceSpec{engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, token.Token, domain.Resources{}), engine.RuntimeServiceSpec(b.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: token.Token, Volume: m.Volume})})
				if err != nil {
					t.Fatal(err)
				}
				m.ServiceID = op.Services[m.Name]
				m.VolumeID = op.Services[m.Volume]
				hostname := m.Name
				if volumeObservation {
					hostname = m.Volume
				}
				rc.Backend = runtimeLossBackend{Backend: b, hostname: hostname, loss: &infra.VolumeLostError{Volume: m.Volume, ServiceID: m.VolumeID, Message: "physical storage absent"}, foreign: foreign}
				err = verifyMember(t.Context(), rc, m)
				if foreign {
					if !errors.Is(err, engine.ErrConflict) {
						t.Fatal("foreign service bypassed ownership", err)
					}
				} else if err != nil || !m.RuntimeRecovery.VolumeLost || m.VolumeID != op.Services[m.Volume] {
					t.Fatal("owned loss not preserved", err, m.RuntimeRecovery)
				}
			})
		}
	}
}

func TestRuntimeRecoveryReplicatedVolumeAdmission(t *testing.T) {
	for _, healthySource := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_writable_source", true: "retained_primary"}[healthySource], func(t *testing.T) {
			p, rc, backend := setup(t, "ha")
			s := converge(t, p, rc)
			lost := s.data()[1]
			delete(backend.services, lost.Name)
			delete(backend.services, lost.Volume)
			if !healthySource {
				backend.statusOverrides = map[string]pgspec.StatusResponse{}
				for _, m := range s.data() {
					backend.statusOverrides[m.Name] = pgspec.StatusResponse{Running: false, Role: "stopped", EtcdHealthy: false}
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

func TestRuntimeRecoveryLostDataUsesInfrastructureFence(t *testing.T) {
	for _, healthySource := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_surviving_writer", true: "reachable_agent_lost_data"}[healthySource], func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			m := s.find("demop1")
			m.RuntimeRecovery.VolumeLost = true
			rc.Backend = runtimeLossBackend{Backend: b, hostname: m.Name, loss: &infra.VolumeLostError{Volume: m.Volume, ServiceID: m.VolumeID, Message: "physical data absent"}}
			if !healthySource {
				b.statusOverrides = map[string]pgspec.StatusResponse{}
				for _, member := range s.data() {
					b.statusOverrides[member.Name] = pgspec.StatusResponse{Running: false, Role: "stopped"}
				}
			}
			cause := errors.New("native engine unavailable because initialized data is lost")
			ok, err := retirementRuntimeFallback(t.Context(), rc, s, m, cause)
			if !healthySource {
				if err == nil || ok || m.Retirement != "" {
					t.Fatal("infrastructure fence without surviving writer", ok, err, m.Retirement)
				}
				return
			}
			if err != nil || ok || m.Retirement != "runtime-stop" {
				t.Fatal("reachable control plane deadlocked lost-data retirement", ok, err, m.Retirement)
			}
			for range 5 {
				if _, err := fenceRetirement(t.Context(), rc, s, m); err != nil {
					t.Fatal(err)
				}
				if m.Retirement == "runtime-wait" {
					break
				}
			}
			if m.Retirement != "runtime-wait" {
				t.Fatal("original runtime stop was not requested", m.Retirement)
			}
			b.runtimeStopBusy = true
			if ok, err := fenceRetirement(t.Context(), rc, s, m); err != nil || ok || m.Retirement != "runtime-wait" {
				t.Fatal("storage loss or desired stop bypassed actual stop receipt", ok, err)
			}
			b.runtimeStopBusy = false
			for range 4 {
				if ok, err := fenceRetirement(t.Context(), rc, s, m); err != nil {
					t.Fatal(err)
				} else if ok {
					if m.Retirement != "runtime-stopped" || b.services[m.Name].Status != infra.StatusStopped {
						t.Fatal("retirement lacked original stopped proof")
					}
					return
				}
			}
			t.Fatal("verified runtime stop did not finish data-loss fence")
		})
	}
}
