// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestVolumeRecoveryRequiresRetainedWritablePrimary(t *testing.T) {
	lost := &member{Name: "lost", Set: "rs0"}
	source := &member{Name: "source", Set: "rs0"}
	s := &internalState{Members: []*member{lost, source}}
	for _, scenario := range []string{"no-primary", "lost-is-primary", "retained-primary", "unowned-primary", "source-volume-lost", "source-removing"} {
		t.Run(scenario, func(t *testing.T) {
			source.Removing = false
			source.RuntimeRecovery.VolumeLost = false
			primary := mongoadmin.Member{Name: "source:27017", State: "PRIMARY", Health: 1}
			switch scenario {
			case "no-primary":
				primary.State = "SECONDARY"
			case "lost-is-primary":
				primary.Name = "lost:27017"
			case "unowned-primary":
				primary.Name = "external:27017"
			case "source-volume-lost":
				source.RuntimeRecovery.VolumeLost = true
			case "source-removing":
				source.Removing = true
			}
			if got := retainedPrimary(s, lost, mongoadmin.Status{Members: []mongoadmin.Member{primary}}); got != (scenario == "retained-primary") {
				t.Fatalf("safe=%v", got)
			}
		})
	}
}

type testBalancerControl struct {
	enabled      bool
	mutations    int
	observations int
}

func (b *testBalancerControl) Balancer(context.Context, *engine.Context) (domain.BalancerStatus, error) {
	b.observations++
	return domain.BalancerStatus{Enabled: b.enabled}, nil
}
func (b *testBalancerControl) SetBalancer(_ context.Context, _ *engine.Context, enabled bool) error {
	b.enabled = enabled
	b.mutations++
	return nil
}
func TestUpgradeBalancerIntentSurvivesDisableLostReply(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			s := &internalState{}
			control := &testBalancerControl{enabled: enabled}
			if err := stopUpgradeBalancer(t.Context(), nil, s, control); err != nil {
				t.Fatal(err)
			}
			if control.mutations != 0 {
				t.Fatal("mutated before original balancer state could commit")
			}
			persisted, _ := json.Marshal(s)
			if err := stopUpgradeBalancer(t.Context(), nil, s, control); err != nil {
				t.Fatal(err)
			}
			// The successful disable reply/checkpoint is lost. Restore pre-action state.
			if err := json.Unmarshal(persisted, &s); err != nil {
				t.Fatal(err)
			}
			if err := stopUpgradeBalancer(t.Context(), nil, s, control); err != nil {
				t.Fatal(err)
			}
			if !s.BalancerStopped || s.BalancerWasEnabled != enabled || control.observations != 1 {
				t.Fatalf("lost operator intent: %+v observations=%d", s, control.observations)
			}
		})
	}
}

type confirmedLossBackend struct {
	*fakeInfra
	volume  string
	service *infra.Service
	err     error
}

func (b *confirmedLossBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	if name == b.volume {
		return b.service, b.err
	}
	return b.fakeInfra.GetService(ctx, name)
}

func TestConfirmedLostVolumeDoesNotBlockNativeReplacementOwnership(t *testing.T) {
	for _, scenario := range []string{"matched", "runtime-matched", "runtime-foreign", "uninitialized", "foreign-error-id", "foreign-returned-service", "api-error"} {
		t.Run(scenario, func(t *testing.T) {
			_, rc, backend, _ := setup(t)
			m := &member{Name: "member", ServiceID: "runtime-id", Volume: "memberv", VolumeID: "volume-id", RuntimeRecovery: engine.RuntimeRecovery{Initialized: true}}
			loss := &infra.VolumeLostError{Volume: m.Volume, ServiceID: m.VolumeID, Message: "physical volume missing"}
			fault := &confirmedLossBackend{fakeInfra: backend, volume: m.Volume, err: loss}
			switch scenario {
			case "runtime-matched", "runtime-foreign":
				fault.volume = m.Name
				fault.service = &infra.Service{ID: m.ServiceID, Hostname: m.Name, Kind: infra.KindRuntime}
				if scenario == "runtime-foreign" {
					fault.service.ID = "foreign-runtime"
				}
			case "uninitialized":
				m.RuntimeRecovery.Initialized = false
			case "foreign-error-id":
				loss.ServiceID = "different-incarnation"
			case "foreign-returned-service":
				fault.service = &infra.Service{ID: "foreign", Hostname: m.Volume, Kind: infra.KindVolume}
			case "api-error":
				fault.err = errors.New("storage API unavailable")
			}
			rc.Backend = fault
			err := verifyMemberServices(t.Context(), rc, m)
			if (err == nil) != (scenario == "matched" || scenario == "runtime-matched") {
				t.Fatalf("ownership result: %v", err)
			}
			if m.VolumeID != "volume-id" || backend.mutations != 0 {
				t.Fatal("lost storage identity was adopted, recreated or mutated")
			}
			if (scenario == "matched" || scenario == "runtime-matched") && !m.RuntimeRecovery.VolumeLost {
				t.Fatal("confirmed loss not retained")
			}
		})
	}
}
