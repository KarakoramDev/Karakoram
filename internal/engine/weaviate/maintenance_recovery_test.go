// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

func TestMaintenanceWaitDoesNotBlockRuntimeRecovery(t *testing.T) {
	for _, change := range []string{"scale-in", "rolling"} {
		t.Run(change, func(t *testing.T) {
			f := coreSetup(t, 4)
			before := coreConverge(t, f)
			m := before.Members[0]
			if change == "scale-in" {
				f.rc.Cluster.Spec.Components.Engine.Replicas = 3
			} else {
				f.rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
				f.rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
				if err := f.p.startRolling(t.Context(), f.rc, before, "config", "", ""); err != nil {
					t.Fatal(err)
				}
				if err := before.save(f.rc.Cluster); err != nil {
					t.Fatal(err)
				}
			}
			f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			if _, err := f.backend.DeleteServiceByID(t.Context(), m.Name, m.ServiceID); err != nil {
				t.Fatal(err)
			}
			recovered := false
			for range 100 {
				if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
					t.Fatal(err)
				}
				s, _ := load(f.rc.Cluster)
				svc, _ := f.backend.GetService(t.Context(), m.Name)
				if svc != nil && s.find(m.Name).Stage == "ready" && s.Pending == nil {
					recovered = true
					break
				}
			}
			s, _ := load(f.rc.Cluster)
			if !recovered || s.find(m.Name).Resources != m.Resources || len(s.Members) != 4 {
				t.Fatal("maintenance wait blocked accepted runtime recovery")
			}
		})
	}
}

type lateResourceRejection struct {
	infra.Backend
	member  string
	polling bool
	rejects int
}

func (*lateResourceRejection) PlanResources(context.Context, string, infra.Resources) error {
	return nil
}
func (b *lateResourceRejection) SetResources(ctx context.Context, name string, r infra.Resources) (infra.Op, error) {
	if name != b.member {
		return b.Backend.SetResources(ctx, name, r)
	}
	b.rejects++
	if b.polling {
		return infra.Op{ID: "rejected-resource"}, nil
	}
	return infra.Op{}, &infra.ResourceUpdateRejectedError{Message: "capacity changed after admission"}
}
func (b *lateResourceRejection) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "rejected-resource" {
		return infra.OpState{}, &infra.ResourceUpdateRejectedError{Message: "replacement refused"}
	}
	return b.Backend.OpStatus(ctx, op)
}
func TestResourceRejectionAfterPreflightAllowsRevert(t *testing.T) {
	for _, polling := range []bool{false, true} {
		t.Run(fmt.Sprint(polling), func(t *testing.T) {
			f := coreSetup(t, 1)
			before := coreConverge(t, f)
			m := before.Members[0]
			old, hash := m.Resources, m.ConfigHash
			f.rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
			f.rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
			backend := &lateResourceRejection{Backend: f.backend, member: m.Name, polling: polling}
			f.rc.Backend = backend
			rejected := false
			for range 30 {
				_, err := f.p.Sync(t.Context(), f.rc)
				if errors.Is(err, infra.ErrRejected) {
					rejected = true
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			s, _ := load(f.rc.Cluster)
			if !rejected || s.Pending != nil || s.Rolling != nil || s.find(m.Name).Resources != old || s.find(m.Name).ConfigHash != hash {
				t.Fatal("definitive refusal retained rejected intent or changed applied state")
			}
			f.rc.Cluster.Spec.Components.Engine.Resources = old
			f.rc.Cluster.Generation++
			for range 10 {
				if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
					t.Fatal("revert blocked", err)
				}
			}
			if backend.rejects != 1 {
				t.Fatal("revert resubmitted rejected resources", backend.rejects)
			}
		})
	}
}

func TestMaintenanceResourceWaitDoesNotBlockTransportRenewal(t *testing.T) {
	f := coreSetup(t, 1)
	before := coreConverge(t, f)
	m := before.Members[0]
	config, err := f.p.MemberConfig(t.Context(), f.rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	accepted := string(config.EngineSpec)
	config.AgentTLS = agentapi.Certificate{}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = putSecret(t.Context(), f.rc, "config/"+m.Name, raw); err != nil {
		t.Fatal(err)
	}
	f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	f.rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
	f.rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
	for range 10 {
		if _, err = f.p.Sync(t.Context(), f.rc); err != nil {
			t.Fatal(err)
		}
	}
	config, err = f.p.MemberConfig(t.Context(), f.rc, m.Name)
	if err != nil || config.AgentTLS.CertPEM == "" || string(config.EngineSpec) != accepted {
		t.Fatal("resource wait blocked transport renewal or changed application config", err)
	}
	s, _ := load(f.rc.Cluster)
	if s.Members[0].Resources != m.Resources {
		t.Fatal("transport renewal applied deferred resources")
	}
}
