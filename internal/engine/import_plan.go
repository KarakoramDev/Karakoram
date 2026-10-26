// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"time"
)

// ImportPlan freezes a backend batch before submission without persisting bearer
// credentials. The member's sealed token remains stable throughout an import.
// Callers must return for persistence after creating the plan, before Submit.
type ImportPlan struct {
	RetrySchedule
	Specs   []infra.ServiceSpec `json:"specs"`
	RetryAt time.Time           `json:"retryAt,omitempty"`
}

func NewImportPlan(specs []infra.ServiceSpec) *ImportPlan {
	plan := &ImportPlan{Specs: make([]infra.ServiceSpec, len(specs))}
	for i, spec := range specs {
		spec.Env = nil
		plan.Specs[i] = spec
	}
	return plan
}

func (p *ImportPlan) Services(layout infra.Layout, cluster, member, token string) []infra.ServiceSpec {
	specs := make([]infra.ServiceSpec, len(p.Specs))
	for i, spec := range p.Specs {
		spec.Env = OwnershipEnv(cluster, member, token)
		if spec.Kind == infra.KindRuntime && spec.Volume != "" {
			spec.Env[DataDirVariable] = layout.VolumePath(spec.Volume)
		}
		specs[i] = spec
	}
	return specs
}

func (p *ImportPlan) Submit(ctx context.Context, rc *Context, member, token string) (infra.Op, error) {
	now := time.Now().UTC()
	if rc.Clock != nil {
		now = rc.Clock.Now()
	}
	if p.Waiting(now, p.RetryAt) {
		return infra.Op{}, &PlatformRetryWait{RetryAt: p.RetryAt, Message: "Waiting to resume the original import batch"}
	}
	op, err := rc.Backend.ImportServices(ctx, p.Services(rc.Backend.Layout(), rc.Cluster.Name, member, token))
	if err != nil {
		p.RetryAt = p.Schedule(now, now.Add(time.Minute))
		if reset := infra.RetryAt(err); reset.After(p.RetryAt) {
			p.RetryAt = p.Schedule(now, reset)
		}
	} else {
		p.RetryAt = time.Time{}
		p.RetrySchedule = RetrySchedule{}
	}
	return op, err
}

// ResumeImportPlan finishes backend-owned setup before operation polling or
// cleanup can wait on, or mutate, an incomplete logical reservation.
func ResumeImportPlan(ctx context.Context, rc *Context, plan *ImportPlan, member, token string) (*infra.Op, error) {
	if plan == nil {
		return nil, nil
	}
	for _, spec := range plan.Specs {
		service, err := rc.Backend.GetService(ctx, spec.Hostname)
		if err != nil {
			return nil, err
		}
		if service != nil && service.Importing {
			op, err := plan.Submit(ctx, rc, member, token)
			if err != nil {
				return nil, err
			}
			return &op, nil
		}
	}
	return nil, nil
}
