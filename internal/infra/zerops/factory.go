// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type factory struct{ base Options }

// NewFactory constructs the registry factory with shared client options.
func NewFactory(base Options) infra.Factory { return factory{base: base} }

func (f factory) Info() infra.BackendInfo {
	return infra.BackendInfo{Kind: "zerops", DisplayName: "Zerops", Capabilities: zeropsCapabilities, Relay: true}
}

func (f factory) TargetFromEnv(getenv func(string) string) (infra.Target, error) {
	if _, err := deploymentLimit(f.base.MaxInflightDeploys, getenv("KARAKORAM_ZEROPS_MAX_INFLIGHT_DEPLOYS")); err != nil {
		return infra.Target{}, err
	}
	id := getenv("KARAKORAM_ZEROPS_PROJECT_ID")
	if id == "" {
		id = getenv("projectId")
	}
	return infra.Target{Kind: "zerops", ID: id, Token: getenv("KARAKORAM_ZEROPS_TOKEN")}, nil
}

func (f factory) options(t infra.Target) Options {
	o := f.base
	o.Token, o.ProjectID = t.Token, t.ID
	return o
}

func (f factory) New(t infra.Target) (infra.Backend, error) { return New(f.options(t)) }
func (f factory) Validate(ctx context.Context, t infra.Target) (infra.Backend, infra.Project, error) {
	b, p, err := ValidateProject(ctx, f.options(t))
	if err != nil {
		return nil, p, err
	}
	return b, p, nil
}
