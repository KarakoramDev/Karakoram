// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package fake

import (
	"context"
	"fmt"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type factoryState struct {
	mu       sync.Mutex
	backends map[string]*Backend
}

var defaultFactoryState = &factoryState{backends: map[string]*Backend{}}

// Factory creates clients that share infrastructure state for the same target.
// Its zero value uses the default fake project and capabilities.
type Factory struct {
	options Options
	state   *factoryState
}

var _ infra.Factory = Factory{}

// NewFactory creates an isolated factory with the supplied backend options.
func NewFactory(options Options) *Factory {
	return &Factory{options: defaults(options), state: &factoryState{backends: map[string]*Backend{}}}
}
func (f Factory) Info() infra.BackendInfo {
	o := defaults(f.options)
	return infra.BackendInfo{Kind: o.Name, DisplayName: o.Name, Capabilities: o.Capabilities}
}
func (f Factory) TargetFromEnv(getenv func(string) string) (infra.Target, error) {
	return infra.Target{Kind: f.Info().Kind, ID: getenv("KARAKORAM_FAKE_TARGET"), Token: getenv("KARAKORAM_FAKE_TOKEN")}, nil
}
func (f Factory) New(target infra.Target) (infra.Backend, error) {
	shared := f.state
	if shared == nil {
		shared = defaultFactoryState
	}
	shared.mu.Lock()
	defer shared.mu.Unlock()
	b := shared.backends[target.ID]
	if b == nil {
		b = New(f.options)
		shared.backends[target.ID] = b
	}
	return &Backend{options: b.options, state: b.state, token: target.Token}, nil
}
func (f Factory) Validate(ctx context.Context, target infra.Target) (infra.Backend, infra.Project, error) {
	o := defaults(f.options)
	if err := ctx.Err(); err != nil {
		return nil, infra.Project{}, err
	}
	if target.Token == "" || target.ID != o.Project.ID {
		return nil, infra.Project{}, fmt.Errorf("%w: target credential or identity rejected", infra.ErrUnauthorized)
	}
	b, err := f.New(target)
	if err != nil {
		return nil, infra.Project{}, err
	}
	return b, o.Project, nil
}
