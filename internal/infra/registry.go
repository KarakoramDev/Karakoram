// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Target selects one backend target and its credential. A namespace maps to
// exactly one target: a Zerops project, a Dokploy environment, a Nomad
// namespace.
type Target struct {
	// Kind is the registered backend kind.
	Kind string
	// ID identifies the target within the backend (project, environment or
	// namespace ID). It is immutable for a namespace.
	ID string
	// Token is the credential. It is sealed at rest and never returned.
	Token string `json:"-"`
	// Config holds non-secret backend settings such as an API URL; accepted
	// keys are listed in BackendInfo.ConfigKeys.
	Config map[string]string
}

// BackendInfo describes a registered backend kind.
type BackendInfo struct {
	Kind         string       `json:"kind"`
	DisplayName  string       `json:"displayName"`
	Experimental bool         `json:"experimental"`
	Capabilities Capabilities `json:"capabilities"`
	// Relay reports that clusters in additional (non-default) namespaces are
	// reached through an outbound relay deployed into the target.
	Relay bool `json:"relay"`
	// ConfigKeys lists accepted Target.Config keys.
	ConfigKeys []string `json:"configKeys,omitempty"`
}

// Factory constructs backends of one kind.
type Factory interface {
	Info() BackendInfo
	// TargetFromEnv reads the default namespace's target from the process
	// environment of the control plane.
	TargetFromEnv(getenv func(string) string) (Target, error)
	// New builds a backend for a target without network calls.
	New(Target) (Backend, error)
	// Validate builds an isolated backend and verifies the target's identity
	// and the credential's permissions. Credential rejection matches
	// ErrUnauthorized; outages match ErrUnavailable.
	Validate(context.Context, Target) (Backend, Project, error)
}

// Registry holds the compiled-in backend factories by kind.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

// NewRegistry registers factories, rejecting duplicate or empty kinds.
func NewRegistry(factories ...Factory) (*Registry, error) {
	r := &Registry{factories: map[string]Factory{}}
	for _, f := range factories {
		if err := r.Register(f); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Register adds a uniquely named factory.
func (r *Registry) Register(f Factory) error {
	if f == nil || f.Info().Kind == "" {
		return errors.New("backend kind is required")
	}
	kind := f.Info().Kind
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.factories == nil {
		r.factories = map[string]Factory{}
	}
	if _, ok := r.factories[kind]; ok {
		return fmt.Errorf("backend %q already registered", kind)
	}
	r.factories[kind] = f
	return nil
}

// Get returns the factory registered for kind.
func (r *Registry) Get(kind string) (Factory, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.factories[kind]
	if !ok {
		return nil, fmt.Errorf("%w: unknown backend %q", ErrUnsupported, kind)
	}
	return f, nil
}

// List returns backend descriptions in kind order.
func (r *Registry) List() []BackendInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]BackendInfo, 0, len(r.factories))
	for _, f := range r.factories {
		out = append(out, f.Info())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
	return out
}

// New builds a backend for t through its kind's factory.
func (r *Registry) New(t Target) (Backend, error) {
	f, err := r.Get(t.Kind)
	if err != nil {
		return nil, err
	}
	if err := checkConfig(f.Info(), t); err != nil {
		return nil, err
	}
	return f.New(t)
}

// Validate verifies t through its kind's factory.
func (r *Registry) Validate(ctx context.Context, t Target) (Backend, Project, error) {
	f, err := r.Get(t.Kind)
	if err != nil {
		return nil, Project{}, err
	}
	if err := checkConfig(f.Info(), t); err != nil {
		return nil, Project{}, err
	}
	return f.Validate(ctx, t)
}

func checkConfig(info BackendInfo, t Target) error {
	for key := range t.Config {
		known := false
		for _, k := range info.ConfigKeys {
			known = known || k == key
		}
		if !known {
			return &RejectedError{Code: "unknownConfigKey", Message: fmt.Sprintf("backend %s does not accept setting %q", info.Kind, key)}
		}
	}
	return nil
}
