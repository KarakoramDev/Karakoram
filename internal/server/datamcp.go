// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/datamcp"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companion"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/nativees"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/nativermq"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (s *Server) initDataMcp() error {
	switch s.MCPMode {
	case "", "off", "readonly", "readwrite":
	default:
		return errors.New("KARAKORAM_MCP must be off, readonly or readwrite")
	}
	manifests := datamcp.CompanionManifests()
	for _, m := range []datamcp.Manifest{nativees.Manifest(), nativermq.Manifest()} {
		manifests[m.Upstream] = m
	}
	factory := s.DataMcpUpstreams
	if factory == nil {
		factory = &dataMcpFactory{server: s, manifests: manifests}
	}
	var err error
	s.dataMcp, err = datamcp.New(datamcp.Options{
		Authenticate: s.authenticateDataMcp, Authorize: s.RBAC.Can,
		Enabled:  func() bool { return s.MCPMode != "off" },
		Writes:   func() bool { return s.MCPMode == "readwrite" },
		Clusters: s.Store, Audit: s.recordAudit, PublicURL: s.PublicURL,
		Upstreams: factory, Manifests: manifests, Logger: s.Logger,
	})
	return err
}

// Close releases cached data MCP sessions and transports after HTTP shutdown.
func (s *Server) Close() error { return s.dataMcp.Close() }

func (s *Server) authenticateDataMcp(r *http.Request) (datamcp.Caller, error) {
	if r.Header.Get("Authorization") == "" || r.Header.Get("Cookie") != "" {
		return datamcp.Caller{}, auth.ErrUnauthenticated
	}
	p, err := s.Auth.Authenticate(r)
	if err != nil {
		return datamcp.Caller{}, err
	}
	if p.TokenID == "" || p.Via != "token" {
		return datamcp.Caller{}, auth.ErrUnauthenticated
	}
	caller := datamcp.Caller{TokenID: p.TokenID, Actor: p.Actor(), Subjects: p.Subjects}
	if r := p.Restrictions; r != nil {
		caller.Scope = &datamcp.TokenScope{ReadOnly: r.ReadOnly, Namespaces: r.Namespaces, Clusters: r.Clusters, Permissions: r.Permissions}
	}
	return caller, nil
}

type dataMcpFactory struct {
	server    *Server
	manifests map[string]datamcp.Manifest
}

func (f *dataMcpFactory) provider(c *domain.Cluster) (engine.DataMcpProvider, engine.DataMcpBackend, error) {
	p, err := f.server.Registry.Get(c.Spec.Provider)
	if err != nil {
		return nil, engine.DataMcpBackend{}, err
	}
	dp, ok := p.(engine.DataMcpProvider)
	if !ok {
		return nil, engine.DataMcpBackend{}, errors.New("provider does not support data MCP")
	}
	backend, err := dp.DataMcp(c)
	if err != nil {
		return nil, backend, err
	}
	if _, ok := f.manifests[backend.Upstream]; !ok {
		return nil, backend, errors.New("unknown data MCP manifest")
	}
	if backend.Kind != engine.DataMcpCompanion && backend.Kind != engine.DataMcpNative {
		return nil, backend, errors.New("unknown data MCP backend")
	}
	return dp, backend, nil
}

func (f *dataMcpFactory) Manifest(_ context.Context, c *domain.Cluster) (string, error) {
	_, backend, err := f.provider(c)
	return backend.Upstream, err
}

func (f *dataMcpFactory) context(c *domain.Cluster) (*engine.Context, error) {
	rc := f.server.Context
	rc.Cluster = c
	rc.Namespace = domain.NormalizedNamespace(c.Namespace)
	if failed := f.server.providerContext(&rc); failed != nil {
		return nil, errors.New("data MCP namespace unavailable")
	}
	return &rc, nil
}

func (f *dataMcpFactory) Fingerprint(ctx context.Context, c *domain.Cluster, level engine.DataAccessLevel) (string, error) {
	p, backend, err := f.provider(c)
	if err != nil {
		return "", err
	}
	rc, err := f.context(c)
	if err != nil {
		return "", err
	}
	if backend.Kind == engine.DataMcpCompanion {
		return companion.Fingerprint(ctx, rc, level)
	}
	credentials, err := p.DataAccessCredentials(ctx, rc, level)
	if err != nil {
		return "", err
	}
	// Native toolsets capture scope at Open, so spec changes retire them too.
	material, err := json.Marshal(struct {
		Credentials engine.DataAccessCredentials
		Generation  int64
		Scope       domain.DataMcpScope
	}{credentials, c.Generation, c.Spec.DataMcp.Scope})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(material)
	return hex.EncodeToString(sum[:]), nil
}

func (f *dataMcpFactory) Open(ctx context.Context, c *domain.Cluster, level engine.DataAccessLevel) (datamcp.Upstream, error) {
	p, backend, err := f.provider(c)
	if err != nil {
		return nil, err
	}
	rc, err := f.context(c)
	if err != nil {
		return nil, err
	}
	if backend.Kind == engine.DataMcpCompanion {
		transport, err := companion.Transport(ctx, rc, level)
		if err != nil {
			return nil, err
		}
		return datamcp.NewMCPUpstream(ctx, transport)
	}
	credentials, err := p.DataAccessCredentials(ctx, rc, level)
	if err != nil {
		return nil, err
	}
	input := datamcp.NativeInput{Credentials: credentials, Dialer: rc.Dialer, Scope: c.Spec.DataMcp.Scope, Level: level}
	switch backend.Upstream {
	case nativees.Manifest().Upstream:
		return nativees.Open(ctx, input)
	case nativermq.Manifest().Upstream:
		return nativermq.Open(ctx, input)
	default:
		return nil, errors.New("unknown native data MCP backend")
	}
}
