// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"crypto/x509"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"net"
	"net/url"
	"strconv"
	"time"
)

func postgresBindingRequest(c *domain.Cluster) (pgbinding.Request, error) {
	o, err := parseOptions(c.Spec.EngineOptions)
	if err != nil {
		return pgbinding.Request{}, err
	}
	if o.Storage.Type != "postgresql" || o.Storage.PostgreSQL == nil {
		return pgbinding.Request{}, invalid("PostgreSQL storage is not configured")
	}
	return pgbinding.Request{ConsumerKind: "openbao", ConsumerNamespace: c.Ref().Namespace, ConsumerName: c.Name, SourceNamespace: c.Ref().Namespace, SourceCluster: o.Storage.PostgreSQL.Cluster, Databases: []string{"openbao"}, MinMajor: 13}, nil
}

func (p *Provider) ensurePostgresStorage(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	if !p.postgresStorage(rc.Cluster) {
		return false, nil
	}
	if p.options.Bindings == nil {
		return false, invalid("PostgreSQL storage requires the PostgreSQL binding service")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var c pgbinding.Connection
	var e error
	if s.BindingID == "" {
		request, err := postgresBindingRequest(rc.Cluster)
		if err != nil {
			return false, err
		}
		c, e = p.options.Bindings.Ensure(ctx, request)
		if e != nil {
			return false, fmt.Errorf("OpenBao PostgreSQL binding is unavailable")
		}
		if c.BindingID == "" {
			return false, fmt.Errorf("OpenBao PostgreSQL binding has no identity")
		}
		s.BindingID = c.BindingID
		s.BindingReleased = false
		s.CredentialGeneration = c.CredentialGeneration
		return true, nil
	}
	c, e = p.options.Bindings.Resolve(ctx, s.BindingID)
	if e != nil {
		return false, fmt.Errorf("OpenBao PostgreSQL binding is unavailable")
	}
	if s.CredentialGeneration != c.CredentialGeneration {
		s.CredentialGeneration = c.CredentialGeneration
		return true, nil
	}
	return false, nil
}

func (p *Provider) memberPostgresStorage(ctx context.Context, rc *engine.Context, s *internalState) (baospec.Storage, error) {
	storage := baospec.Storage{Type: baospec.StoragePostgreSQL}
	if p.options.Bindings == nil || s.BindingID == "" {
		return storage, fmt.Errorf("OpenBao PostgreSQL binding is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	c, e := p.options.Bindings.Resolve(ctx, s.BindingID)
	if e != nil {
		return storage, fmt.Errorf("OpenBao PostgreSQL binding is unavailable")
	}
	if c.Host == "" || c.Username == "" || c.Port < 1 || c.Port > 65535 || !x509.NewCertPool().AppendCertsFromPEM(c.TLS.CAPEM) {
		return storage, fmt.Errorf("OpenBao PostgreSQL binding requires a verified TLS endpoint")
	}
	password, e := p.options.Bindings.Password(ctx, c.PasswordSecretRef)
	if e != nil {
		return storage, fmt.Errorf("OpenBao PostgreSQL credential is unavailable")
	}
	u := url.URL{Scheme: "postgresql", Host: net.JoinHostPort(c.Host, strconv.Itoa(c.Port)), Path: "/openbao", User: url.UserPassword(c.Username, password), RawQuery: "sslmode=verify-full"}
	// OpenBao 2.7.1 creates both tables itself (skip_create_table defaults false).
	// The binding role owns the database and therefore can create these tables.
	storage.PostgreSQL = &baospec.PostgreSQLStorage{ConnectionURL: u.String(), Table: "openbao_kv_store", HATable: "openbao_ha_locks", CAPEM: string(c.TLS.CAPEM)}
	return storage, nil
}
