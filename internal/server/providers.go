// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"log/slog"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/engine/pxc"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type engineOptions struct {
	ControlPlaneURL, InternalCAPEM string
	Store                          *store.Store
	Logger                         *slog.Logger
	Targets                        func(context.Context, domain.ClusterRef) (openbao.Target, error)
	Bindings                       pgbinding.Service
}

func engineProviders(o engineOptions) []engine.Provider {
	// A nil manager inside the service interface must retain nil admission semantics.
	if bindings, ok := o.Bindings.(*pgbinding.Manager); ok && bindings == nil {
		o.Bindings = nil
	}
	providers := []engine.Provider{
		psmdb.New(psmdb.Options{ControlPlaneURL: o.ControlPlaneURL, InternalCAPEM: o.InternalCAPEM, Store: o.Store, Logger: o.Logger}),
		rabbitmq.New(rabbitmq.Options{ControlPlaneURL: o.ControlPlaneURL, InternalCAPEM: o.InternalCAPEM, Store: o.Store, Logger: o.Logger}),
		openbao.New(openbao.ProviderOptions{ControlPlaneURL: o.ControlPlaneURL, InternalCAPEM: o.InternalCAPEM, Store: o.Store, Logger: o.Logger, Targets: o.Targets, Bindings: o.Bindings}),
		elastic.New(elastic.ProviderOptions{ControlPlaneURL: o.ControlPlaneURL, InternalCAPEM: o.InternalCAPEM, Store: o.Store, Logger: o.Logger}),
		postgres.New(postgres.Options{ControlPlaneURL: o.ControlPlaneURL, InternalCAPEM: o.InternalCAPEM, Store: o.Store, Logger: o.Logger}),
		mysql.New(mysql.Options{ControlPlaneURL: o.ControlPlaneURL, InternalCAPEM: o.InternalCAPEM, Store: o.Store, Logger: o.Logger}),
		pxc.New(mysql.Options{ControlPlaneURL: o.ControlPlaneURL, InternalCAPEM: o.InternalCAPEM, Store: o.Store, Logger: o.Logger}),
		supabase.New(postgres.Options{ControlPlaneURL: o.ControlPlaneURL, InternalCAPEM: o.InternalCAPEM, Store: o.Store, Logger: o.Logger}),
		temporal.New(temporal.Options{ControlPlaneURL: o.ControlPlaneURL, InternalCAPEM: o.InternalCAPEM, Store: o.Store, Logger: o.Logger, Bindings: o.Bindings}),
	}
	if o.Bindings != nil {
		providers = append(providers, keycloak.New(keycloak.Options{ControlPlaneURL: o.ControlPlaneURL, InternalCAPEM: o.InternalCAPEM, Bindings: o.Bindings}))
	}
	return providers
}

func (s *Server) bindingSource(ctx context.Context, ref domain.ClusterRef) (*engine.Context, pgbinding.Access, error) {
	rc, provider, err := s.context(ctx, ref)
	if err != nil {
		return nil, nil, err
	}
	if err := s.providerContext(rc); err != nil {
		return nil, nil, engine.ErrUnavailable
	}
	access, ok := provider.(pgbinding.Access)
	if !ok {
		return nil, nil, engine.ErrNotSupported
	}
	return rc, access, nil
}

func (s *Server) bindingOptions() pgbinding.Options {
	return pgbinding.Options{Store: s.Store, Source: s.bindingSource, Trigger: func(ref domain.ClusterRef) {
		if s.Trigger != nil {
			s.Trigger.Trigger(ref)
		}
	}}
}
