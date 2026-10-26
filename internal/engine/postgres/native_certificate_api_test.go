// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres_test

import (
	"net/http"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/events"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/rbac"
	"github.com/KarakoramDev/Karakoram/internal/server"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestNativeCertificateReceiptPreservesPublishedRevision(t *testing.T) {
	postgres.CheckNativeCertificateReceiptPublishedRevision(t, func(p *postgres.Core, rc *engine.Context) http.Handler {
		t.Helper()
		registry, err := engine.NewRegistry(p)
		if err != nil {
			t.Fatal(err)
		}
		// This handler authenticates persisted member tokens, not public API
		// sessions. Its public Auth/RBAC/event services are not exercised.
		api, err := server.New(server.Options{Store: rc.Store.(*store.Store), Registry: registry, Context: *rc,
			Auth: &auth.Service{}, RBAC: &rbac.Enforcer{}, Hub: &events.Hub{}, Backends: &infra.Registry{}})
		if err != nil {
			t.Fatal(err)
		}
		return api.InternalHandler()
	})
}
