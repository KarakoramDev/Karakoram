// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"log/slog"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type engineOptions struct {
	ControlPlaneURL, InternalCAPEM string
	Store                          *store.Store
	Logger                         *slog.Logger
}

func engineProviders(o engineOptions) []engine.Provider {
	return []engine.Provider{
		psmdb.New(psmdb.Options{ControlPlaneURL: o.ControlPlaneURL, InternalCAPEM: o.InternalCAPEM, Store: o.Store, Logger: o.Logger}),
	}
}
