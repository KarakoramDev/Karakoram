// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"os"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/dokploy"
	"github.com/KarakoramDev/Karakoram/internal/infra/zerops"
)

func backendRegistry(c Config) (*infra.Registry, error) {
	return infra.NewRegistry(zerops.NewFactory(zerops.Options{BaseURL: c.ZeropsAPIURL}), dokploy.NewFactory())
}

func (c Config) defaultTarget(registry *infra.Registry) (infra.Target, error) {
	kind := c.Backend
	if kind == "" {
		kind = "zerops"
	}
	factory, err := registry.Get(kind)
	if err != nil {
		return infra.Target{}, err
	}
	return factory.TargetFromEnv(func(key string) string {
		switch key {
		case "KARAKORAM_ZEROPS_TOKEN":
			return c.ZeropsToken
		case "KARAKORAM_ZEROPS_PROJECT_ID", "projectId":
			return c.ZeropsProjectID
		default:
			return os.Getenv(key)
		}
	})
}
