// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/version"
)

func (a *app) printVersion(ctx context.Context) error {
	result := map[string]any{
		"client": map[string]any{
			"version":   version.VersionLabel(),
			"commit":    version.CommitLabel(),
			"buildDate": version.BuildDate,
		},
	}
	if a.format == "table" {
		if err := a.print(result); err != nil {
			return err
		}
		delete(result, "client")
	}
	if a.client == nil {
		result["serverStatus"] = "not configured"
	} else {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		server, err := a.request(ctx, "getVersion", nil, nil, nil)
		if err != nil {
			result["serverStatus"] = "unreachable"
			result["serverError"] = err.Error()
		} else {
			result["server"] = server
		}
	}
	return a.print(result)
}
