// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

var _ infra.PlatformSnapshotter = (*Backend)(nil)

func (b *Backend) PlatformSnapshot() *infra.TargetSnapshot { return b.c.snapshot }

func (c *client) setSnapshot(environmentID string) {
	c.snapshot = infra.PlatformSnapshot("dokploy|" + c.origin + "|" + environmentID)
	c.snapshot.SetListing(func(ctx context.Context) (string, error) {
		// Bypass both snapshot serving and the pass-local backend cache: this
		// request is the independent proof that cached platform facts still hold.
		var listing struct {
			Applications []struct {
				ID          string `json:"applicationId"`
				Name        string `json:"name"`
				AppName     string `json:"appName"`
				Status      string `json:"applicationStatus"`
				Description string `json:"description"`
				ServerID    string `json:"serverId"`
			} `json:"applications"`
		}
		if err := c.get(infra.WithoutSteadyObservation(ctx), "environment.one", map[string]string{"environmentId": environmentID}, &listing); err != nil {
			return "", err
		}
		// Keep ownership metadata, but not Dokploy's volatile timestamps or
		// counters. Application order is not meaningful in environment.one.
		rows := make([][6]string, 0, len(listing.Applications))
		for _, app := range listing.Applications {
			rows = append(rows, [6]string{app.ID, app.Name, app.AppName, app.Status, app.Description, app.ServerID})
		}
		slices.SortFunc(rows, func(a, b [6]string) int {
			for i := range a {
				if cmp := strings.Compare(a[i], b[i]); cmp != 0 {
					return cmp
				}
			}
			return 0
		})
		data, err := json.Marshal(rows)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:]), nil
	})
}
