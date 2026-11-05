// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
)

// migrateMongoDBConnection changes only a managed connection's version pin.
// Both real plugin versions remain registered: existing leases and unmanaged
// mounts must remain usable throughout member replacement and after restart.
func (p *Provider) migrateMongoDBConnection(ctx context.Context, rc *engine.Context, c DatabaseConnection) (bool, error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return false, err
	}
	for _, m := range s.Members {
		if m.PluginRevision != pluginRevision || m.ResumeDeploy != nil || m.Change != nil || !m.Healthy {
			return false, nil
		}
	}
	client, err := p.connectionClient(ctx, rc)
	if err != nil {
		return false, err
	}
	// Storage registration alone is not sufficient during a mixed-member rollout;
	// the durable per-member deployment receipts above prove binary availability.
	if err = client.Do(ctx, "GET", "sys/plugins/catalog/database/mongodb-database-plugin?version="+mongodbPluginVersion, nil, nil); err != nil {
		if baoapi.IsStatus(err, http.StatusNotFound) {
			return false, nil
		}
		return false, err
	}
	var config struct {
		Data struct {
			PluginName    string `json:"plugin_name"`
			PluginVersion string `json:"plugin_version"`
		} `json:"data"`
	}
	path := c.MountPath + "/config/" + c.Name
	if err = client.Do(ctx, "GET", path, nil, &config); err != nil {
		return false, err
	}
	if config.Data.PluginVersion == mongodbPluginVersion {
		return false, nil
	}
	if config.Data.PluginName != "mongodb-database-plugin" {
		return false, engine.Invalid("managed MongoDB connection uses an unexpected plugin")
	}
	// OpenBao merges this partial write into the stored connection, preserving
	// its rotated password, roles and lease state. Never replay bootstrap secrets.
	err = client.Do(ctx, "POST", path, map[string]any{"plugin_version": mongodbPluginVersion, "verify_connection": true}, nil)
	return err == nil, err
}
