// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func storageIdentity(storage *domain.BackupStorage) string {
	return domain.NormalizedNamespace(storage.Namespace) + "/" + storage.Name
}
func storageRef(namespace, key string) (string, string) {
	ns, name, ok := strings.Cut(key, "/")
	if !ok {
		return namespace, key
	}
	return ns, name
}
func clusterBasePath(storage *domain.BackupStorage, c *domain.Cluster, s *state) string {
	return RepositoryBasePath(storage.Prefix, c.Ref().Namespace, c.Name, s.UID)
}
func (p *Provider) ensureRepository(ctx context.Context, rc *engine.Context, s *state, storage *domain.BackupStorage, readOnly bool, basePath string) (string, error) {
	key := storageIdentity(storage)
	name := RepositoryName(storage.Name)
	version := rc.Cluster.Spec.Version
	nodes := s.esNodes()
	if len(nodes) > 0 && nodes[0].Version != "" {
		version = nodes[0].Version
	}
	if readOnly {
		name += "-source-" + digest([]byte(key + basePath))[:8]
	}
	if basePath == "" {
		basePath = clusterBasePath(storage, rc.Cluster, s)
	}
	if !slices.Contains(s.Storages, key) {
		s.Storages = append(s.Storages, key)
		p.rollingStart(rc, s, "config", version, "")
		return name, errWaiting
	}
	if s.Rolling != nil {
		return name, errWaiting
	}
	for _, m := range nodes {
		desired, err := p.desiredSpec(ctx, rc, s, m, version)
		if err != nil {
			return name, err
		}
		if desired.RestartToken != m.ConfigHash {
			p.rollingStart(rc, s, "config", version, "")
			return name, errWaiting
		}
	}
	if changed, err := p.ensureSecureSettings(ctx, rc, s); changed || err != nil {
		if err != nil {
			return name, err
		}
		return name, errWaiting
	}
	client := S3ClientName(domain.NormalizedNamespace(storage.Namespace), storage.Name)
	settings := map[string]any{"bucket": storage.Bucket, "client": client, "base_path": basePath}
	if readOnly {
		settings["readonly"] = true
	}
	repo := esapi.Repository{Type: "s3", Settings: settings}
	b, err := json.Marshal(repo)
	if err != nil {
		return name, err
	}
	hash := digest(b)
	mapKey := key
	if readOnly {
		mapKey += "/source"
	}
	if previous := s.Repositories[mapKey]; previous != nil && previous.Hash == hash {
		return name, nil
	}
	c, err := p.es(ctx, rc, s)
	if err != nil {
		return name, err
	}
	defer c.Close()
	if err = c.PutRepository(ctx, name, repo, false); err != nil {
		return name, err
	}
	if _, err = c.VerifyRepository(ctx, name); err != nil {
		return name, err
	}
	s.Repositories[mapKey] = &repository{Name: name, Client: client, BasePath: basePath, ReadOnly: readOnly, Hash: hash}
	return name, nil
}
func configuredStorages(spec domain.ClusterSpec) []string {
	out := []string{}
	if spec.Backup.Storage != "" {
		out = append(out, spec.Backup.Storage)
	}
	for _, schedule := range spec.Backup.Schedules {
		if !schedule.Enabled {
			continue
		}
		name := schedule.Storage
		if name == "" {
			name = spec.Backup.Storage
		}
		if name != "" && !slices.Contains(out, name) {
			out = append(out, name)
		}
	}
	return out
}

// ensureSecureSettings waits for updated keystores before reloading credentials.
func (p *Provider) ensureSecureSettings(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	_, secure, err := p.staticSettings(ctx, rc, s)
	if err != nil {
		return false, err
	}
	b, err := json.Marshal(secure)
	if err != nil {
		return false, err
	}
	hash := digest(b)
	if s.SecureSettingsHash == "" {
		s.SecureSettingsHash = hash
		return false, nil
	}
	if s.SecureSettingsHash == hash {
		return false, nil
	}
	if !s.SecureSettingsReload {
		s.SecureSettingsReload = true
		p.bumpConfig(s)
		if err = save(rc.Cluster, s); err != nil {
			return true, err
		}
		for _, m := range s.esNodes() {
			p.reloadMembers(ctx, rc, m.Name)
		}
		return true, nil
	}
	for _, m := range s.esNodes() {
		if !p.componentReady(ctx, rc, m) {
			return true, nil
		}
	}
	c, err := p.es(ctx, rc, s)
	if err != nil {
		return true, err
	}
	defer c.Close()
	if err = c.ReloadSecureSettings(ctx); err != nil {
		return true, err
	}
	s.SecureSettingsHash = hash
	s.SecureSettingsReload = false
	return true, nil
}
