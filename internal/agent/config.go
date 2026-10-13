// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"gopkg.in/yaml.v3"
)

// RenderConfig renders a MongoDB configuration, rejecting overrides of agent-owned keys.
func RenderConfig(c agentapi.MemberConfig, dataDir string) ([]byte, error) {
	dir := filepath.Join(dataDir, "karakoram")
	port := c.Port
	if port == 0 {
		port = 27017
	}
	netConfig := map[string]any{"port": port, "bindIpAll": true, "ipv6": true}
	mode := c.TLS.Mode
	if mode == "" {
		mode = "disabled"
	}
	netConfig["tls"] = map[string]any{"mode": mode}
	if mode != "disabled" {
		netConfig["tls"] = map[string]any{"mode": mode, "certificateKeyFile": filepath.Join(dir, "mongodb.pem"), "CAFile": filepath.Join(dir, "ca.pem"), "allowConnectionsWithoutCertificates": true}
	}
	cfg := map[string]any{"net": netConfig, "security": map[string]any{"keyFile": filepath.Join(dir, "keyfile"), "clusterAuthMode": "keyFile"}}
	if c.Keyfile == "" {
		return nil, errors.New("member keyfile is required")
	}
	switch c.Role {
	case agentapi.RoleRouter:
		if c.ConfigDB == "" {
			return nil, errors.New("router configDB is required")
		}
		cfg["sharding"] = map[string]any{"configDB": c.ConfigDB}
	case agentapi.RoleReplicaSet, agentapi.RoleConfigServer:
		if c.ReplSetName == "" {
			return nil, errors.New("replica set name is required")
		}
		cache := c.WiredTigerCacheSizeGB
		if cache <= 0 {
			cache = 0.25
		}
		cfg["storage"] = map[string]any{"dbPath": filepath.Join(dataDir, "db"), "wiredTiger": map[string]any{"engineConfig": map[string]any{"cacheSizeGB": cache}}}
		cfg["replication"] = map[string]any{"replSetName": c.ReplSetName}
		slow := c.SlowOpThresholdMS
		if slow == 0 {
			slow = 100
		}
		cfg["operationProfiling"] = map[string]any{"slowOpThresholdMs": slow}
		if c.Role == agentapi.RoleConfigServer {
			cfg["sharding"] = map[string]any{"clusterRole": "configsvr"}
		} else if c.ShardServer {
			cfg["sharding"] = map[string]any{"clusterRole": "shardsvr"}
		}
	default:
		return nil, fmt.Errorf("unknown member role %q", c.Role)
	}
	overrides, err := agentapi.ParseEngineConfig(c.EngineConfig)
	if err != nil {
		return nil, err
	}
	var merge func(map[string]any, map[string]any) error
	merge = func(dst, src map[string]any) error {
		for key, value := range src {
			if nested, ok := value.(map[string]any); ok {
				existing, ok := dst[key].(map[string]any)
				if !ok {
					existing = map[string]any{}
					dst[key] = existing
				}
				if err := merge(existing, nested); err != nil {
					return err
				}
			} else {
				dst[key] = value
			}
		}
		return nil
	}
	if err := merge(cfg, overrides); err != nil {
		return nil, err
	}
	return yaml.Marshal(cfg)
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".karakoram-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(mode); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func renderedFiles(c agentapi.MemberConfig, dir string) (map[string][]byte, error) {
	config, err := RenderConfig(c, dir)
	if err != nil {
		return nil, err
	}
	name := "mongod.conf"
	if c.Role == agentapi.RoleRouter {
		name = "mongos.conf"
	}
	files := map[string][]byte{name: config, "keyfile": []byte(c.Keyfile)}
	if c.TLS.Mode != "" && c.TLS.Mode != "disabled" {
		files["mongodb.pem"] = []byte(c.TLS.CertPEM + "\n" + c.TLS.KeyPEM)
		files["ca.pem"] = []byte(c.TLS.CAPEM)
	}
	return files, nil
}

func changedFiles(old, next map[string][]byte) []string {
	var changed []string
	for name, data := range next {
		if !bytes.Equal(old[name], data) {
			changed = append(changed, name)
		}
	}
	for name := range old {
		if _, ok := next[name]; !ok {
			changed = append(changed, name)
		}
	}
	sort.Strings(changed)
	return changed
}

func cacheConfig(c agentapi.MemberConfig, dir string) error {
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(dir, "karakoram", "config.json"), data, 0600)
}
