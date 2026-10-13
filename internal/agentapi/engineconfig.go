// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

var managedEngineKeys = [...]string{"net.port", "net.bindIp", "net.bindIpAll", "net.ipv6", "net.tls", "security.keyFile", "security.clusterAuthMode", "security.authorization", "security.transitionToAuth", "storage.dbPath", "storage.wiredTiger.engineConfig.cacheSizeGB", "replication.replSetName", "sharding.clusterRole", "sharding.configDB", "operationProfiling.slowOpThresholdMs", "processManagement", "systemLog.destination", "systemLog.path"}

// ParseEngineConfig is the shared provider/agent override policy. Parent scalar
// replacements and dotted YAML keys cannot bypass managed-key ownership.
func ParseEngineConfig(text string) (map[string]any, error) {
	var overrides map[string]any
	if err := yaml.Unmarshal([]byte(text), &overrides); err != nil {
		return nil, fmt.Errorf("engineConfig must be a YAML mapping: %w", err)
	}
	var validate func(map[string]any, string) error
	validate = func(values map[string]any, prefix string) error {
		for key, value := range values {
			path := prefix + key
			nested, mapping := value.(map[string]any)
			for _, owned := range managedEngineKeys {
				if path == owned || strings.HasPrefix(path, owned+".") || !mapping && strings.HasPrefix(owned, path+".") {
					return fmt.Errorf("engineConfig cannot override managed key %s", path)
				}
			}
			if mapping {
				if err := validate(nested, path+"."); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := validate(overrides, ""); err != nil {
		return nil, err
	}
	return overrides, nil
}
