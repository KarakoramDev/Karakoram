// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

const pluginDirectory = "/opt/karakoram/openbao-plugins"

// memberPlugins is shared by installation and rendering: every declared command
// must be installed before the agent starts OpenBao.
func memberPlugins(o Options) []baospec.Plugin {
	plugins := []baospec.Plugin{{Type: "database", Name: "mongodb-database-plugin", Command: "openbao-plugin-database-mongodb_linux_amd64_v1", Version: "v0.0.1"}}
	if o.Seal.Type == "awskms" {
		plugins = append(plugins, baospec.Plugin{Type: "kms", Name: "awskms", Command: "openbao-plugin-kms-aws_linux_amd64_v1", Version: "v0.1.0"})
	}
	return plugins
}

func pluginPrepare(o Options, owner string) ([]string, error) {
	var commands []string
	for _, plugin := range memberPlugins(o) {
		name := "openbao-plugin-database-mongodb"
		if plugin.Type == "kms" {
			name = "openbao-plugin-kms-aws"
		}
		a, err := nativeartifacts.Get(name)
		if err != nil {
			return nil, err
		}
		url, err := nativeartifacts.ResolveURL(a)
		if err != nil {
			return nil, err
		}
		commands = append(commands, nativeartifacts.PrepareCommands(a, url, pluginDirectory, owner)...)
	}
	return commands, nil
}
