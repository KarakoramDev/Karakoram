// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

const pluginDirectory = "/opt/karakoram/openbao-plugins"

const mongodbPluginVersion = "v0.0.2"
const mongodbPluginCommand = "openbao-plugin-database-mongodb_0.0.2"
const pluginRevision = "mongodb-0.0.1+0.0.2;awskms-0.1.1"

// memberPlugins renders the installed set recorded by a successful deployment
// receipt. An absent revision is legacy state, not proof of current artifacts.
func memberPlugins(o Options, installedRevision string) []baospec.Plugin {
	// Keep the real old binary registered for persisted connection pins and leases.
	// Removing it requires a separate migration of operator-created mounts.
	plugins := []baospec.Plugin{
		{Type: "database", Name: "mongodb-database-plugin", Command: "openbao-plugin-database-mongodb_linux_amd64_v1", Version: "v0.0.1"},
	}
	kmsVersion := "v0.1.0"
	if installedRevision == pluginRevision {
		plugins = append(plugins, baospec.Plugin{Type: "database", Name: "mongodb-database-plugin", Command: mongodbPluginCommand, Version: mongodbPluginVersion})
		kmsVersion = "v0.1.1"
	}
	if o.Seal.Type == "awskms" {
		plugins = append(plugins, baospec.Plugin{Type: "kms", Name: "awskms", Command: "openbao-plugin-kms-aws_linux_amd64_v1", Version: kmsVersion})
	}
	return plugins
}

func pluginPrepare(o Options, owner string) ([]string, error) {
	var commands []string
	for _, plugin := range memberPlugins(o, pluginRevision) {
		name := "openbao-plugin-database-mongodb"
		if plugin.Type == "kms" {
			name = "openbao-plugin-kms-aws"
		} else if plugin.Version == "v0.0.1" {
			name += "-0.0.1"
		}
		a, err := nativeartifacts.Get(name)
		if err != nil {
			return nil, err
		}
		url, err := nativeartifacts.ResolveURL(a)
		if err != nil {
			return nil, err
		}
		directory := pluginDirectory
		if plugin.Version == mongodbPluginVersion {
			directory += "/mongodb-0.0.2"
		}
		commands = append(commands, nativeartifacts.PrepareCommands(a, url, directory, owner)...)
		if plugin.Version == mongodbPluginVersion {
			// Non-OCI plugins must resolve directly inside plugin_directory.
			// Publish a real executable, not a symlink into the extraction directory.
			commands = append(commands, "sudo -u '"+strings.ReplaceAll(owner, "'", "'\"'\"'")+"' install -m 0755 "+directory+"/openbao-plugin-database-mongodb_linux_amd64_v1 "+pluginDirectory+"/"+mongodbPluginCommand)
		}
	}
	return commands, nil
}
