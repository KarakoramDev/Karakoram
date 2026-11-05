// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestMaintenanceCatalog(t *testing.T) {
	p := New(Options{})
	var spec domain.ClusterSpec
	p.ApplyDefaults(&spec)
	if spec.Version != "8.0.34-15" {
		t.Fatal(spec.Version)
	}
	info := p.Describe()
	for _, pair := range [][2]string{{"7.0.40-22", "7.0.45-24"}, {"7.0.43-23", "7.0.45-24"}, {"8.0.29-13", "8.0.34-15"}, {"8.0.32-14", "8.0.34-15"}, {"7.0.45-24", "8.0.34-15"}} {
		found := false
		for _, path := range info.UpgradePaths {
			if path.From == pair[0] && path.To == pair[1] {
				found = true
				if major(pair[0]) != major(pair[1]) && path.RequiresFCV != "7.0" {
					t.Fatal(path)
				}
			}
		}
		if !found {
			t.Fatalf("missing path %v", pair)
		}
	}
	for _, version := range info.Versions {
		if version.Default != (version.Version == spec.Version) {
			t.Fatal(version)
		}
		tools := version.Version + ".noble"
		if version.Version == "7.0.45-24" || version.Version == "8.0.34-15" {
			tools = "100.19.1-2.noble"
		}
		commands := strings.Join(PrepareCommands(version.Version), "\n")
		for _, want := range []string{"percona-server-mongodb-server=" + version.Version + ".noble", "percona-server-mongodb-mongos=" + version.Version + ".noble", "percona-server-mongodb-tools=" + tools} {
			if !strings.Contains(commands, want) {
				t.Fatalf("%s missing %s", version.Version, want)
			}
		}
	}
}

func TestShellPackagePins(t *testing.T) {
	for _, version := range catalog {
		commands := strings.Join(PrepareCommands(version.Version), "\n")
		if version.Major == "6.0" {
			if !strings.Contains(commands, "percona-mongodb-mongosh=2.8.3.noble") || strings.Contains(commands, "mongosh-2.14.1-linux-x64") {
				t.Fatal("legacy shell changed", commands)
			}
			continue
		}
		for _, want := range []string{"mongosh-2.14.1-linux-x64.tgz", "8d24940ebe3675959b518fd41ea16b4e2182d6a75c09777db09cfa64e18834a1", "percona-mongodb-mongosh=2.10.0.noble", "dpkg-divert --local --rename --divert /usr/bin/mongosh.percona --add /usr/bin/mongosh", "ln -sfn /opt/karakoram/mongosh-2.14.1/mongosh-2.14.1-linux-x64/bin/mongosh /usr/bin/mongosh"} {
			if !strings.Contains(commands, want) {
				t.Fatalf("%s missing %s", version.Version, want)
			}
		}
		if strings.Index(commands, "sha256sum -c -") > strings.Index(commands, "sudo dpkg-divert") {
			t.Fatal("shell installation precedes integrity check")
		}
	}
}

func TestShellPackageHonorsArtifactMirror(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.test/native")
	commands := strings.Join(PrepareCommands("8.0.34-15"), "\n")
	if !strings.Contains(commands, "https://artifacts.example.test/native/mongosh-2.14.1-linux-x64.tgz") || strings.Contains(commands, "downloads.mongodb.com") {
		t.Fatal("shell download bypasses native artifact mirror", commands)
	}
}
