// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"strings"
	"testing"
)

func TestUpgradeAllowed(t *testing.T) {
	for _, tt := range []struct {
		from, to   string
		ok, backup bool
	}{
		{"9.5.4", "9.5.5", true, false}, {"9.4.8", "9.5.4", true, false},
		{"9.5.5", "9.6.0", true, false}, {"8.19.22", "8.19.23", true, false},
		{"8.19.23", "9.1.0", true, true}, {"8.19.23", "9.5.5", true, true},
		{"8.19.23", "9.0.9", false, false}, {"8.18.9", "9.1.0", false, false},
		{"9.5.5", "8.19.23", false, false}, {"9.5.5", "9.5.4", false, false},
		{"9.5.5", "9.4.8", false, false}, {"9.5.5", "9.5.5", false, false},
		{"9.5.5", "10.0.0", false, false}, {"bad", "9.5.5", false, false},
		{"9.5.5", "9.6", false, false}, {"9.5.5", "9.6.0-SNAPSHOT", false, false},
		{"9.5.5", "09.6.0", false, false}, {"9.5.5", "9.6.99999999999999999999999", false, false},
	} {
		t.Run(tt.from+"_"+tt.to, func(t *testing.T) {
			ok, backup := UpgradeAllowed(tt.from, tt.to)
			if ok != tt.ok || backup != tt.backup {
				t.Fatalf("got (%v,%v), want (%v,%v)", ok, backup, tt.ok, tt.backup)
			}
		})
	}
}

func TestCatalog(t *testing.T) {
	want := []struct {
		version, major string
		def            bool
	}{{"9.5.5", "9.5", true}, {"9.5.4", "9.5", false}, {"9.4.8", "9.4", false}, {"8.19.23", "8.19", false}}
	if len(catalog) != len(want) {
		t.Fatal("catalog length")
	}
	for i, w := range want {
		t.Run(w.version, func(t *testing.T) {
			v := catalog[i]
			if v.Version != w.version || v.Major != w.major || v.Default != w.def || v.Status != "supported" {
				t.Fatalf("catalog entry: %+v", v)
			}
		})
	}
	paths := catalogUpgradePaths()
	if len(paths) != 6 {
		t.Fatalf("got %d upgrade paths", len(paths))
	}
	seen := map[string]bool{}
	for _, p := range paths {
		ok, backup := UpgradeAllowed(p.From, p.To)
		if !ok || backup != p.RequiresBackup {
			t.Fatalf("invalid path: %+v", p)
		}
		key := p.From + ">" + p.To
		if seen[key] {
			t.Fatal("duplicate path")
		}
		seen[key] = true
	}
	for _, from := range catalog {
		for _, to := range catalog {
			ok, _ := UpgradeAllowed(from.Version, to.Version)
			if seen[from.Version+">"+to.Version] != ok {
				t.Fatal("missing catalog path")
			}
		}
	}
}

func TestCatalogProvidesAPMAssets(t *testing.T) {
	// Since 8.15, Elasticsearch's apm-data plugin installs APM index templates,
	// ILM policies and ingest pipelines itself; standalone APM needs no Fleet
	// package installation, including when Kibana is disabled.
	for _, entry := range catalog {
		version, ok := parseVersion(entry.Version)
		if !ok || version[0] < 8 || version[0] == 8 && version[1] < 15 {
			t.Fatalf("catalog version %q does not provide APM assets", entry.Version)
		}
	}
}

func TestArtifacts(t *testing.T) {
	for _, component := range []string{"elasticsearch", "kibana", "logstash", "apm-server"} {
		for _, v := range catalog {
			t.Run(component+v.Version, func(t *testing.T) {
				file := component + "-" + v.Version + "-linux-x86_64.tar.gz"
				url := "https://artifacts.elastic.co/downloads/" + component + "/" + file
				if ArtifactURL(component, v.Version) != url {
					t.Fatal("artifact URL")
				}
				commands := prepareCommands(component, v.Version)
				if len(commands) != 1 {
					t.Fatal("command count")
				}
				for _, part := range []string{"curl -fsSLO " + url, "curl -fsSLO " + url + ".sha512", "sha512sum -c " + file + ".sha512", "sudo tar -xzf " + file + " -C /opt/elastic", "sudo chown -R zerops:zerops /opt/elastic", "rm -f " + file + " " + file + ".sha512"} {
					if !strings.Contains(commands[0], part) {
						t.Fatalf("missing %q", part)
					}
				}
			})
		}
	}
	for _, tt := range []struct{ component, version string }{{"unknown", "9.5.5"}, {"elasticsearch", "9.5.5;false"}} {
		if ArtifactURL(tt.component, tt.version) != "" || prepareCommands(tt.component, tt.version) != nil {
			t.Fatal("unsafe artifact input")
		}
	}
}
