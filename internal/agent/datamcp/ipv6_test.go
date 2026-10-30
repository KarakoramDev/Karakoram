// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package datamcp

import (
	"github.com/KarakoramDev/Karakoram/internal/configtest"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"gopkg.in/yaml.v3"
	"strings"
	"testing"
)

func TestIPv6NativeConfig(t *testing.T) {
	for _, upstream := range []string{companionspec.UpstreamMongoDB, companionspec.UpstreamPostgres} {
		t.Run(upstream, func(t *testing.T) {
			p := companionspec.Process{Level: "read", Port: 9000, URI: "mongodb://[" + configtest.IPv6 + "]:27017/test", Hosts: []string{"[" + configtest.IPv6 + "]:5432"}, Username: "user", Password: "secret", Database: "test"}
			s := companionspec.Spec{Upstream: upstream, Version: "1.0.0", InstallDir: "/opt/datamcp", Processes: []companionspec.Process{p}}
			process, files, err := Render(s, p, t.TempDir(), "local-token")
			if err != nil {
				t.Fatal(err)
			}
			c := configtest.NetworkConfig{AdvertisedEndpoints: []string{}}
			if upstream == companionspec.UpstreamMongoDB {
				v := configtest.Settings(strings.Join(process.Env, "\n"), "=")
				c.LocalOnlyListeners = []string{v["MDB_MCP_HTTP_HOST"] + ":" + v["MDB_MCP_HTTP_PORT"]}
				c.AdvertisedEndpoints = []string{v["MDB_MCP_CONNECTION_STRING"]}
			} else {
				var v struct {
					HTTP struct{ Address string }
				}
				for path, data := range files {
					if strings.HasSuffix(path, "config.yaml") {
						if err := yaml.Unmarshal(data, &v); err != nil {
							t.Fatal(err)
						}
					}
				}
				c.LocalOnlyListeners = []string{v.HTTP.Address}
				c.AdvertisedEndpoints = p.Hosts
				found := false
				for _, data := range files {
					if strings.Contains(string(data), configtest.IPv6) {
						found = true
					}
				}
				if !found {
					t.Fatal("PostgreSQL companion lost IPv6 database host")
				}
			}
			configtest.CheckIPv6NativeConfig(t, c)
		})
	}
}

func TestIPv4NativeConfig(t *testing.T) {
	for _, upstream := range []string{companionspec.UpstreamMongoDB, companionspec.UpstreamPostgres} {
		t.Run(upstream, func(t *testing.T) {
			p := companionspec.Process{Level: "read", Port: 9000, URI: "mongodb://" + configtest.IPv4 + ":27017/test", Hosts: []string{configtest.IPv4 + ":5432"}, Username: "user", Password: "secret", Database: "test"}
			s := companionspec.Spec{Upstream: upstream, Version: "1.0.0", InstallDir: "/opt/datamcp", Processes: []companionspec.Process{p}}
			process, files, err := Render(s, p, t.TempDir(), "local-token")
			if err != nil {
				t.Fatal(err)
			}
			c := configtest.NetworkConfig{}
			if upstream == companionspec.UpstreamMongoDB {
				v := configtest.Settings(strings.Join(process.Env, "\n"), "=")
				c.LocalOnlyListeners, c.AdvertisedEndpoints = []string{v["MDB_MCP_HTTP_HOST"] + ":" + v["MDB_MCP_HTTP_PORT"]}, []string{v["MDB_MCP_CONNECTION_STRING"]}
			} else {
				var v struct{ HTTP struct{ Address string } }
				for path, data := range files {
					if strings.HasSuffix(path, "config.yaml") {
						if err := yaml.Unmarshal(data, &v); err != nil {
							t.Fatal(err)
						}
						if !strings.Contains(string(data), configtest.IPv4) {
							t.Fatal("PostgreSQL companion lost IPv4 database host")
						}
					}
				}
				c.LocalOnlyListeners, c.AdvertisedEndpoints = []string{v.HTTP.Address}, p.Hosts
			}
			configtest.CheckIPv4NativeConfig(t, c)
		})
	}
}
