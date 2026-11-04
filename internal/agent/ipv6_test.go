// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agent

import (
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/configtest"
	"gopkg.in/yaml.v3"
	"testing"
)

func TestIPv6NativeConfig(t *testing.T) {
	for _, role := range []string{agentapi.RoleReplicaSet, agentapi.RoleConfigServer, agentapi.RoleRouter} {
		t.Run(string(role), func(t *testing.T) {
			c := memberConfig(t)
			c.Role = role
			c.ConfigDB = "config/[" + configtest.IPv6 + "]:27017,[" + configtest.PeerIPv6 + "]:27017"
			data, err := RenderConfig(c, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			configtest.CheckIPv6Rendered(t, map[string][]byte{"mongod.yaml": data}, nil, nil, nil)
			var v struct {
				Net struct {
					BindAll bool `yaml:"bindIpAll"`
					IPv6    bool `yaml:"ipv6"`
				}
				Sharding struct {
					ConfigDB string `yaml:"configDB"`
				}
			}
			if err := yaml.Unmarshal(data, &v); err != nil {
				t.Fatal(err)
			}
			if !v.Net.BindAll || !v.Net.IPv6 {
				t.Fatal("MongoDB IPv6 listener disabled")
			}
			configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{"::"}, AdvertisedEndpoints: []string{"[" + configtest.IPv6 + "]:27017", "[" + configtest.PeerIPv6 + "]:27017"}})
			if role == agentapi.RoleRouter && v.Sharding.ConfigDB != c.ConfigDB {
				t.Fatal("mongos changed IPv6 config-server seed list")
			}
		})
	}
}

func TestIPv4NativeConfig(t *testing.T) {
	for _, role := range []string{agentapi.RoleReplicaSet, agentapi.RoleConfigServer, agentapi.RoleRouter} {
		t.Run(role, func(t *testing.T) {
			c := memberConfig(t)
			c.Role, c.ConfigDB = role, "config/"+configtest.IPv4+":27017"
			data, err := RenderConfig(c, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			var v struct {
				Net struct {
					BindAll bool `yaml:"bindIpAll"`
				}
				Sharding struct {
					ConfigDB string `yaml:"configDB"`
				}
			}
			if err := yaml.Unmarshal(data, &v); err != nil {
				t.Fatal(err)
			}
			if !v.Net.BindAll {
				t.Fatal("MongoDB no longer enables IPv4 interfaces")
			}
			if role == agentapi.RoleRouter && v.Sharding.ConfigDB != c.ConfigDB {
				t.Fatal("mongos lost IPv4 seeds")
			}
			configtest.CheckIPv4NativeConfig(t, configtest.NetworkConfig{Listeners: []string{"0.0.0.0"}, AdvertisedEndpoints: []string{configtest.IPv4 + ":27017"}})
		})
	}
}
