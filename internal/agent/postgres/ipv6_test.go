// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"github.com/KarakoramDev/Karakoram/internal/configtest"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"gopkg.in/yaml.v3"
	"net"
	"strings"
	"testing"
)

func TestIPv6NativeConfig(t *testing.T) {
	for _, topology := range []string{pgspec.TopologySingle, pgspec.TopologyHA, pgspec.TopologyDistributed} {
		t.Run(topology, func(t *testing.T) {
			s := testSpec()
			s.Topology = topology
			s.Member = configtest.Member
			s.ListenHost = "::"
			s.Peers = []pgspec.Peer{{Name: configtest.Member, Host: configtest.IPv6}, {Name: configtest.Peer, Host: configtest.PeerIPv6}}
			if topology == pgspec.TopologyDistributed {
				s.Citus = &pgspec.Citus{Group: 0, Database: "postgres"}
			}
			p := dataPaths(t.TempDir())
			values := configtest.Settings(renderPostgres(s, p), "=")
			configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{strings.Trim(values["listen_addresses"], "'")}})
			text, err := renderPatroni(s, p)
			if err != nil {
				t.Fatal(err)
			}
			var v struct {
				Rest struct {
					Listen  string `yaml:"listen"`
					Connect string `yaml:"connect_address"`
				} `yaml:"restapi"`
				PG struct {
					Listen  string `yaml:"listen"`
					Connect string `yaml:"connect_address"`
				} `yaml:"postgresql"`
				Etcd struct {
					Hosts []string `yaml:"hosts"`
				} `yaml:"etcd3"`
			}
			if err := yaml.Unmarshal([]byte(text), &v); err != nil {
				t.Fatal(err)
			}
			configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{v.Rest.Listen, v.PG.Listen}, AdvertisedEndpoints: append([]string{v.Rest.Connect, v.PG.Connect}, v.Etcd.Hosts...)})
			s.Etcd = &pgspec.Etcd{InitialCluster: configtest.Member + "=https://" + net.JoinHostPort(configtest.IPv6, "2380"), State: "new", Token: "test"}
			listeners, endpoints := []string{}, []string{}
			for _, arg := range etcdProcess(s, p).Args {
				k, value, ok := strings.Cut(arg, "=")
				if !ok {
					continue
				}
				switch k {
				case "--listen-client-urls", "--listen-peer-urls":
					listeners = append(listeners, value)
				case "--advertise-client-urls", "--initial-advertise-peer-urls":
					endpoints = append(endpoints, value)
				}
			}
			configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: listeners, AdvertisedEndpoints: endpoints})
			// ProxySQL must have explicit IPv6 writer and reader sockets; IPv4's
			// companion sockets are intentional because its IPv6 sockets are v6-only.
			interfaces := proxyInterfaces("::", true)
			for _, want := range []string{"[::]:5432", "[::]:5433"} {
				if !strings.Contains(interfaces, want) {
					t.Fatal("missing IPv6 proxy socket", interfaces)
				}
			}
			configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{"[::]:5432", "[::]:5433"}})
		})
	}
}

func TestIPv4NativeConfig(t *testing.T) {
	for _, topology := range []string{pgspec.TopologySingle, pgspec.TopologyHA, pgspec.TopologyDistributed} {
		t.Run(topology, func(t *testing.T) {
			s := testSpec()
			s.Topology, s.Member, s.ListenHost = topology, configtest.Member, "0.0.0.0"
			s.Peers = []pgspec.Peer{{Name: configtest.Member, Host: configtest.IPv4}, {Name: configtest.Peer, Host: configtest.PeerIPv4}}
			if topology == pgspec.TopologyDistributed {
				s.Citus = &pgspec.Citus{Database: "postgres"}
			}
			p := dataPaths(t.TempDir())
			v := configtest.Settings(renderPostgres(s, p), "=")
			configtest.CheckIPv4NativeConfig(t, configtest.NetworkConfig{Listeners: []string{strings.Trim(v["listen_addresses"], "'")}})
			text, err := renderPatroni(s, p)
			if err != nil {
				t.Fatal(err)
			}
			var patroni struct {
				Rest struct {
					Listen  string
					Connect string `yaml:"connect_address"`
				} `yaml:"restapi"`
				PG struct {
					Listen  string
					Connect string `yaml:"connect_address"`
				} `yaml:"postgresql"`
			}
			if err := yaml.Unmarshal([]byte(text), &patroni); err != nil {
				t.Fatal(err)
			}
			configtest.CheckIPv4NativeConfig(t, configtest.NetworkConfig{Listeners: []string{patroni.Rest.Listen, patroni.PG.Listen}, AdvertisedEndpoints: []string{patroni.Rest.Connect, patroni.PG.Connect}})
			s.Etcd = &pgspec.Etcd{State: "new", Token: "test"}
			c := configtest.NetworkConfig{}
			for _, arg := range etcdProcess(s, p).Args {
				k, value, _ := strings.Cut(arg, "=")
				switch k {
				case "--listen-client-urls", "--listen-peer-urls":
					c.Listeners = append(c.Listeners, value)
				case "--advertise-client-urls", "--initial-advertise-peer-urls":
					c.AdvertisedEndpoints = append(c.AdvertisedEndpoints, value)
				}
			}
			configtest.CheckIPv4NativeConfig(t, c)
			interfaces := proxyInterfaces(s.ListenHost, true)
			if strings.Contains(interfaces, "[::]") {
				t.Fatal("IPv4 proxy requires IPv6")
			}
			configtest.CheckIPv4NativeConfig(t, configtest.NetworkConfig{Listeners: strings.Split(interfaces, ";")})
		})
	}
}
