//go:build !integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"github.com/KarakoramDev/Karakoram/internal/configtest"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"net"
	"strconv"
	"strings"
	"testing"
)

func TestIPv6NativeConfig(t *testing.T) {
	for _, s := range []myspec.Spec{coreTestSpec(), testGroupSpec(), testGaleraSpec()} {
		if s.GroupReplication != nil {
			s.Topology = myspec.TopologyGroup
		}
		t.Run(s.Topology, func(t *testing.T) {
			s.Member = configtest.Member
			s.ListenHost = "::"
			s.Peers = []myspec.Peer{{Name: configtest.Member, Host: configtest.IPv6, Port: 3306}, {Name: configtest.Peer, Host: configtest.PeerIPv6, Port: 3306}}
			p := dataPaths(t.TempDir())
			v := configtest.Settings(renderConfig(s, p), "=")
			configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{strings.Trim(v["bind_address"], `"`)}, AdvertisedHosts: []string{strings.Trim(v["report_host"], `"`)}})
			if s.GroupReplication != nil {
				s.GroupReplication.Seeds = s.Peers
				options := RenderGroupOptions(s, p.root)
				peers := strings.Split(options["loose-group_replication_group_seeds"], ",")
				peers = append(peers, options["loose-group_replication_local_address"])
				configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{"*"}, AdvertisedEndpoints: peers})
			}
			if s.Galera != nil {
				s.Galera.ListenHost = "::"
				s.Galera.Peers = s.Peers
				for i := range s.Galera.Peers {
					s.Galera.Peers[i].Port = s.Galera.GaleraPort
				}
				o := RenderGaleraOptions(s, p.root)
				provider := configtest.Settings(strings.ReplaceAll(o["wsrep_provider_options"], ";", "\n"), "=")
				configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{provider["gmcast.listen_addr"]}, AdvertisedEndpoints: append(strings.Split(strings.TrimPrefix(o["wsrep_cluster_address"], "gcomm://"), ","), o["wsrep_node_address"], o["wsrep_node_incoming_address"], o["wsrep_sst_receive_address"], provider["ist.recv_addr"])})
			}
		})
	}
	s := testProxySpec()
	s.ListenHost = "::"
	s.Proxy.ReaderPort = true
	for i := range s.Peers {
		s.Peers[i].Host = configtest.IPv6
	}
	text, err := RenderProxyConfig(s, "/config", "/data")
	if err != nil {
		t.Fatal(err)
	}
	for _, port := range []int{3306, 3307} {
		endpoint := net.JoinHostPort("::", strconv.Itoa(port))
		if !strings.Contains(text, endpoint) {
			t.Fatal("missing IPv6 ProxySQL listener", endpoint)
		}
	}
	configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{"[::]:3306", "[::]:3307"}, AdvertisedHosts: []string{configtest.IPv6}})
}

func TestIPv4NativeConfig(t *testing.T) {
	for _, s := range []myspec.Spec{coreTestSpec(), testGroupSpec(), testGaleraSpec()} {
		if s.GroupReplication != nil {
			s.Topology = myspec.TopologyGroup
		}
		t.Run(s.Topology, func(t *testing.T) {
			s.Member, s.ListenHost = configtest.Member, "0.0.0.0"
			s.Peers = []myspec.Peer{{Name: configtest.Member, Host: configtest.IPv4, Port: 3306}, {Name: configtest.Peer, Host: configtest.PeerIPv4, Port: 3306}}
			if s.GroupReplication != nil {
				s.GroupReplication.Seeds = s.Peers
			}
			if s.Galera != nil {
				s.Galera.ListenHost, s.Galera.Peers = "0.0.0.0", s.Peers
			}
			p := dataPaths(t.TempDir())
			v := configtest.Settings(renderConfig(s, p), "=")
			c := configtest.NetworkConfig{Listeners: []string{strings.Trim(v["bind_address"], `"`)}, AdvertisedHosts: []string{strings.Trim(v["report_host"], `"`)}}
			if s.GroupReplication != nil {
				c.AdvertisedEndpoints = strings.Split(strings.Trim(v["loose-group_replication_group_seeds"], `"`), ",")
				c.AdvertisedEndpoints = append(c.AdvertisedEndpoints, strings.Trim(v["loose-group_replication_local_address"], `"`))
			}
			if s.Galera != nil {
				o := RenderGaleraOptions(s, p.root)
				provider := configtest.Settings(strings.ReplaceAll(o["wsrep_provider_options"], ";", "\n"), "=")
				c.Listeners = append(c.Listeners, provider["gmcast.listen_addr"])
				c.AdvertisedEndpoints = []string{o["wsrep_node_address"], o["wsrep_sst_receive_address"], provider["ist.recv_addr"]}
			}
			configtest.CheckIPv4NativeConfig(t, c)
		})
	}
	s := testProxySpec()
	s.ListenHost, s.Proxy.ReaderPort = "0.0.0.0", true
	text, err := RenderProxyConfig(s, "/config", "/data")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, `interfaces="0.0.0.0:3306;0.0.0.0:3307"`) || strings.Contains(text, "[::]") {
		t.Fatal("IPv4 ProxySQL listener requires IPv6")
	}
	configtest.CheckIPv4NativeConfig(t, configtest.NetworkConfig{Listeners: []string{"0.0.0.0:3306", "0.0.0.0:3307"}})
}
