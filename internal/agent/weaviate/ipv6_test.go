// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/configtest"
	"net"
	"strings"
	"testing"
)

func TestIPv6NativeConfig(t *testing.T) {
	s := testSpec()
	s.NodeName = configtest.Member
	s.BindHost = "::"
	s.RaftJoin = []string{net.JoinHostPort(configtest.IPv6, "8301")}
	s.GossipJoin = []string{net.JoinHostPort(configtest.PeerIPv6, "7946")}
	ip, err := resolveAdvertiseAddr(t.Context(), s, func(_ context.Context, network, host string) ([]net.IP, error) {
		if network != "ip6" || host != configtest.Member {
			t.Fatal("wrong member discovery")
		}
		return []net.IP{net.ParseIP(configtest.IPv6)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	p, files := render(t.TempDir(), s, config(s), ip)
	configtest.CheckIPv6Rendered(t, files, p.Args, p.Env, nil)
	v := configtest.Settings(strings.Join(p.Env, "\n"), "=")
	configtest.CheckIPv6NativeConfig(t, configtest.NetworkConfig{Listeners: []string{v["CLUSTER_BIND_ADDR"]}, AdvertisedHosts: []string{v["CLUSTER_ADVERTISE_ADDR"]}, AdvertisedEndpoints: append(strings.Split(v["RAFT_JOIN"], ","), strings.Split(v["CLUSTER_JOIN"], ",")...)})
}

func TestIPv4NativeConfig(t *testing.T) {
	s := testSpec()
	s.NodeName, s.BindHost = configtest.Member, "0.0.0.0"
	s.RaftJoin = []string{net.JoinHostPort(configtest.IPv4, "8301")}
	s.GossipJoin = []string{net.JoinHostPort(configtest.PeerIPv4, "7946")}
	ip, err := resolveAdvertiseAddr(t.Context(), s, func(_ context.Context, network, host string) ([]net.IP, error) {
		if network != "ip4" || host != configtest.Member {
			t.Fatal("wrong IPv4 member discovery")
		}
		return []net.IP{net.ParseIP(configtest.IPv4)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := render(t.TempDir(), s, config(s), ip)
	v := configtest.Settings(strings.Join(p.Env, "\n"), "=")
	configtest.CheckIPv4NativeConfig(t, configtest.NetworkConfig{Listeners: []string{v["CLUSTER_BIND_ADDR"]}, AdvertisedHosts: []string{v["CLUSTER_ADVERTISE_ADDR"]}, AdvertisedEndpoints: append(strings.Split(v["RAFT_JOIN"], ","), strings.Split(v["CLUSTER_JOIN"], ",")...)})
}
