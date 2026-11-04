// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"context"
	"errors"
	"net"
	"slices"
	"testing"
)

func TestPrivateMemberAdvertisement(t *testing.T) {
	for _, backend := range []struct{ name, bind, network, address string }{
		{"zerops", "::", "ip6", "fd00::12"},
		{"nomad", "0.0.0.0", "ip4", "10.0.0.12"},
		{"dokploy", "0.0.0.0", "ip4", "172.20.0.12"},
	} {
		for _, replicas := range []int{1, 3} {
			t.Run(backend.name+string(rune('0'+replicas)), func(t *testing.T) {
				s := testSpec()
				s.BindHost, s.BootstrapExpect = backend.bind, replicas
				s.RaftJoin, s.GossipJoin = nil, nil
				for i := range replicas {
					name := "node0" + string(rune('0'+i))
					s.RaftJoin = append(s.RaftJoin, net.JoinHostPort(name, "8301"))
					s.GossipJoin = append(s.GossipJoin, net.JoinHostPort(name, "7946"))
				}
				address, err := resolveAdvertiseAddr(t.Context(), s, func(_ context.Context, network, host string) ([]net.IP, error) {
					if host != s.NodeName || network != backend.network {
						t.Fatalf("lookup %s %s", network, host)
					}
					// Ignore another family's result and an unspecified address.
					return []net.IP{net.ParseIP("::"), net.ParseIP("0.0.0.0"), net.ParseIP(backend.address)}, nil
				})
				if err != nil || address != backend.address {
					t.Fatalf("address=%q err=%v", address, err)
				}
				p, _ := render(t.TempDir(), s, config(s), address)
				for _, want := range []string{"CLUSTER_ADVERTISE_ADDR=" + backend.address, "CLUSTER_BIND_ADDR=" + backend.bind, "CLUSTER_HOSTNAME=" + s.NodeName, "CLUSTER_ADVERTISE_PORT=27946"} {
					if !slices.Contains(p.Env, want) {
						t.Fatalf("missing %q", want)
					}
				}
				// The bare IPv6 env value becomes a bracketed Raft endpoint upstream.
				endpoint := net.JoinHostPort(address, "8300")
				host, port, err := net.SplitHostPort(endpoint)
				if err != nil || host != address || port != "8300" {
					t.Fatalf("invalid raft endpoint %q", endpoint)
				}
			})
		}
	}
}

func TestPrivateAdvertisementLookupFailure(t *testing.T) {
	s := testSpec()
	s.BindHost = "::"
	for _, lookup := range []func(context.Context, string, string) ([]net.IP, error){
		func(context.Context, string, string) ([]net.IP, error) { return nil, errors.New("DNS unavailable") },
		func(context.Context, string, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("::"), net.ParseIP("127.0.0.1"), net.ParseIP("fe80::1"), net.ParseIP("10.0.0.1")}, nil
		},
	} {
		if address, err := resolveAdvertiseAddr(t.Context(), s, lookup); err == nil || address != "" {
			t.Fatal("unusable advertisement accepted")
		}
	}
}

func TestConcreteBindAdvertisement(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "::1", "10.0.0.12", "fd00::12"} {
		s := testSpec()
		s.BindHost = address
		got, err := resolveAdvertiseAddr(t.Context(), s, func(context.Context, string, string) ([]net.IP, error) {
			t.Fatal("concrete bind must not use DNS")
			return nil, nil
		})
		if err != nil || got != address {
			t.Fatalf("address=%q err=%v", got, err)
		}
	}
}
