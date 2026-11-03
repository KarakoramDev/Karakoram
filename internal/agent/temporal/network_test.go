// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
)

type routeConnection struct {
	net.Conn
	address net.Addr
	closed  bool
}

func (c *routeConnection) LocalAddr() net.Addr { return c.address }
func (c *routeConnection) Close() error        { c.closed = true; return nil }

func TestMemberAddressPrivateFamily(t *testing.T) {
	for _, tc := range []struct {
		name, ip    string
		ipv6, valid bool
	}{
		{"IPv4", "10.0.0.7", false, true},
		{"IPv6", "fd00::7", true, true},
		{"IPv4-mapped", "::ffff:10.0.0.7", false, true},
		{"wrong-family-v4", "10.0.0.7", true, false},
		{"wrong-family-v6", "fd00::7", false, false},
		{"mapped-not-v6", "::ffff:10.0.0.7", true, false},
		{"public-v4", "192.0.2.7", false, false},
		{"public-v6", "2001:db8::7", true, false},
		{"loopback", "::1", true, false},
		{"unspecified", "::", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &routeConnection{address: &net.UDPAddr{IP: net.ParseIP(tc.ip)}}
			address, err := memberAddress(t.Context(), &tspec.Database{Host: "writer.example.test", Port: 5432}, tc.ipv6, func(_ context.Context, network, remote string) (net.Conn, error) {
				expected := "udp4"
				if tc.ipv6 {
					expected = "udp6"
				}
				if network != expected || remote != "writer.example.test:5432" {
					t.Fatalf("route discovery: %s %s", network, remote)
				}
				return conn, nil
			})
			if (err == nil) != tc.valid || !conn.closed {
				t.Fatalf("address=%q error=%v closed=%v", address, err, conn.closed)
			}
			if tc.valid && address != net.ParseIP(tc.ip).String() {
				t.Fatalf("address=%q", address)
			}
		})
	}
}

func TestMemberAddressFailureSanitization(t *testing.T) {
	_, err := memberAddress(t.Context(), &tspec.Database{Host: "private-writer", Port: 5432}, true, func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("private-route-detail")
	})
	if err == nil || strings.Contains(err.Error(), "private-route-detail") {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestLegacyTemporalSpecRemainsIPv4(t *testing.T) {
	var spec tspec.Spec
	if err := json.Unmarshal([]byte(`{"component":"server"}`), &spec); err != nil {
		t.Fatal(err)
	}
	if spec.IPv6 || loopback(spec.IPv6) != "127.0.0.1" {
		t.Fatal("legacy payload changed family")
	}
	if loopback(true) != "::1" {
		t.Fatal("IPv6 local management must not depend on dual stack sockets")
	}
}
