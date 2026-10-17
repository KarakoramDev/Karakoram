// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package relay

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"
)

func TestInstanceTunnelRequiresLogicalPolicyAndFreshAuthorization(t *testing.T) {
	f := newFixture(t)
	inventory := map[string]bool{"192.0.2.10": true, "proxy.2.taskid": true}
	f.hub.o.AuthorizeInstance = func(_ context.Context, namespace, member, instance string) error {
		if namespace != "proj-test" || member != "proxy" || !inventory[instance] {
			return &ErrRefused{Reason: refusedPolicy}
		}
		return nil
	}
	f.hub.SetPolicy("proj-test", []string{"proxy:9443", "proxy:27017"})
	f.runRelay(f.o)
	f.connected()
	for _, instance := range []string{"192.0.2.10", "proxy.2.taskid"} {
		conn, err := f.hub.DialInstanceContext(t.Context(), "proj-test", "tcp", "proxy:9443", instance)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, 4)
		if _, err := io.ReadFull(conn, data); err != nil || string(data) != "ping" {
			t.Fatalf("pinned tunnel: %q %v", data, err)
		}
		conn.Close()
	}
	delete(inventory, "192.0.2.10")
	for _, tc := range []struct{ namespace, addr, instance string }{
		{"proj-test", "proxy:9443", "192.0.2.10"}, // removed since previous dial
		{"proj-test", "proxy:9443", "192.0.2.11"}, // unrelated runtime
		{"proj-test", "other:9443", "proxy.2.taskid"},
		{"other-project", "proxy:9443", "proxy.2.taskid"},
		{"proj-test", "proxy:27017", "proxy.2.taskid"},
		{"proj-test", "proxy:9443", "proxy"},
	} {
		conn, err := f.hub.DialInstanceContext(t.Context(), tc.namespace, "tcp", tc.addr, tc.instance)
		if conn != nil {
			conn.Close()
		}
		if err == nil {
			t.Fatalf("unauthorized instance accepted: %+v", tc)
		}
	}
	// A raw dotted/IP target remains forbidden, even if mistakenly allowlisted.
	f.hub.SetPolicy("proj-test", []string{"192.0.2.10:9443", "proxy.2.taskid:9443"})
	for _, addr := range []string{"192.0.2.10:9443", "proxy.2.taskid:9443"} {
		if conn, err := f.hub.DialContext(t.Context(), "proj-test", "tcp", addr); err == nil {
			conn.Close()
			t.Fatal("raw instance address accepted")
		}
	}
}

func TestInstanceTargetRetainsAddressDefenses(t *testing.T) {
	for _, instance := range []string{"127.0.0.1", "::1", "::ffff:127.0.0.1", "169.254.169.254", "fe80::1", "0.0.0.0", "224.0.0.1", "core", "relayhost", "proxy", "task/path", "task:9443", "blocked.task"} {
		t.Run(instance, func(t *testing.T) {
			dialed := false
			r := &runtime{o: defaults(Options{Hostname: "relayhost", Resolver: func(context.Context, string) ([]netip.Addr, error) {
				return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
			}, DialContext: func(context.Context, string, string) (net.Conn, error) {
				dialed = true
				return nil, errors.New("unexpected dial")
			}})}
			s := &relaySession{targets: map[string]struct{}{"proxy:9443": {}}}
			if _, err := r.target(t.Context(), s, "proxy:9443", instance); err == nil || dialed {
				t.Fatal("prohibited instance destination reached dialer", err)
			}
		})
	}
	f := newFixture(t)
	f.hub.SetPolicy("proj-test", []string{"proxy:9443"})
	if _, err := f.hub.DialInstanceContext(t.Context(), "proj-test", "tcp", "proxy:9443", "192.0.2.10"); err == nil {
		t.Fatal("missing inventory authorizer accepted")
	}
}
