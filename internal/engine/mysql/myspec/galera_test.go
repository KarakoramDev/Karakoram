// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import (
	"strings"
	"testing"
)

func TestGaleraValidation(t *testing.T) {
	good := Galera{ClusterName: "cluster-one", FormationEpoch: 1, Peers: []Peer{{Name: "db1", Host: "127.0.0.1", Port: 4567}}, GaleraPort: 4567, ISTPort: 4568, SSTPort: 4444, GCacheSize: "32M"}
	certs := validSpec()
	good.TrafficCAPEM = certs.CAPEM
	good.TrafficCert = certs.ServerCert
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Galera){func(g *Galera) { g.FormationEpoch = 0 }, func(g *Galera) { g.ClusterName = "bad;option" }, func(g *Galera) { g.SSTPort = g.ISTPort }, func(g *Galera) { g.GCacheSize = "32M;pc.bootstrap=YES" }, func(g *Galera) { g.StartPosition = "00000000-0000-0000-0000-000000000000:-1" }, func(g *Galera) { g.Peers = nil }, func(g *Galera) { g.TrafficCAPEM = "" }, func(g *Galera) { g.TrafficCert = nil }, func(g *Galera) { g.TrafficCert = &Certificate{CertPEM: certs.ServerCert.CertPEM} }, func(g *Galera) { g.TrafficCert = &Certificate{KeyPEM: certs.ServerCert.KeyPEM} }} {
		g := good
		mutate(&g)
		if g.Validate() == nil {
			t.Fatalf("accepted invalid Galera: %+v", g)
		}
	}
	for _, p := range []string{"826914cc-cd20-11f1-b4c4-6ac2787ad4af:18", "826914cc-cd20-11f1-b4c4-6ac2787ad4af:0"} {
		if _, _, err := ParseGaleraPosition(p); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{"826914cc-cd20-11f1-b4c4-6ac2787ad4af:-1", "garbage:3", "826914cc-cd20-11f1-b4c4-6ac2787ad4af:2:3"} {
		if _, _, err := ParseGaleraPosition(p); err == nil {
			t.Fatalf("accepted %q", p)
		}
	}
}
func TestGaleraTrafficEligibility(t *testing.T) {
	s := GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true}
	if !s.Eligible() {
		t.Fatal("healthy member fenced")
	}
	for _, mutate := range []func(*GaleraStatusResponse){func(s *GaleraStatusResponse) { s.ClusterStatus = "non-Primary" }, func(s *GaleraStatusResponse) { s.LocalStateComment = "Joining" }, func(s *GaleraStatusResponse) { s.Ready = false }, func(s *GaleraStatusResponse) { s.Connected = false }} {
		bad := s
		mutate(&bad)
		if bad.Eligible() {
			t.Fatalf("unhealthy member eligible %+v", bad)
		}
	}
}

func TestGaleraClusterNameProtocolBound(t *testing.T) {
	for _, name := range []string{"cluster-one", "9347c03140ab4a90b950ec3525ae4321", strings.Repeat("a", 32)} {
		if !ValidGaleraClusterName(name) {
			t.Fatalf("rejected protocol-safe cluster name %q", name)
		}
	}
	for _, name := range []string{"", strings.Repeat("a", 33), "9347c031-40ab-4a90-b950-ec3525ae4321", "bad;option", "cluster/name", "cluster name", "clustér"} {
		if ValidGaleraClusterName(name) {
			t.Fatalf("accepted unsafe or oversized gmcast name %q", name)
		}
	}
}
