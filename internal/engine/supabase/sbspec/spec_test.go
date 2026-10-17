// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package sbspec

import (
	"strings"
	"testing"
)

func validSpec() Spec {
	return Spec{Component: "auth", Version: "2.197.0", InstallDir: "/opt/karakoram/supabase-auth-2.197.0", RestartToken: "revision", Topology: "single", DataPeers: []string{"data.example"}, Database: "postgres", JWTSecret: strings.Repeat("x", 32), AnonKey: "anon", ServiceRoleKey: "service"}
}
func TestValidate(t *testing.T) {
	if err := validSpec().Validate(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*Spec){"component": func(s *Spec) { s.Component = "unknown" }, "escape": func(s *Spec) { s.InstallDir = "/tmp/auth-2.197.0" }, "version": func(s *Spec) { s.Version = "../x" }, "peers": func(s *Spec) { s.DataPeers = nil }, "extra single peer": func(s *Spec) { s.DataPeers = append(s.DataPeers, "other") }, "host injection": func(s *Spec) { s.DataPeers = []string{"host/name"} }, "HA trust": func(s *Spec) { s.Topology = "ha" }, "JWT": func(s *Spec) { s.JWTSecret = "short" }, "function escape": func(s *Spec) { s.Functions = map[string]map[string]string{"hello": {"../../secret": "x"}} }, "reserved main": func(s *Spec) { s.Functions = map[string]map[string]string{"main": {"index.ts": "x"}} }, "sink": func(s *Spec) { s.VectorSinkURL = "file:///secret" }}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			s := validSpec()
			change(&s)
			if s.Validate() == nil {
				t.Fatal("invalid spec accepted")
			}
		})
	}
}
