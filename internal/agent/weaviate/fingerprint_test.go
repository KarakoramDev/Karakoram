// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
)

func TestRestartFingerprintComponents(t *testing.T) {
	base := testSpec()
	c := config(base)
	p, files := render("/data", base, c)
	old := fingerprint(base, p, files)
	cases := map[string]func(*wvspec.MemberSpec, *agent.ProcessSpec, map[string][]byte){
		"version": func(s *wvspec.MemberSpec, _ *agent.ProcessSpec, _ map[string][]byte) { s.Version = "1.39.12" },
		"binary":  func(_ *wvspec.MemberSpec, p *agent.ProcessSpec, _ map[string][]byte) { p.Binary += "-new" },
		"args": func(_ *wvspec.MemberSpec, p *agent.ProcessSpec, _ map[string][]byte) {
			p.Args = append(p.Args, "--new")
		},
		"environment": func(_ *wvspec.MemberSpec, p *agent.ProcessSpec, _ map[string][]byte) {
			p.Env = append(p.Env, "CHANGED=true")
		},
		"certificate": func(_ *wvspec.MemberSpec, _ *agent.ProcessSpec, f map[string][]byte) {
			f["tls/weaviate.crt"] = []byte("renewed")
		},
		"token": func(s *wvspec.MemberSpec, _ *agent.ProcessSpec, _ map[string][]byte) { s.RestartToken = "new" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := base
			p, files := render("/data", s, c)
			mutate(&s, &p, files)
			if old == fingerprint(s, p, files) {
				t.Fatal("restart input ignored")
			}
		})
	}
	c.ConfigVersion++
	next, files := render("/data", base, c)
	if old != fingerprint(base, next, files) {
		t.Fatal("config epoch alone restarts native process")
	}
}
