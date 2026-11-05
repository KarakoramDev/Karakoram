// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

// Exercise actual checksum-verified preparation and java execution on a fresh
// filesystem. This tests the process dependency behind readiness, not a live DB.
// The directory must contain both locked Temurin JRE release archives.
func TestLegacyJREReplacementMissingReceiptRealPreparation(t *testing.T) {
	dir := os.Getenv("KARAKORAM_NEO4J_JRE_ARTIFACT_DIR")
	if dir == "" {
		t.Skip("KARAKORAM_NEO4J_JRE_ARTIFACT_DIR unset")
	}
	p, rc, b, a := setup(t)
	rc.Cluster.Spec.Version = "2026.10.0"
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	s.Member.JavaVersion = ""
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	refresh := func() {
		cfg, err := p.MemberConfig(t.Context(), rc, s.Member.Name)
		if err != nil {
			t.Fatal(err)
		}
		a.config = cfg
		if err := json.Unmarshal(cfg.EngineSpec, &a.spec); err != nil {
			t.Fatal(err)
		}
	}
	refresh()
	javaWorks := func() bool {
		home := strings.Replace(a.spec.Install.JavaHome, "/opt/karakoram", root, 1)
		return exec.Command(filepath.Join(home, "bin/java"), "-version").Run() == nil
	}
	if javaWorks() {
		t.Fatal("replacement unexpectedly contains legacy Java")
	}
	// The old member is healthy while gating deployment. Thereafter readiness
	// is conditional on launching the actual selected JRE on the replacement.
	replacement := false
	build := "old"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ready := !replacement || javaWorks()
		switch r.URL.Path {
		case "/v1/status":
			process := "stopped"
			if ready {
				process = "running"
			}
			json.NewEncoder(w).Encode(agentapi.Status{Agent: agentapi.AgentBuild{Build: build}, ConfigVersion: a.config.ConfigVersion, Processes: map[string]agentapi.ProcessStatus{"neo4j": {State: process}}})
		case "/v1/neo4j/readiness":
			json.NewEncoder(w).Encode(neo4jspec.Readiness{Ready: ready, Version: a.spec.Version, ConfigVersion: a.config.ConfigVersion, AppliedRestartToken: a.spec.RestartToken})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Agents = testAgents{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "file://"+dir)
	wrapped := &upgradeBackend{Backend: b}
	rc.Backend = wrapped
	op := upgradeOperation(false)
	gate, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = gate.Checkpoint
	if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	// Deliberately discard the returned deployment receipt.
	if len(wrapped.specs) != 1 {
		t.Fatal("deployment not submitted")
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "sudo"), []byte("#!/bin/sh\nif [ \"$1\" = '-u' ]; then shift 3; fi\nexec \"$@\"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(root, "temurin-jre-21")
	if err := os.MkdirAll(dest, 0755); err != nil {
		t.Fatal(err)
	}
	prepared := 0
	for _, command := range wrapped.specs[0].Prepare {
		if !strings.Contains(command, "OpenJDK21U-jre_") {
			continue
		}
		command = strings.ReplaceAll(command, "/opt/karakoram", root)
		cmd := exec.Command("sh", "-c", command)
		cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("prepare: %v\n%s", err, output)
		}
		prepared++
	}
	replacement = true
	build = engine.CurrentAgentBuild().Build
	if prepared != 2 || !javaWorks() {
		t.Fatal("replacement cannot launch cached legacy Java", prepared)
	}
	done := false
	for range 6 {
		refresh()
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			done = true
			break
		}
	}
	refresh()
	if !done || len(wrapped.specs) != 1 || !strings.HasSuffix(a.spec.Install.JavaHome, "/jdk-21.0.13+9-jre") || !javaWorks() {
		t.Fatalf("lost-receipt recovery failed: done=%v deploys=%d home=%s", done, len(wrapped.specs), a.spec.Install.JavaHome)
	}
}
