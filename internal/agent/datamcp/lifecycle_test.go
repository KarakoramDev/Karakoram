package datamcp

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
)

func status(t *testing.T, a *integrationAgent) agentapi.Status {
	t.Helper()
	req, _ := http.NewRequest("GET", a.url+"/v1/status", nil)
	resp, err := a.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s agentapi.Status
	if err = json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	return s
}
func reload(t *testing.T, a *integrationAgent, s companionspec.Spec) {
	t.Helper()
	raw, _ := json.Marshal(s)
	a.mu.Lock()
	a.config.EngineSpec = raw
	a.config.ConfigVersion++
	a.mu.Unlock()
	req, _ := http.NewRequest("POST", a.url+"/v1/reload", nil)
	resp, err := a.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("reload %d %s", resp.StatusCode, b)
	}
}
func TestApplyLifecycle(t *testing.T) {
	e := fakeEndpoint(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":{}}`)
	}))
	s, p := fixture(companionspec.UpstreamMongoDB)
	p.Port = e.Port
	s.Processes = []companionspec.Process{p}
	s.InstallDir = t.TempDir()
	if err := os.MkdirAll(filepath.Join(s.InstallDir, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.InstallDir, "bin/start"), []byte("#!/bin/sh\nexec sleep 300\n"), 0700); err != nil {
		t.Fatal(err)
	}
	a := startAgent(t, s)
	first := status(t, a).Processes[companionspec.ProcessRead]
	if first.PID == 0 {
		t.Fatal(first)
	}
	secretPath := filepath.Join(a.dir, "karakoram/datamcp/mcp-ro/local-secret")
	secret, err := os.ReadFile(secretPath)
	if err != nil {
		t.Fatal(err)
	}
	reload(t, a, s)
	same := status(t, a).Processes[companionspec.ProcessRead]
	if same.PID != first.PID {
		t.Fatal("unchanged payload restarted process", first, same)
	}
	s.Processes[0].MaxTimeMS = 15000
	reload(t, a, s)
	changed := status(t, a).Processes[companionspec.ProcessRead]
	if changed.PID == 0 || changed.PID == first.PID {
		t.Fatal("changed payload did not restart process", changed)
	}
	current, err := os.ReadFile(secretPath)
	if err != nil || string(current) != string(secret) {
		t.Fatal("local secret changed across apply", err)
	}
	info, _ := os.Stat(secretPath)
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
	s.Processes = nil
	reload(t, a, s)
	stopped := status(t, a).Processes[companionspec.ProcessRead]
	if stopped.PID != 0 {
		t.Fatal("removed process still running", stopped)
	}
	req, _ := http.NewRequest("POST", a.url+"/v1/processes/mcp-ro/start", nil)
	resp, err := a.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == 200 {
		t.Fatal("explicit start accepted absent process")
	}
}
