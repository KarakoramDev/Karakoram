// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestRecoveryMongoRestartFailureDoesNotAcknowledge(t *testing.T) {
	a, c := recoveryAgent(t)
	binary := buildFake(t)
	dir := t.TempDir()
	if err := os.Symlink(binary, filepath.Join(dir, "mongod")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	c.Maintenance = false
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	states, _ := a.sup.snapshot()
	pid := states["mongod"].PID
	defer func() { syscall.Kill(pid, syscall.SIGCONT); a.sup.shutdown(context.Background()) }()
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	awaitState(t, a.sup, func(p agentapi.ProcessStatus) bool { return p.State == "paused" })
	c.ConfigVersion++
	c.Keyfile = strings.Repeat("b", 40)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := a.apply(ctx, c); err == nil {
		t.Fatal("failed restart acknowledged")
	}
	if a.config().ConfigVersion == c.ConfigVersion {
		t.Fatal("unapplied version published")
	}
	if err := syscall.Kill(pid, syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	awaitState(t, a.sup, func(p agentapi.ProcessStatus) bool { return p.PID != 0 && p.PID != pid })
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if a.config().ConfigVersion != c.ConfigVersion {
		t.Fatal("retry not acknowledged")
	}
}

func TestRecoveryPipeFailureKeepsRetrying(t *testing.T) {
	for _, failure := range []int{1, 2} {
		t.Run(string(rune('0'+failure)), func(t *testing.T) {
			s := newSupervisor(io.Discard)
			s.backoff = 10 * time.Millisecond
			s.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
			calls := 0
			s.pipe = func() (*os.File, *os.File, error) {
				calls++
				if calls == failure {
					return nil, nil, syscall.EMFILE
				}
				return os.Pipe()
			}
			if err := s.action(context.Background(), "mongod", "start"); !errors.Is(err, syscall.EMFILE) {
				t.Fatal(err)
			}
			defer s.shutdown(context.Background())
			states, _ := s.snapshot()
			if states["mongod"].State != "failed" || states["mongod"].Error == "" {
				t.Fatal("pipe failure not visible")
			}
			awaitState(t, s, func(p agentapi.ProcessStatus) bool { return p.PID != 0 && p.Restarts > 0 })
		})
	}
}

func TestRecoverySurvivingOwnerIsFenced(t *testing.T) {
	dir := t.TempDir()
	first := newSupervisor(io.Discard)
	first.ownershipDir = dir
	first.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	if err := first.action(context.Background(), "mongod", "start"); err != nil {
		t.Fatal(err)
	}
	defer first.shutdown(context.Background())
	second := newSupervisor(io.Discard)
	second.ownershipDir = dir
	second.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	defer second.shutdown(context.Background())
	if err := second.action(context.Background(), "mongod", "start"); err == nil || !strings.Contains(err.Error(), "unmanaged process conflict") {
		t.Fatal("surviving owner not fenced", err)
	}
}

func TestRecoveryAmbiguousLaunchIsFenced(t *testing.T) {
	s := newSupervisor(io.Discard)
	s.ownershipDir = t.TempDir()
	if err := s.writeProcessOwner("mongod", processOwner{Pending: true}); err != nil {
		t.Fatal(err)
	}
	s.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	defer s.shutdown(context.Background())
	if err := s.action(context.Background(), "mongod", "start"); err == nil || !strings.Contains(err.Error(), "outcome unknown") {
		t.Fatal("ambiguous launch not fenced", err)
	}
}

func TestRecoveryAmbiguousLaunchAfterReboot(t *testing.T) {
	current, _, err := processIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	previousBoot := []byte(current.Boot)
	if previousBoot[0] == '0' {
		previousBoot[0] = '1'
	} else {
		previousBoot[0] = '0'
	}
	for _, tc := range []struct {
		name, boot string
		recover    bool
	}{
		{"previous-boot", string(previousBoot), true},
		{"current-boot", current.Boot, false},
		{"legacy", "", false},
		{"corrupt-boot", "invalid", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSupervisor(io.Discard)
			s.ownershipDir = t.TempDir()
			if err := s.writeProcessOwner("mongod", processOwner{Pending: true, Boot: tc.boot}); err != nil {
				t.Fatal(err)
			}
			s.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
			defer s.shutdown(context.Background())
			err := s.action(context.Background(), "mongod", "start")
			if tc.recover {
				if err != nil {
					t.Fatalf("a prior kernel boot cannot retain a surviving process: %v", err)
				}
				states, _ := s.snapshot()
				if states["mongod"].PID == 0 {
					t.Fatal("new supervisor did not launch the process")
				}
			} else if err == nil || !strings.Contains(err.Error(), "outcome unknown") {
				t.Fatalf("ambiguous launch without reboot evidence was not fenced: %v", err)
			}
		})
	}
}

func TestRecoveryLaunchIntentRecordsKernelBoot(t *testing.T) {
	s := newSupervisor(io.Discard)
	s.ownershipDir = t.TempDir()
	if err := s.recordProcessIntent("patroni"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(s.ownershipDir, "patroni.json"))
	if err != nil {
		t.Fatal(err)
	}
	var owner processOwner
	if err := json.Unmarshal(data, &owner); err != nil {
		t.Fatal(err)
	}
	current, _, err := processIdentity(os.Getpid())
	if err != nil || !owner.Pending || owner.Boot != current.Boot || owner.PID != 0 {
		t.Fatal("interrupted launches cannot be tied to their kernel boot", err)
	}
}

func TestRecoveryStorageFailureKeepsAuthenticatedStatus(t *testing.T) {
	a, c := recoveryAgent(t)
	a.o.Token = "token"
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(a.o.DataDir, "karakoram", "config.json")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	c.ConfigVersion++
	if _, err := a.apply(context.Background(), c); err == nil {
		t.Fatal("storage fault ignored")
	}
	response := requestNode(a, "GET", "/v1/status", "", "token")
	var status agentapi.Status
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 || status.Error == "" || status.ConfigVersion == c.ConfigVersion || a.cert == nil {
		t.Fatal("storage diagnostic unavailable", response.Code, status)
	}
}

func TestRecoveryCorruptControlsKeepStatusAndFence(t *testing.T) {
	a, c := recoveryAgent(t)
	a.o.Token = "token"
	if err := os.MkdirAll(filepath.Join(a.o.DataDir, "karakoram"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.o.DataDir, "karakoram", "controls.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	a.controls, a.controlsErr = loadControls(a.o.DataDir)
	if _, err := a.apply(context.Background(), c); err == nil {
		t.Fatal("corrupt controls accepted")
	}
	if a.cert == nil {
		t.Fatal("transport unavailable during safety fault")
	}
	response := requestNode(a, "GET", "/v1/status", "", "token")
	if response.Code != 200 || !strings.Contains(response.Body.String(), "controls corrupt") {
		t.Fatal("safety diagnostic unavailable", response.Body.String())
	}
	if _, err := os.Stat(filepath.Join(a.o.DataDir, "db")); !os.IsNotExist(err) {
		t.Fatal("safety failure mutated database directory", err)
	}
}

func TestRecoveryCorruptCachePullsAuthoritativeConfig(t *testing.T) {
	a, c := recoveryAgent(t)
	if err := a.saveControls(localControls{}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.o.DataDir, "karakoram", "config.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	pulled := make(chan struct{}, 4)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case pulled <- struct{}{}:
		default:
		}
		json.NewEncoder(w).Encode(c)
	}))
	defer server.Close()
	o := a.o
	o.ControlPlaneURL = server.URL
	o.InternalCAPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	o.Token = "token"
	o.Listen = "127.0.0.1:0"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, o) }()
	select {
	case <-pulled:
	case err := <-done:
		cancel()
		t.Fatal("startup exited before CP repair", err)
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("startup never pulled authoritative config")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		data, err := os.ReadFile(filepath.Join(o.DataDir, "karakoram", "config.json"))
		var repaired agentapi.MemberConfig
		if err == nil && json.Unmarshal(data, &repaired) == nil && repaired.ConfigVersion == c.ConfigVersion {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("authoritative configuration did not repair the cache")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startup did not stop")
	}
}

func TestRecoveryCanceledMaintenanceDoesNotReleaseHold(t *testing.T) {
	a, _ := recoveryAgent(t)
	if err := a.maintenance(true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.maintenanceContext(ctx, false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	controls, err := loadControls(a.o.DataDir)
	if err != nil || !controls.Maintenance {
		t.Fatal("canceled release persisted", controls, err)
	}
}

func TestRecoveryTransportRenewsDespiteEngineFailure(t *testing.T) {
	a, c, e := fakeNode(t, "/bin/sleep")
	e.onApply = func(*Node, agentapi.MemberConfig) error { return errors.New("engine storage unavailable") }
	if _, err := a.apply(context.Background(), c); err == nil {
		t.Fatal("engine error lost")
	}
	if a.cert == nil || a.config().ConfigVersion != 0 {
		t.Fatal("transport coupled to runtime recovery")
	}
}

func TestRecoveryIdenticalFilesAvoidPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	data := []byte(`{"configuration":"unchanged"}`)
	if err := atomicWrite(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	original := time.Unix(12345, 0)
	if err := os.Chtimes(path, original, original); err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(path)
	if err != nil || !stat.ModTime().Equal(original) {
		t.Fatal("identical file required republishing", stat, err)
	}
}

func TestRecoveryRestartUnknownReceiptFailsClosed(t *testing.T) {
	a, _ := recoveryAgent(t)
	request := agentapi.ProcessAction{Process: "mongod", Action: "restart", RequestID: "uncertain-restart"}
	digest := sha256.Sum256([]byte(request.RequestID))
	path := filepath.Join(a.o.DataDir, "karakoram", "restarts", hex.EncodeToString(digest[:])+".json")
	data, err := json.Marshal(restartReceipt{Request: request})
	if err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.processActionRequest(context.Background(), request); err == nil || !strings.Contains(err.Error(), "outcome unknown") {
		t.Fatal("ambiguous restart replayed", err)
	}
}
