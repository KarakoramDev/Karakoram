// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

const nativeDiagnosticFixture = `AuthenticationFailed credentials:{SCRAM-SHA-256:{salt:"fixture-secret",storedKey:BinData(0,"fixture-secret")}} password:"fixture-secret" mongodb+srv://fixture-user:fixture-secret@host/db`

func TestSupervisorRedactsNativeOutputBeforeRing(t *testing.T) {
	for _, name := range []string{"mongod", "mongos", "pbm-agent", "other"} {
		s := newSupervisor(io.Discard)
		for _, line := range []string{nativeDiagnosticFixture, `AuthenticationFailed credentials:{salt:"fixture-secret"` + strings.Repeat("x", supervisorLogLineBytes)} {
			nested, _ := json.Marshal(map[string]string{"error": line})
			nested, _ = json.Marshal(string(nested))
			s.capture(name, "stderr", strings.NewReader(string(nested)+"\n"))
		}
		lines := s.logTail(name, 10)
		if len(lines) != 2 {
			t.Fatal("missing captured diagnostics")
		}
		for _, line := range lines {
			if strings.Contains(line.Message, "fixture-secret") || strings.Contains(line.Message, "fixture-user") || !strings.Contains(line.Message, "AuthenticationFailed") {
				t.Fatal("log ring exposed credentials or lost error class")
			}
		}
	}
}

func TestSupervisorRedactsNativeMultilineContinuations(t *testing.T) {
	for _, diagnostic := range []string{
		"secret-access-key: |\n  fixture-secret\n  more-fixture-secret\nAuthenticationFailed",
		"  secret-access-key: >-\n    fixture-secret\n  status: AuthenticationFailed",
		"password:\n  fixture-secret\nAuthenticationFailed",
		"password: \"prefix\nfixture-secret\\\"more-fixture-secret\nend\" | AuthenticationFailed",
		"credentials: {\n salt: \"fixture-secret\",\n storedKey: BinData(0,\"fixture-secret\")\n} | AuthenticationFailed",
		"password: \"prefix\nfixture-secret",
		"password: 'prefix''fixture-secret\nmore-fixture-secret' | AuthenticationFailed",
		"password: 'prefix\n''fixture-secret' | AuthenticationFailed",
		"AuthenticationFailed {\\u0022password\\u0022: \nfixture-secret",
	} {
		for _, name := range []string{"mongod", "pbm-agent"} {
			s := newSupervisor(io.Discard)
			s.capture(name, "stderr", strings.NewReader(diagnostic))
			lines := s.logTail(name, 100)
			if len(lines) == 0 {
				t.Fatal("lost diagnostic records")
			}
			var safe strings.Builder
			for _, line := range lines {
				safe.WriteString(line.Message)
				if strings.Contains(line.Message, "fixture-secret") {
					t.Fatal("multiline log ring exposed credentials")
				}
			}
			if strings.Contains(diagnostic, "AuthenticationFailed") && !strings.Contains(safe.String(), "AuthenticationFailed") {
				t.Fatal("multiline redaction lost error class after value ended")
			}
		}
	}
}

func TestSupervisorFailsClosedAfterDiscardedOpener(t *testing.T) {
	for _, opener := range []string{"secret-access-key: |", `password: "prefix`, "credentials: {", `"pass\u0077ord": "prefix`, `\u0022password\u0022: \u0022prefix`} {
		for _, offset := range []int{supervisorLogLineBytes - 5, supervisorLogLineBytes + 32, 2*supervisorLogLineBytes - 5} {
			s := newSupervisor(io.Discard)
			diagnostic := "AuthenticationFailed " + strings.Repeat("x", offset-len("AuthenticationFailed ")) + " " + opener + "\n  fixture-secret\nmore-fixture-secret"
			s.capture("pbm-agent", "stderr", strings.NewReader(diagnostic))
			lines := s.logTail("pbm-agent", 10)
			if len(lines) != 2 {
				t.Fatal("missing bounded log records")
			}
			for _, line := range lines {
				if len(line.Message) > supervisorLogLineBytes || strings.Contains(line.Message, "fixture-secret") {
					t.Fatal("discarded opener exposed a continuation or broke log bound")
				}
			}
			if !strings.Contains(lines[0].Message, "AuthenticationFailed") || lines[1].Message != "[2 lines omitted: possible credential material]" {
				t.Fatal("uncertain credential truncation did not fail closed")
			}
		}
	}
}

func TestSupervisorOmissionResumesAtNewRecord(t *testing.T) {
	for _, record := range []string{
		`{"t":{"$date":"2026-10-21T12:34:56Z"},"s":"I","msg":"progress"}`,
		"2026-10-21T12:34:56.000Z [INFO] progress",
		"[2026-10-21T12:34:56Z] progress",
		"2026-10-21 12:34:56 progress",
		"time=2026-10-21T12:34:56.000Z level=INFO msg=progress",
		`{"time":"2026-10-21T12:34:56Z","level":"INFO","msg":"progress"}`,
		"[pbm-agent stderr] progress",
	} {
		s := newSupervisor(io.Discard)
		s.capture("pbm-agent", "stderr", strings.NewReader("password: \\\"prefix\nfixture-secret\n"+record))
		lines := s.logTail("pbm-agent", 10)
		if len(lines) != 3 || lines[1].Message != "[1 lines omitted: possible credential material]" || lines[2].Message != record {
			t.Fatal("omission did not resume with a single gap marker at a new record")
		}
	}
}

func TestSupervisorOmissionHasHardLineAndByteCaps(t *testing.T) {
	for _, tc := range []struct {
		body string
		gap  string
	}{
		{strings.Repeat("fixture-secret\n", 64), "[64 lines omitted: possible credential material]"},
		{strings.Repeat("fixture-secret"+strings.Repeat("x", (16<<10)-len("fixture-secret")-1)+"\n", 4), "[4 lines omitted: possible credential material]"},
	} {
		s := newSupervisor(io.Discard)
		s.capture("pbm-agent", "stderr", strings.NewReader("password: \\\"prefix\n"+tc.body+"progress"))
		lines := s.logTail("pbm-agent", 100)
		if len(lines) != 3 || lines[1].Message != tc.gap || lines[2].Message != "progress" {
			t.Fatal("omission did not resume with a single gap marker at its hard cap")
		}
	}
}

func TestPBMResponseSanitizationPreservesStructuredOutput(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"status": "error", "error": nativeDiagnosticFixture, "replsets": []any{map[string]string{"name": "rs0"}}})
	response := agentapi.PBMResponse{Output: raw, Stderr: nativeDiagnosticFixture, ExitCode: 1}
	sanitizePBMResponse(&response)
	data, _ := json.Marshal(response)
	if strings.Contains(string(data), "fixture-secret") || strings.Contains(string(data), "fixture-user") {
		t.Fatal("PBM response exposed credentials")
	}
	var decoded struct {
		Status, Error string
		Replsets      []struct{ Name string }
	}
	if json.Unmarshal(response.Output, &decoded) != nil || decoded.Status != "error" || !strings.Contains(decoded.Error, "AuthenticationFailed") || len(decoded.Replsets) != 1 || decoded.Replsets[0].Name != "rs0" {
		t.Fatal("PBM output no longer parseable")
	}
	success := json.RawMessage(`{ "status":"done", "name":"restore", "ts":123 }`)
	response = agentapi.PBMResponse{Output: success}
	sanitizePBMResponse(&response)
	if string(response.Output) != string(success) {
		t.Fatal("modified successful structured output")
	}
}

func TestPBMReceiptReplayRedactsAndRewritesLegacyDiagnostic(t *testing.T) {
	a := &nodeAgent{o: Options{DataDir: t.TempDir()}}
	request := agentapi.PBMRequest{Command: "restore", Args: []string{"snapshot", "--yes"}, RequestID: "redaction-fixture"}
	id := sha256.Sum256([]byte(request.RequestID))
	path := filepath.Join(a.o.DataDir, "karakoram", "pbm-receipts", hex.EncodeToString(id[:])+".json")
	raw, _ := json.Marshal(request)
	hash := sha256.Sum256(raw)
	output, _ := json.Marshal(map[string]string{"status": "error", "error": nativeDiagnosticFixture})
	receipt := pbmReceipt{Hash: hex.EncodeToString(hash[:]), Response: &agentapi.PBMResponse{Output: output, Stderr: nativeDiagnosticFixture, ExitCode: 1}}
	data, _ := json.Marshal(receipt)
	if err := atomicWrite(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	response, err := a.pbm(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	returned, _ := json.Marshal(response)
	if strings.Contains(string(data), "fixture-secret") || strings.Contains(string(returned), "fixture-secret") || !strings.Contains(string(returned), "AuthenticationFailed") {
		t.Fatal("stored or returned receipt exposed credentials or lost class")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("receipt permissions changed")
	}
}

func TestPBMNewFailureReceiptRedactsNativeDiagnostics(t *testing.T) {
	bin := t.TempDir()
	script := "#!/bin/sh\nprintf '%s\\n' 'AuthenticationFailed password: fixture-secret' >&2\nprintf '%s\\n' '{\"status\":\"error\",\"error\":\"AuthenticationFailed token: fixture-secret\"}'\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "pbm"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	a := &nodeAgent{o: Options{DataDir: t.TempDir()}, c: memberConfig(t)}
	request := agentapi.PBMRequest{Command: "restore", Args: []string{"snapshot", "--yes"}, RequestID: "new-redaction-fixture"}
	response, err := a.pbm(context.Background(), request)
	if err != nil || response.ExitCode != 1 {
		t.Fatal("missing command failure", err)
	}
	id := sha256.Sum256([]byte(request.RequestID))
	path := filepath.Join(a.o.DataDir, "karakoram", "pbm-receipts", hex.EncodeToString(id[:])+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	returned, _ := json.Marshal(response)
	if strings.Contains(string(data), "fixture-secret") || strings.Contains(string(returned), "fixture-secret") || !strings.Contains(string(returned), "AuthenticationFailed") {
		t.Fatal("new receipt retained native secrets")
	}
	replayed, err := a.pbm(context.Background(), request)
	if err != nil || string(replayed.Output) != string(response.Output) || replayed.Stderr != response.Stderr {
		t.Fatal("sanitized replay changed response", err)
	}
}
