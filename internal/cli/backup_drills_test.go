// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

const drillReport = `{"id":"drill-one","cluster":"demo","namespace":"project","phase":"Succeeded","backup":"nightly-one","trigger":"manual","createdAt":"2026-10-21T00:00:00Z","restoreDurationSeconds":12,"dataAgeSeconds":300,"checks":[{"name":"restore","passed":true,"message":"inventory confirmed"}],"limitations":"definitions only, no messages","cleanup":{"phase":"Done","orphanedVolumes":["volume-one"]},"error":"example error"}`

func TestBackupDrillCommands(t *testing.T) {
	for _, tc := range []struct {
		args           []string
		method, suffix string
		body           map[string]any
	}{
		{[]string{"list", "demo", "--limit", "2", "--cursor", "next"}, "GET", "", nil},
		{[]string{"get", "demo", "drill-one"}, "GET", "/drill-one", nil},
		{[]string{"run", "demo"}, "POST", "", map[string]any{}},
		{[]string{"run", "demo", "--backup", "nightly-one"}, "POST", "", map[string]any{"backup": "nightly-one"}},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != tc.method || r.URL.Path != "/api/v1/clusters/demo/backup-drills"+tc.suffix || r.URL.Query().Get("namespace") != "project" {
					t.Errorf("request %s %s", r.Method, r.URL)
				}
				if tc.args[0] == "list" && (r.URL.Query().Get("limit") != "2" || r.URL.Query().Get("cursor") != "next") {
					t.Errorf("pagination %s", r.URL)
				}
				if tc.body != nil {
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if !reflect.DeepEqual(body, tc.body) {
						t.Errorf("body %#v want %#v", body, tc.body)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.method == "POST" {
					w.WriteHeader(http.StatusAccepted)
				}
				if tc.args[0] == "list" {
					fmt.Fprintf(w, `{"items":[%s]}`, drillReport)
				} else {
					fmt.Fprint(w, drillReport)
				}
			}))
			defer server.Close()
			args := append([]string{"-n", "project", "backup-drills"}, tc.args...)
			code, out, stderr := runCLI(t, server.URL, "", false, args...)
			if code != 0 || calls != 1 {
				t.Fatalf("code=%d calls=%d out=%q err=%q", code, calls, out, stderr)
			}
			for _, text := range []string{"drill-one", "Succeeded", "nightly-one", "manual", "12", "300", "2026-10-21"} {
				if !strings.Contains(out, text) {
					t.Errorf("missing %q: %s", text, out)
				}
			}
			if tc.args[0] == "get" {
				for _, text := range []string{"inventory confirmed", "passed=true", "definitions only, no messages", "Done", "volume-one", "example error"} {
					if !strings.Contains(out, text) {
						t.Errorf("missing %q: %s", text, out)
					}
				}
			}
		})
	}
}

func TestBackupDrillWait(t *testing.T) {
	for _, phase := range []string{"Succeeded", "Failed", "Pending"} {
		t.Run(phase, func(t *testing.T) {
			posts, gets := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == "POST" {
					posts++
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprint(w, `{"id":"drill-one","phase":"Pending"}`)
					return
				}
				gets++
				if r.URL.Path != "/api/v1/clusters/demo/backup-drills/drill-one" || r.URL.Query().Get("namespace") != "project" {
					t.Errorf("request %s", r.URL)
				}
				fmt.Fprintf(w, `{"id":"drill-one","phase":%q,"error":"verification rejected"}`, phase)
			}))
			defer server.Close()
			code, out, stderr := runCLI(t, server.URL, "", false, "-n", "project", "backup-drills", "run", "demo", "--wait", "--timeout", "50ms")
			if posts != 1 || gets < 1 {
				t.Fatalf("posts=%d gets=%d", posts, gets)
			}
			if phase == "Succeeded" {
				if code != 0 || !strings.Contains(out, phase) {
					t.Fatalf("code=%d out=%q err=%q", code, out, stderr)
				}
			} else if code == 0 {
				t.Fatalf("expected failure: out=%q err=%q", out, stderr)
			}
			if phase == "Pending" && (!strings.Contains(stderr, "work continues server-side") || !strings.Contains(stderr, `backup-drills get "demo" "drill-one" --wait`)) {
				t.Fatalf("missing resume: %s", stderr)
			}
			if phase == "Failed" && (!strings.Contains(out, "Failed") || !strings.Contains(stderr, "verification rejected")) {
				t.Fatalf("missing failure: out=%q err=%q", out, stderr)
			}
		})
	}
}

func TestBackupDrillPolicyFlags(t *testing.T) {
	for _, command := range [][]string{{"clusters", "create", "demo"}, {"clusters", "patch", "demo"}} {
		t.Run(command[1], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				want := map[string]any{"enabled": true, "schedule": "0 4 * * 0", "timeout": "2h", "backupSchedule": "nightly"}
				if !reflect.DeepEqual(nested(body, "spec", "backup", "drill"), want) {
					t.Errorf("body %#v", body)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"name":"demo"}`)
			}))
			defer server.Close()
			args := append(command, "--drill-enabled", "--drill-schedule", "0 4 * * 0", "--drill-timeout", "2h", "--drill-backup-schedule", "nightly")
			code, out, stderr := runCLI(t, server.URL, "", false, args...)
			if code != 0 {
				t.Fatalf("code=%d out=%q err=%q", code, out, stderr)
			}
		})
	}
}

func TestBackupDrillStructuredOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, drillReport)
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "backup-drills", "get", "demo", "drill-one", "-o", "json")
	var report map[string]any
	if code != 0 || json.Unmarshal([]byte(out), &report) != nil || nested(report, "cleanup", "phase") != "Done" {
		t.Fatalf("code=%d out=%q err=%q", code, out, stderr)
	}
}

func TestBackupDrillResumeWait(t *testing.T) {
	gets := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gets++
		if r.Method != "GET" {
			t.Errorf("resume must not mutate: %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		if gets == 2 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"message":"temporarily unavailable"}`)
			return
		}
		phase := "Pending"
		if gets > 2 {
			phase = "Succeeded"
		}
		fmt.Fprintf(w, `{"id":"drill-one","phase":%q}`, phase)
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "backup-drills", "get", "demo", "drill-one", "--wait", "--timeout", "2s")
	if code != 0 || gets != 3 || !strings.Contains(out, "Succeeded") || !strings.Contains(stderr, "Observation temporarily unavailable") {
		t.Fatalf("code=%d gets=%d out=%q err=%q", code, gets, out, stderr)
	}
}
