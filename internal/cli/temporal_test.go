// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const temporalIssuedFixture = `{"serial":"serial-one","commonName":"worker","currentCa":true,"notAfter":"2026-11-01T00:00:00Z","frontendAddress":"frontend:7233","serverName":"temporal.internal","certificatePem":"CERT\n","privateKeyPem":"KEY\n","caPem":"CA\n"}`

func TestTemporalIssueCertificate(t *testing.T) {
	for _, destination := range []string{"files", "stdout"} {
		t.Run(destination, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != "POST" || r.URL.Path != "/api/v1/clusters/app/temporal/client-certificates" || r.URL.Query().Get("namespace") != "project" {
					t.Error(r.Method, r.URL)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if !reflect.DeepEqual(body, map[string]any{"commonName": "worker", "ttlHours": float64(721)}) {
					t.Error(body)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(201)
				fmt.Fprint(w, temporalIssuedFixture)
			}))
			defer server.Close()
			dir := filepath.Join(t.TempDir(), "certificates")
			args := []string{"--namespace", "project", "temporal", "client-certificate", "issue", "app", "--common-name", "worker", "--ttl", "720h31m"}
			if destination == "files" {
				args = append(args, "--out-dir", dir)
			} else {
				args = append(args, "--stdout")
			}
			out, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", args...)
			if err != nil || requests != 1 {
				t.Fatal(err, requests)
			}
			for _, value := range []string{"serial: serial-one", "notAfter: 2026-11-01", "frontendAddress: frontend:7233", "serverName: temporal.internal"} {
				if !strings.Contains(stderr, value) {
					t.Error(stderr)
				}
			}
			if destination == "stdout" {
				if out != "CERT\nKEY\nCA\n" {
					t.Fatal(out)
				}
			} else {
				if out != "" {
					t.Fatal(out)
				}
				info, err := os.Stat(dir)
				if err != nil || info.Mode().Perm() != 0700 {
					t.Fatal(info, err)
				}
				for i, name := range []string{"worker.crt", "worker.key", "ca.crt"} {
					path := filepath.Join(dir, name)
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					wantMode := os.FileMode(0600)
					if i == 2 {
						wantMode = 0644
					}
					if info.Mode().Perm() != wantMode {
						t.Errorf("%s: mode %o", name, info.Mode().Perm())
					}
					data, err := os.ReadFile(path)
					if err != nil || string(data) != []string{"CERT\n", "KEY\n", "CA\n"}[i] {
						t.Fatal(string(data), err)
					}
				}
			}
		})
	}
}

func TestTemporalCertificateOverwriteNoPartialWrites(t *testing.T) {
	for _, existing := range []string{"worker.crt", "worker.key", "ca.crt"} {
		t.Run(existing, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, existing)
			if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
				t.Fatal(err)
			}
			paths := []string{filepath.Join(dir, "worker.crt"), filepath.Join(dir, "worker.key"), filepath.Join(dir, "ca.crt")}
			if err := writeTemporalCertificateFiles(paths, []string{"CERT", "KEY", "CA"}); err == nil {
				t.Fatal("overwrite accepted")
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 {
				t.Fatal(entries, err)
			}
			data, err := os.ReadFile(path)
			if err != nil || string(data) != "original" {
				t.Fatal(string(data), err)
			}
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++ }))
			defer server.Close()
			_, _, err = execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "temporal", "client-certificate", "issue", "app", "--common-name", "worker", "--out-dir", dir)
			if err == nil || requests != 0 {
				t.Fatal(err, requests)
			}
		})
	}
}

func TestTemporalLists(t *testing.T) {
	for _, resource := range []string{"client-certificate", "namespace"} {
		for _, format := range []string{"table", "json", "yaml"} {
			t.Run(resource+"/"+format, func(t *testing.T) {
				path := "client-certificates"
				response := `{"items":[{"serial":"one","commonName":"worker","currentCa":true,"notAfter":"2026-11-01T00:00:00Z","caFingerprintSha256":"1234567890abcdef","fingerprintSha256":"full-client-fingerprint"},{"serial":"two","commonName":"old","currentCa":false,"notAfter":"2026-11-01T00:00:00Z","caFingerprintSha256":"1234567890abcdef","fingerprintSha256":"old-client-fingerprint"}]}`
				if resource == "namespace" {
					path = "namespaces"
					response = `{"items":[{"name":"orders","retention":"168h","managed":false,"state":"registered"}]}`
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != "GET" || r.URL.Path != "/api/v1/clusters/app/temporal/"+path || r.URL.Query().Get("namespace") != "project" {
						t.Error(r.Method, r.URL)
					}
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, response)
				}))
				defer server.Close()
				out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "--namespace", "project", "temporal", resource, "list", "app", "-o", format)
				if err != nil {
					t.Fatal(err)
				}
				if resource == "namespace" {
					if !strings.Contains(out, "orders") {
						t.Fatal(out)
					}
					return
				}
				if format == "table" {
					if !strings.Contains(out, "SERIAL") || !strings.Contains(out, "COMMONNAME") || !strings.Contains(out, "NOTAFTER") || !strings.Contains(out, "CAFINGERPRINT") || !strings.Contains(out, "1234567890ab") || strings.Contains(out, "1234567890abcdef") || strings.Contains(out, "full-client-fingerprint") {
						t.Fatal(out)
					}
					if !strings.Contains(out, "CURRENT") || !strings.Contains(out, "yes") || !strings.Contains(out, "no") {
						t.Fatal(out)
					}
				} else if !strings.Contains(out, "1234567890abcdef") || !strings.Contains(out, "full-client-fingerprint") {
					t.Fatal(out)
				}
				if format == "json" && (!strings.Contains(out, `"currentCa": true`) || !strings.Contains(out, `"currentCa": false`)) {
					t.Fatal(out)
				}
				if format == "yaml" && (!strings.Contains(out, "currentCa: true") || !strings.Contains(out, "currentCa: false")) {
					t.Fatal(out)
				}
			})
		}
	}
}

func TestTemporalNamespaceDeleteConfirmation(t *testing.T) {
	for _, input := range []string{"", "no\n", "yes\n", "flag"} {
		t.Run(input, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != "DELETE" || r.URL.Path != "/api/v1/clusters/app/temporal/namespaces/orders" || r.URL.Query().Get("namespace") != "project" {
					t.Error(r.Method, r.URL)
				}
				w.WriteHeader(202)
			}))
			defer server.Close()
			args := []string{"--namespace", "project", "temporal", "namespace", "delete", "app", "orders"}
			if input == "flag" {
				args = append(args, "--yes")
			}
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), input, args...)
			if input == "yes\n" || input == "flag" {
				if err != nil || requests != 1 {
					t.Fatal(err, requests)
				}
			} else if err == nil || requests != 0 {
				t.Fatal(err, requests)
			}
		})
	}
}

func TestTemporalOIDCSecret(t *testing.T) {
	for _, source := range []string{"file", "stdin"} {
		t.Run(source, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "PUT" || r.URL.Path != "/api/v1/clusters/app/temporal/ui/oidc-client-secret" || r.URL.Query().Get("namespace") != "project" {
					t.Error(r.Method, r.URL)
				}
				var body map[string]string
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if !reflect.DeepEqual(body, map[string]string{"clientSecret": "secret\n"}) {
					t.Error(body)
				}
				w.WriteHeader(204)
			}))
			defer server.Close()
			args := []string{"--namespace", "project", "temporal", "ui-oidc-secret", "set", "app"}
			input := "secret\n\n"
			if source == "file" {
				path := filepath.Join(t.TempDir(), "secret")
				if err := os.WriteFile(path, []byte(input), 0600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--from-file", path)
			} else {
				args = append(args, "--from-stdin")
			}
			out, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), input, args...)
			if err != nil || strings.Contains(out+stderr, "secret") {
				t.Fatal(err, out, stderr)
			}
		})
	}
}

func TestTemporalInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		{"client-certificate", "issue", "app", "--common-name", "worker"},
		{"client-certificate", "issue", "app", "--common-name", "worker", "--stdout", "--out-dir", "dir"},
		{"client-certificate", "issue", "app", "--stdout"},
		{"client-certificate", "issue", "app", "--common-name", "../worker", "--stdout"},
		{"client-certificate", "issue", "app", "--common-name", "ca", "--out-dir", "dir"},
		{"client-certificate", "issue", "app", "--common-name", "worker", "--stdout=false"},
		{"client-certificate", "issue", "app", "--common-name", "worker", "--stdout", "--ttl", "bad"},
		{"client-certificate", "issue", "app", "--common-name", "worker", "--stdout", "--ttl", "0h"},
		{"client-certificate", "issue", "app", "--common-name", "worker", "--stdout", "--ttl", "-1h"},
		{"client-certificate", "issue", "app", "--common-name", "worker", "--stdout", "--ttl", "1m"},
		{"ui-oidc-secret", "set", "app"},
		{"ui-oidc-secret", "set", "app", "argv-secret"},
		{"ui-oidc-secret", "set", "app", "--client-secret", "argv-secret"},
		{"ui-oidc-secret", "set", "app", "--from-stdin", "--from-file", "secret"},
		{"ui-oidc-secret", "set", "app", "--from-stdin=false"},
		{"ui-oidc-secret", "set", "app", "--from-stdin"},
	} {
		requests := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++ }))
		_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "\n", append([]string{"temporal"}, args...)...)
		server.Close()
		if err == nil || requests != 0 {
			t.Errorf("%v: err=%v requests=%d", args, err, requests)
		}
	}
}

func TestTemporalHelpInventoryAndCompletion(t *testing.T) {
	root := NewCommand(Options{})
	for _, spec := range temporalCommandSpecs {
		words := strings.Fields(spec.path)[:3]
		cmd, _, err := root.Find(words)
		if err != nil || cmd.Annotations[operationAnnotation] != spec.operation {
			t.Fatalf("%v: %v", words, err)
		}
		code, out, stderr := runCLI(t, "", "", false, append(words, "--help")...)
		if code != 0 || out == "" || stderr != "" {
			t.Fatal(code, out, stderr)
		}
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--help"}, "temporal"},
		{[]string{"__complete", ""}, "temporal"},
		{[]string{"__complete", "temporal", ""}, "client-certificate"},
		{[]string{"__complete", "temporal", "namespace", ""}, "delete"},
	} {
		code, out, stderr := runCLI(t, "", "", false, tc.args...)
		if code != 0 || !strings.Contains(out, tc.want) {
			t.Fatal(code, out, stderr)
		}
	}
}

func TestTemporalIssueDefaultTTL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["ttlHours"] != float64(720) {
			t.Error(body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		fmt.Fprint(w, temporalIssuedFixture)
	}))
	defer server.Close()
	_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "temporal", "client-certificate", "issue", "app", "--common-name", "worker", "--stdout")
	if err != nil {
		t.Fatal(err)
	}
}

func TestTemporalOIDCSecretNewlinesAndEmptyFile(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"", ""}, {"\n", ""}, {"\r\n", ""},
		{"secret\r\n", "secret"}, {" secret \n", " secret "},
		{"secret\r", "secret\r"}, {"secret\n\n", "secret\n"},
	} {
		t.Run(fmt.Sprintf("%q", tc.input), func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				var body map[string]string
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if body["clientSecret"] != tc.want {
					t.Errorf("secret = %q, want %q", body["clientSecret"], tc.want)
				}
				w.WriteHeader(204)
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "secret")
			if err := os.WriteFile(path, []byte(tc.input), 0600); err != nil {
				t.Fatal(err)
			}
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "temporal", "ui-oidc-secret", "set", "app", "--from-file", path)
			if tc.want == "" {
				if err == nil || requests != 0 {
					t.Fatal(err, requests)
				}
			} else if err != nil || requests != 1 {
				t.Fatal(err, requests)
			}
		})
	}
}
