// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

type confirmationInput struct {
	io.Reader
	reads int
}

func (r *confirmationInput) Read(p []byte) (int, error) {
	r.reads++
	return r.Reader.Read(p)
}

func TestDestructiveStdinPayloadRequiresYesBeforeReading(t *testing.T) {
	for _, tty := range []bool{false, true} {
		for _, args := range [][]string{
			{"accounts", "reset-password", "user", "-f", "-"},
			{"namespaces", "rotate-token", "project", "--token-file", "-"},
		} {
			t.Run(fmt.Sprintf("%v/tty=%t", args, tty), func(t *testing.T) {
				calls := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					w.WriteHeader(http.StatusNoContent)
				}))
				defer server.Close()
				input := &confirmationInput{Reader: strings.NewReader("y\n")}
				var out, stderr bytes.Buffer
				options := Options{ConfigPath: filepath.Join(t.TempDir(), "config.yaml"), In: input, Out: &out, Err: &stderr, IsTerminal: func(io.Reader) bool { return tty }}
				code := Run(context.Background(), options, append([]string{"--server", server.URL}, args...))
				if code != 2 || calls != 0 || input.reads != 0 || !strings.Contains(stderr.String(), "pass --yes") {
					t.Fatalf("code=%d calls=%d stdin reads=%d stderr=%q", code, calls, input.reads, stderr.String())
				}
			})
		}
	}
}

func TestDestructiveStdinPayloadWithYesIsUnchanged(t *testing.T) {
	for _, tc := range []struct {
		args               []string
		input, field, want string
	}{
		{[]string{"accounts", "reset-password", "user", "-f", "-", "--yes"}, `{"password":"synthetic-password"}`, "password", "synthetic-password"},
		{[]string{"namespaces", "rotate-token", "project", "--token-file", "-", "--yes"}, "synthetic-token\n", "token", "synthetic-token"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if body[tc.field] != tc.want {
					t.Errorf("payload changed: %v", body)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{}`)
			}))
			defer server.Close()
			code, _, stderr := runCLI(t, server.URL, tc.input, false, tc.args...)
			if code != 0 || calls != 1 {
				t.Fatalf("code=%d calls=%d stderr=%q", code, calls, stderr)
			}
		})
	}
}

func TestYesDoesNotReadConfirmationInput(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	input := &confirmationInput{Reader: strings.NewReader("n\n")}
	var out, stderr bytes.Buffer
	code := Run(context.Background(), Options{ConfigPath: filepath.Join(t.TempDir(), "config.yaml"), In: input, Out: &out, Err: &stderr}, []string{"--server", server.URL, "tokens", "delete", "token-one", "--yes"})
	if code != 0 || calls != 1 || input.reads != 0 {
		t.Fatalf("code=%d calls=%d stdin reads=%d stderr=%q", code, calls, input.reads, stderr.String())
	}
}
