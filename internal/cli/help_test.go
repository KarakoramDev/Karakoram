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

	"github.com/KarakoramDev/Karakoram/internal/version"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func TestEveryCommandHasHumanHelp(t *testing.T) {
	t.Setenv("KARAKORAM_SERVER", "")
	t.Setenv("KARAKORAM_TOKEN", "")
	root := NewCommand(Options{})
	root.InitDefaultHelpCmd()
	var walk func(*cobra.Command, []string)
	walk = func(cmd *cobra.Command, path []string) {
		t.Run(strings.Join(path, " "), func(t *testing.T) {
			short := strings.ToLower(strings.TrimSpace(cmd.Short))
			commandPath := strings.ToLower(strings.Join(path, " "))
			if short == "" || short == strings.ToLower(cmd.Name()) || short == commandPath || short == "manage "+strings.ToLower(cmd.Name()) || short == "manage "+commandPath || strings.HasPrefix(short, commandPath+":") {
				t.Errorf("command lacks a human description: %s: %q", cmd.CommandPath(), cmd.Short)
			}
			for _, operation := range strings.Split(cmd.Annotations[operationAnnotation], ",") {
				if operation != "" && short == strings.ToLower(operation) {
					t.Errorf("API operation used as help: %s: %q", cmd.CommandPath(), cmd.Short)
				}
			}
			args := append(append([]string{}, path...), "--help")
			code, out, stderr := runCLI(t, "", "", false, args...)
			if code != 0 || out == "" || stderr != "" {
				t.Errorf("help failed: code=%d stdout=%q stderr=%q", code, out, stderr)
			}
		})
		for _, child := range cmd.Commands() {
			walk(child, append(append([]string{}, path...), child.Name()))
		}
	}
	walk(root, nil)
}

func TestVersionWithoutConfiguredServer(t *testing.T) {
	t.Setenv("KARAKORAM_SERVER", "")
	t.Setenv("KARAKORAM_TOKEN", "")
	for _, format := range []string{"table", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			code, out, stderr := runCLI(t, "", "", false, "version", "-o", format)
			if code != 0 || stderr != "" || !strings.Contains(out, version.VersionLabel()) || !strings.Contains(out, "not configured") || !strings.Contains(out, "client") {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, out, stderr)
			}
			if format == "table" {
				return
			}
			var document map[string]any
			var err error
			if format == "json" {
				err = json.Unmarshal([]byte(out), &document)
			} else {
				err = yaml.Unmarshal([]byte(out), &document)
			}
			if err != nil || nested(document, "client", "version") != version.VersionLabel() || document["serverStatus"] != "not configured" {
				t.Fatalf("invalid version document: %v: %s", err, out)
			}
		})
	}
}

func TestVersionWithUnreachableServerStillShowsClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "version", "-o", "json")
	var document map[string]any
	if err := json.Unmarshal([]byte(out), &document); err != nil {
		t.Fatal(err)
	}
	if code != 0 || stderr != "" || nested(document, "client", "version") != version.VersionLabel() || document["serverStatus"] != "unreachable" || document["serverError"] == nil {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, out, stderr)
	}
}

func TestVersionPrintsClientBeforeRequestingServer(t *testing.T) {
	var out, stderr bytes.Buffer
	requested := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = true
		if !strings.Contains(out.String(), "client:") || !strings.Contains(out.String(), version.VersionLabel()) {
			t.Error("client metadata was not printed before the server request")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"version":"1.2.3","commit":"server-commit","buildDate":"2026-01-01T00:00:00Z"}`)
	}))
	defer server.Close()
	code := Run(context.Background(), Options{ConfigPath: filepath.Join(t.TempDir(), "config.yaml"), In: strings.NewReader(""), Out: &out, Err: &stderr, IsTerminal: func(io.Reader) bool { return false }}, []string{"--server", server.URL, "version"})
	if code != 0 || !requested || stderr.Len() != 0 || !strings.Contains(out.String(), "server:") || !strings.Contains(out.String(), "1.2.3") {
		t.Fatalf("code=%d requested=%t stdout=%q stderr=%q", code, requested, out.String(), stderr.String())
	}
}
