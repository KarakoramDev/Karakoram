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
	"strings"
	"testing"
)

func TestNamespaceResolution(t *testing.T) {
	for _, tt := range []struct {
		name, saved, flag, want string
		args                    []string
	}{
		{"default", "", "", "default", []string{"clusters", "get", "orders"}},
		{"context", "dev", "", "dev", []string{"clusters", "get", "orders"}},
		{"flag", "dev", "prod", "prod", []string{"clusters", "get", "orders"}},
		{"collection-all", "dev", "", "", []string{"clusters", "list"}},
		{"collection-filter", "dev", "prod", "prod", []string{"clusters", "list"}},
		{"storage-all", "dev", "", "", []string{"storages", "list"}},
		{"operation-all", "dev", "", "", []string{"operations", "list"}},
		{"events-filter", "dev", "prod", "prod", []string{"events", "list"}},
		{"cluster-list-scoped", "dev", "", "dev", []string{"backups", "list", "orders"}},
		{"member", "dev", "", "dev", []string{"clusters", "members", "orders", "m0"}},
		{"storage", "dev", "", "dev", []string{"storages", "get", "snapshots"}},
		{"metrics", "dev", "prod", "prod", []string{"metrics"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("namespace"); got != tt.want {
					t.Errorf("namespace=%q want=%q", got, tt.want)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{}`)
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := SaveConfig(path, Config{CurrentContext: "saved", Contexts: map[string]Context{"saved": {Namespace: tt.saved}}}); err != nil {
				t.Fatal(err)
			}
			args := append([]string{}, tt.args...)
			if tt.flag != "" {
				args = append(args, "-n", tt.flag)
			}
			if _, _, err := execute(t, server.URL, path, "", args...); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNamespaceCollectionTable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[{"namespace":"dev","name":"orders"},{"namespace":"prod","name":"orders"}]}`)
	}))
	defer server.Close()
	out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "clusters", "list")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out, "NAMESPACE") || !strings.Contains(out, "dev") || !strings.Contains(out, "prod") {
		t.Fatalf("missing namespace column: %q", out)
	}
}

func TestNamespaceTokenInput(t *testing.T) {
	const secret = "synthetic-project-token"
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(secret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, file, input string
		args              []string
	}{
		{"create-file", path, "", []string{"namespaces", "create", "--name", "dev", "--project-id", "proj-test", "--description", "Development"}},
		{"create-stdin", "-", secret + "\n", []string{"namespaces", "create", "--name", "dev", "--project-id", "proj-test"}},
		{"rotate-stdin", "-", secret + "\n", []string{"namespaces", "rotate-token", "dev", "--yes"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body["token"] != secret {
					t.Error("token input mismatch")
				}
				if tt.name != "rotate-stdin" && (body["name"] != "dev" || body["projectId"] != "proj-test") {
					t.Error("namespace create fields missing")
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"name":"dev","phase":"Ready"}`)
			}))
			defer server.Close()
			args := append(append([]string{}, tt.args...), "--token-file", tt.file)
			out, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), tt.input, args...)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(out+stderr, secret) {
				t.Fatal("token appeared in output")
			}
		})
	}
	for _, args := range [][]string{
		{"namespaces", "create", "--name", "dev", "--project-id", "proj-test"},
		{"namespaces", "rotate-token", "dev", "--token-file", "-"},
	} {
		if _, _, err := execute(t, "https://example.com", filepath.Join(t.TempDir(), "config.yaml"), "", args...); err == nil {
			t.Fatal("missing token accepted")
		}
	}
}

func TestCrossNamespaceCloneBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			if r.URL.Path != "/api/v1/clusters/orders" || r.URL.Query().Get("namespace") != "prod" {
				t.Errorf("unexpected source request %s", r.URL)
			}
			fmt.Fprint(w, `{"spec":{"provider":"psmdb","backup":{"storage":"prod-storage"},"topology":{"type":"replicaSet"}}}`)
		case http.MethodPost:
			if r.URL.Query().Get("namespace") != "dev" {
				t.Error("clone not created in target namespace")
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if nested(body, "name") != "copy" || nested(body, "spec", "dataSource", "namespace") != "prod" || nested(body, "spec", "dataSource", "cluster") != "orders" || nested(body, "spec", "dataSource", "backup") != "snapshot" {
				t.Fatalf("wrong clone body: %+v", body)
			}
			if nested(body, "spec", "backup") != nil {
				t.Error("clone retained namespace-local backup configuration")
			}
			fmt.Fprint(w, `{"name":"copy","namespace":"dev"}`)
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	if _, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "-n", "dev", "restores", "create", "orders", "--backup", "snapshot", "--source-namespace", "prod", "--into-new", "copy"); err != nil {
		t.Fatal(err)
	}
}

func TestInPlaceRestoreSourceNamespace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["sourceNamespace"] != "prod" || nested(body, "source", "sourceCluster") != "orders" || r.URL.Query().Get("namespace") != "dev" {
			t.Errorf("wrong restore request")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	if _, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "-n", "dev", "restores", "create", "copy", "--yes", "--backup", "snapshot", "--source-cluster", "orders", "--source-namespace", "prod"); err != nil {
		t.Fatal(err)
	}
}

func TestSetContextNamespace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if _, _, err := execute(t, "https://example.com", path, "", "config", "set-context", "dev", "-n", "development"); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Contexts["dev"].Namespace != "development" || c.Contexts["dev"].Server != "https://example.com" {
		t.Fatal("context values not saved")
	}
}

func TestNamespaceStatusTable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[{"name":"dev","backend":"zerops","projectId":"proj-test","default":false,"phase":"Ready","token":{"valid":true},"relay":{"phase":"Ready","connected":true},"clusterCount":3}]}`)
	}))
	defer server.Close()
	out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "namespaces", "list")
	if err != nil {
		t.Fatal(err)
	}
	for _, heading := range []string{"NAME", "BACKEND", "PROJECT", "DEFAULT", "PHASE", "TOKEN VALID", "RELAY PHASE", "CONNECTED", "CLUSTERS", "zerops"} {
		if !strings.Contains(out, heading) {
			t.Errorf("missing %s", heading)
		}
	}
}

func TestNamespaceListFollowsAllPages(t *testing.T) {
	for _, format := range []string{"table", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			var cursors []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/namespaces" {
					t.Errorf("unexpected namespace request: %s", r.URL)
				}
				cursor := r.URL.Query().Get("cursor")
				cursors = append(cursors, cursor)
				w.Header().Set("Content-Type", "application/json")
				switch cursor {
				case "":
					fmt.Fprint(w, `{"items":[{"name":"dev","projectId":"proj-test"}],"nextCursor":"page/+two="}`)
				case "page/+two=":
					fmt.Fprint(w, `{"items":[{"name":"prod","projectId":"proj-other"}]}`)
				default:
					t.Errorf("unexpected cursor: %q", cursor)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			out, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "namespaces", "list", "-o", format)
			if err != nil {
				t.Fatal(err)
			}
			if len(cursors) != 2 || cursors[0] != "" || cursors[1] != "page/+two=" {
				t.Fatalf("did not follow namespace pages: %v", cursors)
			}
			if !strings.Contains(out, "dev") || !strings.Contains(out, "prod") {
				t.Fatalf("namespace output omitted a page: %q", out)
			}
			if strings.Contains(out, "nextCursor") || strings.Contains(stderr, "Next page") {
				t.Fatalf("complete collection still advertises another page: %q %q", out, stderr)
			}
		})
	}
}

func TestNamespaceListPageFailureDoesNotPrintPartialCollection(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("cursor") == "" {
			fmt.Fprint(w, `{"items":[{"name":"dev"}],"nextCursor":"second"}`)
			return
		}
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"code":"forbidden","message":"namespace read denied"}`)
	}))
	defer server.Close()
	out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "namespaces", "list")
	if err == nil || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("expected the second page error, got %v", err)
	}
	if requests != 2 || out != "" {
		t.Fatalf("partial namespace collection printed after page failure: requests=%d output=%q", requests, out)
	}
}

func TestNamespaceOperationGetAndWaitUseGlobalID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/operations/one" || r.URL.Query().Get("namespace") != "" {
			t.Errorf("operation ID lookup must be global: %s", r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"one","namespace":"dev","cluster":"orders","kind":"restart","phase":"Succeeded"}`)
	}))
	defer server.Close()
	for _, command := range []string{"get", "wait"} {
		if _, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "-n", "dev", "operations", command, "one"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNamespaceBackendAndConfigFlags(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body struct {
			Backend string            `json:"backend"`
			Config  map[string]string `json:"config"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Backend != "nomad" || body.Config["region"] != "example" || body.Config["url"] != "https://example.com?a=b" {
			t.Fatalf("backend inputs: %+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"name":"dev"}`)
	}))
	defer server.Close()
	args := []string{"namespaces", "create", "--name", "dev", "--project-id", "target", "--backend", "nomad", "--config", "region=example", "--config", "url=https://example.com?a=b", "--token-file", "-"}
	if _, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "credential", args...); err != nil {
		t.Fatal(err)
	}
	for _, config := range []string{"invalid", "=value"} {
		if _, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "credential", "namespaces", "create", "--name", "dev", "--project-id", "target", "--config", config, "--token-file", "-"); err == nil {
			t.Fatal("invalid backend config accepted", config)
		}
	}
	if requests != 1 {
		t.Fatal("invalid configuration made requests", requests)
	}
}

func TestBackendsList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/backends" {
			t.Errorf("backend path: %s", r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[{"kind":"zerops","displayName":"Zerops","experimental":false,"relay":true,"capabilities":["publicTcp"],"configKeys":["apiUrl"]}]}`)
	}))
	defer server.Close()
	out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "backends", "list")
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"KIND", "NAME", "EXPERIMENTAL", "RELAY", "CAPABILITIES", "CONFIG KEYS", "zerops", "publicTcp", "apiUrl"} {
		if !strings.Contains(out, text) {
			t.Errorf("missing %q in %s", text, out)
		}
	}
}
