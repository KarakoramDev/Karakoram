// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak/kcspec"
)

func validSpec() kcspec.Spec {
	return kcspec.Spec{Version: kcspec.Version, Host: "writer.internal", Port: 5432, Database: "keycloak", Username: "binding_role", Password: "secret-value", CAPEM: "CA", AdminPassword: "bootstrap-value", CredentialGeneration: 1}
}
func TestRenderProtectsSecretsAndTLS(t *testing.T) {
	s := validSpec()
	p, files, e := Render(s, "/data")
	if e != nil {
		t.Fatal(e)
	}
	if !strings.HasSuffix(p.Binary, "/bin/kc.sh") || strings.Join(p.Args, " ") != "--config-file=/data/karakoram/keycloak/keycloak.conf start --optimized" {
		t.Fatal(p)
	}
	for _, v := range append(p.Env, p.Args...) {
		if strings.Contains(v, s.Password) || strings.Contains(v, s.AdminPassword) {
			t.Fatal("secret outside protected files")
		}
	}
	conf := string(files["keycloak/keycloak.conf"])
	for _, expected := range []string{"db-password=secret-value", "bootstrap-admin-password=bootstrap-value", "http-management-host=127.0.0.1", "http-management-port=9000", "health-enabled=true"} {
		if !strings.Contains(conf, expected) {
			t.Errorf("missing %s", expected)
		}
	}
	jdbc := strings.TrimPrefix(s.JDBCURL("/data/ca.pem"), "jdbc:")
	u, e := url.Parse(jdbc)
	if e != nil || u.Query().Get("sslmode") != "verify-full" || u.Query().Get("sslrootcert") != "/data/ca.pem" || u.User != nil {
		t.Fatal(u, e)
	}
	if string(files["keycloak/ca.pem"]) != "CA" {
		t.Fatal("missing CA")
	}
}
func TestConfigurationEscapesProperties(t *testing.T) {
	s := validSpec()
	s.Password = "password\nhttp-enabled=false\r${lookup}\\=:"
	_, files, e := Render(s, "/data")
	if e != nil {
		t.Fatal(e)
	}
	conf := string(files["keycloak/keycloak.conf"])
	if strings.Contains(conf, "\nhttp-enabled=false") || !strings.Contains(conf, `db-password=password\nhttp-enabled\=false\r\\${lookup}\\\=\:`) {
		t.Fatal("property injection not escaped")
	}
}
func TestRenderRejectsInvalidConfiguration(t *testing.T) {
	s := validSpec()
	s.Password = ""
	if _, _, e := Render(s, "/data"); e == nil {
		t.Fatal("accepted missing password")
	}
	s = validSpec()
	s.ProxyHeaders = "unsafe"
	if _, _, e := Render(s, "/data"); e == nil {
		t.Fatal("accepted invalid proxy headers")
	}
}
func TestAgentRoutesAcceptEngineRPC(t *testing.T) {
	mux := http.NewServeMux()
	(&Runner{}).Routes(mux, nil)
	for _, method := range []string{"POST", "GET"} {
		r := httptest.NewRequest(method, "/v1/keycloak/status", strings.NewReader(`{}`))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"ready":`) {
			t.Fatal(method, w.Code, w.Body.String())
		}
	}
}

type lifecycleNode struct {
	actions []string
	held    bool
	running bool
	fail    error
}

func (*lifecycleNode) DataDir() string                             { return "/data" }
func (*lifecycleNode) WriteFile(string, []byte, os.FileMode) error { return nil }
func (*lifecycleNode) Configure(string, agent.ProcessSpec) error   { return nil }
func (n *lifecycleNode) Stopped(string) bool                       { return n.held }
func (n *lifecycleNode) Action(_ context.Context, name, action string) error {
	n.actions = append(n.actions, name+":"+action)
	if n.fail != nil {
		return n.fail
	}
	n.running = action == "start"
	return nil
}

func TestApplyLifecycle(t *testing.T) {
	for _, hold := range []string{"none", "maintenance", "operator-stop", "desired-stop"} {
		t.Run(hold, func(t *testing.T) {
			r := &Runner{}
			n := &lifecycleNode{held: hold == "operator-stop"}
			c := agentapi.MemberConfig{Engine: "keycloak", Maintenance: hold == "maintenance"}
			if hold == "desired-stop" {
				c.DesiredProcesses = []string{}
			}
			builds := 0
			build := func(context.Context, agent.ProcessSpec) error {
				builds++
				return nil
			}
			s := validSpec()
			for generation := range 2 {
				s.CredentialGeneration = int64(generation + 1)
				if generation > 0 {
					s.Password = "rotated-password"
				}
				var err error
				c.EngineSpec, err = json.Marshal(s)
				if err != nil {
					t.Fatal(err)
				}
				changed, err := r.apply(t.Context(), n, c, build)
				if err != nil || !slices.Equal(changed, []string{"keycloak"}) {
					t.Fatalf("apply %d: changed=%v err=%v", generation, changed, err)
				}
				if n.running != (hold == "none") {
					t.Fatalf("apply %d: running=%v", generation, n.running)
				}
			}
			want := []string{"keycloak:stop"}
			if hold == "none" {
				want = []string{"keycloak:start", "keycloak:stop", "keycloak:start"}
			}
			if !slices.Equal(n.actions, want) || builds != 1 {
				t.Fatalf("actions=%v want=%v builds=%d", n.actions, want, builds)
			}
		})
	}
}

func TestApplyStartFailureDoesNotRecordDigest(t *testing.T) {
	r := &Runner{}
	n := &lifecycleNode{fail: errors.New("start failed")}
	raw, err := json.Marshal(validSpec())
	if err != nil {
		t.Fatal(err)
	}
	c := agentapi.MemberConfig{Engine: "keycloak", EngineSpec: raw}
	build := func(context.Context, agent.ProcessSpec) error { return nil }
	if _, err = r.apply(t.Context(), n, c, build); !errors.Is(err, n.fail) {
		t.Fatalf("start failure: %v", err)
	}
	if r.applied || r.digest != [32]byte{} {
		t.Fatal("failed start recorded as applied")
	}
	n.fail = nil
	if changed, err := r.apply(t.Context(), n, c, build); err != nil || len(changed) != 1 || !n.running {
		t.Fatalf("retry: changed=%v running=%v err=%v", changed, n.running, err)
	}
}
