// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestImportRejectsVariableAndSecretKeyOverlap(t *testing.T) {
	calls := 0
	b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) { calls++; t.Errorf("unexpected request %s", r.URL.Path) })
	_, err := b.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: "member", Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Env: map[string]string{"OWNER": "secret"}, Vars: map[string]string{"OWNER": "public"}}})
	if err == nil || calls != 0 {
		t.Fatalf("overlap accepted: calls=%d err=%v", calls, err)
	}
}

func TestSetEnvVarsAndSecretsSensitivity(t *testing.T) {
	for _, sensitive := range []bool{false, true} {
		name := "variables"
		if sensitive {
			name = "secrets"
		}
		t.Run(name, func(t *testing.T) {
			writes := map[string]string{}
			values := map[string]string{"SAME": "value", "KARAKORAM_OWNER": "value", "MASKED": "value", "MASKED_LITERAL": "REDACTED", "NEW": "value"}
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/env") {
					_ = json.NewEncoder(w).Encode(map[string]any{"items": []EnvItem{
						{ID: "same", Key: "SAME", Content: "value", Type: "USER", Sensitive: sensitive},
						{ID: "convert", Key: "KARAKORAM_OWNER", Content: "value", Type: "USER", Sensitive: !sensitive},
						{ID: "masked", Key: "MASKED", Content: "REDACTED", Type: "USER", Sensitive: sensitive},
						{ID: "masked-literal", Key: "MASKED_LITERAL", Content: "REDACTED", Type: "USER", Sensitive: sensitive},
					}})
					return
				}
				if serviceHandler(t, w, r) {
					return
				}
				if strings.HasSuffix(r.URL.Path, "/user-data") || strings.HasPrefix(r.URL.Path, "/user-data/") {
					var body struct {
						Key       string `json:"key"`
						Content   string `json:"content"`
						Sensitive *bool  `json:"sensitive"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if body.Sensitive == nil || *body.Sensitive != sensitive || body.Content != values[body.Key] {
						t.Errorf("incorrect user-data payload: %+v", body)
					}
					writes[body.Key] = r.Method + " " + r.URL.Path
					if r.Method == http.MethodPost {
						reply(t, w, "createSecret")
					} else {
						reply(t, w, "updateSecret")
					}
					return
				}
				if strings.HasPrefix(r.URL.Path, "/process/") {
					reply(t, w, "finished")
					return
				}
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				http.Error(w, "unexpected", http.StatusNotFound)
			})
			var err error
			if sensitive {
				err = b.SetEnvSecrets(t.Context(), "controlplane", values)
			} else {
				err = b.SetEnvVars(t.Context(), "controlplane", values)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(writes) != 4 || writes["SAME"] != "" || writes["KARAKORAM_OWNER"] != "PUT /user-data/convert" || writes["MASKED"] != "PUT /user-data/masked" || writes["MASKED_LITERAL"] != "PUT /user-data/masked-literal" || !strings.HasPrefix(writes["NEW"], "POST /service-stack/") {
				t.Fatalf("incorrect writes: %v", writes)
			}
		})
	}
}

func TestOpStatusProcessNotFoundExpiryByKind(t *testing.T) {
	for _, tc := range []struct {
		name, op, status      string
		done, failed, expired bool
		serviceIDs            []string
	}{
		{name: "single-expired", op: "expired", status: "EXPIRED", done: true, expired: true},
		{name: "all-expired", op: `multi:["expired","expired2"]`, status: "EXPIRED", done: true, expired: true},
		{name: "expired-and-running", op: `multi:["expired","running"]`, status: "RUNNING", expired: true, serviceIDs: []string{"runtime-id"}},
		{name: "expired-and-finished", op: `multi:["expired","finished"]`, status: "EXPIRED", done: true, expired: true, serviceIDs: []string{"volume-id"}},
		{name: "service-ids", op: `multi:["finished","running","finished"]`, status: "RUNNING", serviceIDs: []string{"volume-id", "runtime-id"}},
		{name: "failure-wins", op: `multi:["expired","running","failed"]`, status: "FAILED", done: true, failed: true},
	} {
		for _, kind := range []string{"import", "deploy", "delete", ""} {
			t.Run(tc.name+"/"+kind, func(t *testing.T) {
				b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
					id := strings.TrimPrefix(r.URL.Path, "/process/")
					if strings.HasPrefix(id, "expired") {
						w.WriteHeader(http.StatusBadRequest)
						_, _ = w.Write([]byte(`{"error":{"code":"processNotFound","message":"Process not found"}}`))
						return
					}
					status, serviceID := "FINISHED", "volume-id"
					if id == "running" {
						status, serviceID = "RUNNING", "runtime-id"
					}
					if id == "failed" {
						status = "FAILED"
					}
					_ = json.NewEncoder(w).Encode(Process{ID: id, Status: status, ServiceStackID: serviceID})
				})
				state, err := b.OpStatus(t.Context(), infra.Op{ID: tc.op, Kind: kind})
				wantFailed := tc.failed || tc.done && tc.expired && kind != "import"
				if err != nil || state.Done != tc.done || state.Failed != wantFailed || state.Status != tc.status {
					t.Fatalf("state=%+v err=%v", state, err)
				}
				if tc.done && tc.expired && kind != "import" && !strings.Contains(state.Message, "retry") {
					t.Fatalf("expired non-import did not explain the retry: %+v", state)
				}
				if state.Expired != tc.expired || !reflect.DeepEqual(state.ServiceIDs, tc.serviceIDs) {
					t.Fatalf("expiration/identity state=%+v", state)
				}
			})
		}
	}
}

func TestSetEnvVarsRestrictsSensitiveConversionToOwnerMarkers(t *testing.T) {
	for _, key := range []string{"KARAKORAM_AGENT_TOKEN", "OTHER_SECRET", "KARAKORAM_OWNER", "KARAKORAM_RELAY_OWNER"} {
		for _, content := range []string{"value", "REDACTED"} {
			t.Run(key+"/"+content, func(t *testing.T) {
				writes := 0
				allowed := key == "KARAKORAM_OWNER" || key == "KARAKORAM_RELAY_OWNER"
				b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
					if strings.HasSuffix(r.URL.Path, "/env") {
						_ = json.NewEncoder(w).Encode(map[string]any{"items": []EnvItem{{ID: "existing", Key: key, Content: content, Type: "USER", Sensitive: true}}})
						return
					}
					if serviceHandler(t, w, r) {
						return
					}
					if r.Method == http.MethodPut && r.URL.Path == "/user-data/existing" {
						writes++
						var body struct {
							Key       string `json:"key"`
							Content   string `json:"content"`
							Sensitive *bool  `json:"sensitive"`
						}
						if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
							t.Fatal(err)
						}
						if !allowed || body.Key != key || body.Content != "value" || body.Sensitive == nil || *body.Sensitive {
							t.Errorf("unsafe conversion payload: %+v", body)
						}
						reply(t, w, "updateSecret")
						return
					}
					if strings.HasPrefix(r.URL.Path, "/process/") {
						reply(t, w, "finished")
						return
					}
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected", http.StatusNotFound)
				})
				err := b.SetEnvVars(t.Context(), "controlplane", map[string]string{key: "value"})
				if allowed {
					if err != nil || writes != 1 {
						t.Fatalf("owner marker was not converted: writes=%d err=%v", writes, err)
					}
				} else if err == nil || !strings.Contains(err.Error(), "refusing") || writes != 0 {
					t.Fatalf("sensitive value was not protected: writes=%d err=%v", writes, err)
				}
			})
		}
	}
}

func TestSudoModeParsesAuthenticationStatus(t *testing.T) {
	for _, want := range []bool{false, true} {
		b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/auth/sudo" {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			}
			_ = json.NewEncoder(w).Encode(map[string]bool{"inSudo": want})
		})
		got, err := b.SudoMode(t.Context())
		if err != nil || got != want {
			t.Fatalf("sudo=%v want=%v err=%v", got, want, err)
		}
	}
}
