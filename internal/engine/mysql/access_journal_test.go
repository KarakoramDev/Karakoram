// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type accessFaultSecrets struct {
	engine.SecretStore
	key    string
	failed bool
}

func (s *accessFaultSecrets) SetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string, raw []byte) error {
	if key == s.key && !s.failed {
		s.failed = true
		return errors.New("secret-store interruption")
	}
	return s.SecretStore.SetClusterSecret(ctx, ref, key, raw)
}
func installAccessAgent(t *testing.T, rc *engine.Context, handler http.HandlerFunc) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	rc.Agents = fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
}
func TestAccessIntentReplayAndProxyPublication(t *testing.T) {
	for _, operation := range []string{"create", "password", "delete"} {
		t.Run(operation, func(t *testing.T) {
			p, rc, b := setup(t, "group")
			converge(t, p, rc, b)
			previous := []myspec.ProxyUser{{Username: "app", Password: "previous-password", RequireTLS: true}}
			raw, _ := json.Marshal(previous)
			if err := putSecret(t.Context(), rc, accessUsersKey, raw); err != nil {
				t.Fatal(err)
			}
			requests := []myspec.AccessRequest{}
			writers := []string{}
			first := true
			acknowledged := false
			installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				switch r.URL.Path {
				case "/v1/mysql/status":
					status := myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "SECONDARY", ReadOnly: true, SuperReadOnly: true}
					writer := "demom0"
					if !first {
						writer = "demom1"
					}
					if name == writer {
						status.MemberRole = "PRIMARY"
						status.ReadOnly = false
						status.SuperReadOnly = false
					}
					json.NewEncoder(w).Encode(status)
				case "/v1/mysql/" + myspec.ActionAccessUsers:
					var req myspec.AccessRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					requests = append(requests, req)
					writers = append(writers, name)
					intent, err := loadAccessIntent(r.Context(), rc)
					if err != nil || intent == nil || intent.RequestHash != accessRequestHash(req) {
						t.Error("SQL reached before sealed exact intent", err)
					}
					if first {
						first = false
						w.WriteHeader(503)
						return
					}
					json.NewEncoder(w).Encode(myspec.AccessResult{Outcome: "applied"})
				case "/v1/status":
					cfg, err := p.MemberConfig(r.Context(), rc, name)
					if err != nil {
						t.Error(err)
						w.WriteHeader(503)
						return
					}
					var sp myspec.Spec
					_ = json.Unmarshal(cfg.EngineSpec, &sp)
					if sp.Proxy != nil {
						found := false
						for _, user := range sp.Proxy.Users {
							if user.Username == "app" {
								found = true
								if operation != "delete" && user.Password == "previous-password" {
									t.Error("published old password")
								}
							}
						}
						if found == (operation == "delete") {
							t.Error("incorrect managed proxy publication")
						}
						acknowledged = true
					}
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: cfg.ConfigVersion})
				case "/v1/mysql/proxy-status":
					json.NewEncoder(w).Encode(myspec.ProxyStatusResponse{Backends: []myspec.ProxyBackend{{Hostgroup: 10, Status: "ONLINE"}}})
				default:
					t.Error("unexpected route", r.URL.Path)
					w.WriteHeader(404)
				}
			})
			invoke := func() (string, error) {
				switch operation {
				case "create":
					return p.CreateUser(t.Context(), rc, UserWrite{Name: "app"})
				case "password":
					return p.RotateUserPassword(t.Context(), rc, "app", false)
				default:
					return "", p.DeleteUser(t.Context(), rc, "app")
				}
			}
			if _, err := invoke(); !errors.Is(err, engine.ErrUnavailable) {
				t.Fatal("missing ambiguous response failure", err)
			}
			pending, err := loadAccessIntent(t.Context(), rc)
			if err != nil || pending == nil {
				t.Fatal("missing sealed recovery", err)
			}
			password := generatedAccessPassword(pending)
			if password != "" && strings.Contains(string(rc.Cluster.Internal), password) {
				t.Fatal("password leaked into Cluster.Internal")
			}
			p = New(Options{})
			got, err := invoke()
			if err != nil {
				t.Fatal(err)
			}
			if got != password {
				t.Fatal("replay generated different password")
			}
			if len(requests) != 2 || accessRequestHash(requests[0]) != accessRequestHash(requests[1]) || writers[0] == writers[1] {
				t.Fatal("request changed or current writer not rediscovered", writers)
			}
			if !acknowledged {
				t.Fatal("success before proxy acknowledgement")
			}
			if pending, err := loadAccessIntent(t.Context(), rc); err != nil || pending != nil {
				t.Fatal("completed intent retained", err)
			}
		})
	}
}
func TestAccessSealBeforeAgentAndFences(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc, b)
	calls := 0
	installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/mysql/status" {
			json.NewEncoder(w).Encode(myspec.StatusResponse{ReadOnly: true, SuperReadOnly: true})
			return
		}
		calls++
		w.WriteHeader(503)
	})
	if _, err := p.CreateUser(t.Context(), rc, UserWrite{Name: "app"}); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("read-only mutation reached agent")
	}
	if err := clearAccessIntent(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	rc.Secrets = &accessFaultSecrets{SecretStore: rc.Secrets, key: accessPendingKey}
	if _, err := p.CreateUser(t.Context(), rc, UserWrite{Name: "app"}); err == nil {
		t.Fatal("failed intent seal accepted")
	}
	if calls != 0 {
		t.Fatal("agent called before intent seal")
	}
	for _, phase := range []domain.Phase{domain.PhaseRestoring, domain.PhaseUpgrading, domain.PhaseSuspended} {
		rc.Cluster.Status.Phase = phase
		if _, err := p.ListUsers(t.Context(), rc); !errors.Is(err, engine.ErrUnavailable) {
			t.Fatal("lifecycle fence missing", phase, err)
		}
	}
	rc.Cluster.Status.Phase = domain.PhaseReady
	s, _ := load(rc.Cluster)
	s.Formation.State = "unformed"
	_ = save(rc.Cluster, s)
	if _, err := p.ListDatabases(t.Context(), rc); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal("formation fence missing", err)
	}
}
func TestRetainedPasswordRejectedWithProxyBeforeSQL(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	if _, err := p.RotateUserPassword(t.Context(), rc, "app", true); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal(err)
	}
	if pending, err := loadAccessIntent(t.Context(), rc); err != nil || pending != nil {
		t.Fatal("rejected retainCurrent created intent", err)
	}
}
func TestAccessPublicationRecoverySkipsAppliedSQL(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc, b)
	calls := 0
	installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/mysql/status":
			json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"})
		case "/v1/mysql/" + myspec.ActionAccessUsers:
			calls++
			json.NewEncoder(w).Encode(myspec.AccessResult{Outcome: "applied"})
		default:
			w.WriteHeader(404)
		}
	})
	original := rc.Secrets
	rc.Secrets = &accessFaultSecrets{SecretStore: original, key: accessUsersKey}
	if _, err := p.CreateUser(t.Context(), rc, UserWrite{Name: "app"}); err == nil {
		t.Fatal("publication interruption missing")
	}
	pending, err := loadAccessIntent(t.Context(), rc)
	if err != nil || pending == nil || !pending.Applied {
		t.Fatal("application receipt not sealed", err)
	}
	rc.Secrets = original
	p = New(Options{})
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("published replay repeated SQL")
	}
}

func TestAccessCrashBeforeAgentCallReplaysSealedRequest(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc, b)
	available, calls := false, 0
	installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/mysql/status":
			if !available {
				w.WriteHeader(503)
				return
			}
			json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"})
		case "/v1/mysql/" + myspec.ActionAccessUsers:
			calls++
			var request myspec.AccessRequest
			_ = json.NewDecoder(r.Body).Decode(&request)
			intent, err := loadAccessIntent(r.Context(), rc)
			if err != nil || intent == nil || intent.RequestHash != accessRequestHash(request) {
				t.Error("replay did not use sealed intent", err)
			}
			json.NewEncoder(w).Encode(myspec.AccessResult{Outcome: "applied"})
		default:
			w.WriteHeader(404)
		}
	})
	if _, err := p.CreateUser(t.Context(), rc, UserWrite{Name: "app"}); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal(err)
	}
	intent, err := loadAccessIntent(t.Context(), rc)
	if err != nil || intent == nil || calls != 0 {
		t.Fatal("missing pre-agent crash intent", calls, err)
	}
	available = true
	p = New(Options{})
	password, err := p.CreateUser(t.Context(), rc, UserWrite{Name: "app"})
	if err != nil || password != generatedAccessPassword(intent) || calls != 1 {
		t.Fatal("pre-agent crash not recovered", calls, err)
	}
}

func TestAccessDefinitiveRejectionRestoresSnapshot(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc, b)
	prior := []byte(`[{"username":"app","password":"previous-password","requireTls":true}]`)
	if err := putSecret(t.Context(), rc, accessUsersKey, prior); err != nil {
		t.Fatal(err)
	}
	installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/mysql/status":
			json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"})
		case "/v1/mysql/" + myspec.ActionAccessUsers:
			json.NewEncoder(w).Encode(myspec.AccessResult{Outcome: "rejected", Status: 409, Code: "foreign_account"})
		default:
			w.WriteHeader(404)
		}
	})
	if _, err := p.CreateUser(t.Context(), rc, UserWrite{Name: "app"}); !errors.Is(err, engine.ErrConflict) {
		t.Fatal(err)
	}
	got, err := secret(t.Context(), rc, accessUsersKey)
	if err != nil || string(got) != string(prior) {
		t.Fatal("foreign account adopted into managed index", err)
	}
	if pending, err := loadAccessIntent(t.Context(), rc); err != nil || pending != nil {
		t.Fatal("rejected intent retained", err)
	}
}

func TestAccessWaitsForProxyAcknowledgement(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/mysql/status":
			json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"})
		case "/v1/mysql/" + myspec.ActionAccessUsers:
			json.NewEncoder(w).Encode(myspec.AccessResult{Outcome: "applied"})
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 0})
		case "/v1/reload":
			json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		default:
			w.WriteHeader(404)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := p.CreateUser(ctx, rc, UserWrite{Name: "app"}); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal("unacknowledged proxy reported success", err)
	}
	pending, err := loadAccessIntent(t.Context(), rc)
	if err != nil || pending == nil || !pending.Applied {
		t.Fatal("unacknowledged publication lost its applied intent", err)
	}
}

func TestAccessRetainedCredentialWithoutProxy(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc, b)
	installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/mysql/status" {
			json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"})
			return
		}
		json.NewEncoder(w).Encode(myspec.AccessResult{Outcome: "applied"})
	})
	old, err := p.CreateUser(t.Context(), rc, UserWrite{Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	current, err := p.RotateUserPassword(t.Context(), rc, "app", true)
	if err != nil || current == old {
		t.Fatal("direct dual-password rotation failed", err)
	}
	raw, err := secret(t.Context(), rc, accessCredentialsKey)
	if err != nil {
		t.Fatal(err)
	}
	var credentials map[string]accessCredential
	if err := json.Unmarshal(raw, &credentials); err != nil {
		t.Fatal(err)
	}
	if credentials["app"].Current != current || credentials["app"].Secondary != old {
		t.Fatal("retained credentials not sealed")
	}
	if _, err := p.proxyUsers(t.Context(), rc); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal("proxy deployment accepted retained credentials", err)
	}
	if err := p.DiscardOldPassword(t.Context(), rc, "app"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.proxyUsers(t.Context(), rc); err != nil {
		t.Fatal("discard did not unblock proxy publication", err)
	}
}
