// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type revisionAgentDialer struct {
	engine.AgentDialer
	member string
	client *agentapi.Client
}

func (a revisionAgentDialer) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if name == a.member {
		return a.client, nil
	}
	return a.AgentDialer.Dial(ctx, ref, name)
}

// CheckNativeCertificateReceiptPublishedRevision keeps private provider fixtures
// here while the external test constructs the real server handler without an
// engine -> server -> engine import cycle.
func CheckNativeCertificateReceiptPublishedRevision(t *testing.T, newInternalHandler func(*Core, *engine.Context) http.Handler) {
	for _, lostReply := range []bool{false, true} {
		name := "delayed-publication"
		if lostReply {
			name = "lost-reload-response"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, _ := setup(t, "ha")
			s := converge(t, p, rc)
			m := s.data()[1]
			memberName := m.Name
			oldOptions := append(json.RawMessage(nil), m.Options...)
			oldVersion, oldResources := m.Version, m.Resources
			st := rc.Store.(*store.Store)
			persist := func() {
				t.Helper()
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
					t.Fatal(err)
				}
			}
			persist()
			rc.Cluster.Spec.EngineOptions = parameterOptions(map[string]string{"log_min_duration_statement": "250"})
			if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
				t.Fatal(err)
			}
			reloadState := func() {
				t.Helper()
				stored, err := st.GetCluster(t.Context(), rc.Cluster.Ref())
				if err != nil {
					t.Fatal(err)
				}
				// Preserve the existing native fixture's object identity, but reload
				// every state byte from persistence rather than sharing provider state
				// with the internal HTTP configuration endpoint.
				*rc.Cluster = *stored
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				m = s.find(memberName)
			}
			reloadState()
			var api http.Handler
			startAPI := func() {
				t.Helper()
				api = newInternalHandler(p, rc)
			}
			startAPI()
			configServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				api.ServeHTTP(w, r)
			}))
			t.Cleanup(configServer.Close)
			token, err := st.GetAgentToken(t.Context(), rc.Cluster.Ref(), memberName)
			if err != nil {
				t.Fatal(err)
			}
			pull := func(ctx context.Context) (agentapi.MemberConfig, error) {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, configServer.URL+"/internal/agent/v1/config", nil)
				if err != nil {
					return agentapi.MemberConfig{}, err
				}
				request.Header.Set("Authorization", "Bearer "+token.Token)
				response, err := configServer.Client().Do(request)
				if err != nil {
					return agentapi.MemberConfig{}, err
				}
				defer response.Body.Close()
				if response.StatusCode != http.StatusOK {
					return agentapi.MemberConfig{}, fmt.Errorf("internal config HTTP %d", response.StatusCode)
				}
				var config agentapi.MemberConfig
				err = json.NewDecoder(response.Body).Decode(&config)
				return config, err
			}
			baseAgents := rc.Agents
			base, err := baseAgents.Dial(t.Context(), rc.Cluster.Ref(), memberName)
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			applied := m.ConfigVersion
			var appliedSpec pgspec.Spec
			staleRefusals := 0
			loseNext := false
			legacyAgent := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.URL.Path {
				case "/v1/status":
					// An existing HA agent has no reload-only capability, but it
					// already enforces monotonically published config versions.
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: applied})
				case "/v1/postgres/status":
					var status pgspec.StatusResponse
					if err := base.Engine(r.Context(), "postgres", "status", struct{}{}, &status); err != nil {
						http.Error(w, "native status unavailable", http.StatusServiceUnavailable)
						return
					}
					json.NewEncoder(w).Encode(status)
				case "/v1/reload":
					config, err := pull(r.Context())
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					if config.CertificateReloadOnly {
						t.Error("legacy HA agent received unsupported reload-only intent")
					}
					if config.ConfigVersion < applied {
						staleRefusals++
						http.Error(w, fmt.Sprintf("stale configuration version %d refused", config.ConfigVersion), http.StatusBadRequest)
						return
					}
					applied = config.ConfigVersion
					if err := json.Unmarshal(config.EngineSpec, &appliedSpec); err != nil {
						t.Error(err)
					}
					if loseNext {
						loseNext = false
						http.Error(w, "reload response lost", http.StatusServiceUnavailable)
						return
					}
					json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(legacyAgent.Close)
			client, err := (fakeAgents{url: legacyAgent.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: legacyAgent.Certificate().Raw})}).Dial(t.Context(), rc.Cluster.Ref(), memberName)
			if err != nil {
				t.Fatal(err)
			}
			rc.Agents = revisionAgentDialer{AgentDialer: baseAgents, member: memberName, client: client}
			caCert, err := secret(t.Context(), rc, "tls/ca.crt")
			if err != nil {
				t.Fatal(err)
			}
			caKey, err := secret(t.Context(), rc, "tls/ca.key")
			if err != nil {
				t.Fatal(err)
			}
			ca, err := pki.LoadCA(caCert, caKey)
			if err != nil {
				t.Fatal(err)
			}
			basePath := "tls/servers/" + memberName
			active, err := secret(t.Context(), rc, basePath+".crt")
			if err != nil {
				t.Fatal(err)
			}
			if err := putSecret(t.Context(), rc, basePath+".crt", invalidTransportLeaf(t, ca, active, true)); err != nil {
				t.Fatal(err)
			}
			if err := ensureCertificates(t.Context(), rc, s, false); err != nil {
				t.Fatal(err)
			}
			persist()
			// Sync's in-memory save precedes outer persistence. The first pull
			// can therefore see the existing revision, equal to the agent floor.
			if handled, err := p.recoverNativeCertificates(t.Context(), rc, s); !handled || err != nil {
				t.Fatal("identity publication failed", err)
			}
			receiptVersion := m.TLSReloadVersion
			persist()
			mu.Lock()
			loseNext = lostReply
			mu.Unlock()
			_, err = p.recoverNativeCertificates(t.Context(), rc, s)
			if (err != nil) != lostReply {
				t.Fatalf("unexpected reload response outcome: lost=%v err=%v", lostReply, err)
			}
			persist()
			mu.Lock()
			if applied != receiptVersion {
				t.Errorf("receipt revision was not accepted: applied=%d receipt=%d", applied, receiptVersion)
			}
			mu.Unlock()
			// Restart the CP with only persisted provider state, retaining the
			// running old agent and its monotonic accepted-version floor.
			reloadState()
			p = New(p.options)
			startAPI()
			if handled, err := p.recoverNativeCertificates(t.Context(), rc, s); !handled || err != nil {
				t.Fatal("receipt acknowledgement failed after CP restart", err)
			}
			if m.TLSReloadPending || m.TLSReloadVersion != 0 || m.ConfigVersion < receiptVersion || s.ConfigVersion <= m.ConfigVersion {
				t.Fatal("acknowledgement lost the member publication floor or skipped deferred desired rollout")
			}
			if !reflect.DeepEqual(m.Options, oldOptions) || m.Version != oldVersion || !reflect.DeepEqual(m.Resources, oldResources) {
				t.Fatal("identity acknowledgement falsely applied desired configuration")
			}
			persist()
			reloadState()
			config, err := pull(t.Context())
			if err != nil || config.ConfigVersion < receiptVersion {
				t.Fatalf("internal config API regressed below accepted receipt after restart: version=%d receipt=%d err=%v", config.ConfigVersion, receiptVersion, err)
			}
			for range 60 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal("ordinary rollout rejected its published revision", err)
				}
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				persist()
				config, err = pull(t.Context())
				if err != nil || config.ConfigVersion < receiptVersion {
					t.Fatal("ordinary rollout regressed the persisted internal config revision", err)
				}
				mu.Lock()
				desiredApplied := appliedSpec.Postgres != nil && appliedSpec.Postgres.Parameters["log_min_duration_statement"] == "250"
				refusals := staleRefusals
				mu.Unlock()
				if refusals != 0 {
					t.Fatal("store-backed configuration API caused a stale-version refusal")
				}
				if desiredApplied {
					return
				}
			}
			t.Fatal("ordinary rollout never applied the deferred desired options")
		})
	}
}
