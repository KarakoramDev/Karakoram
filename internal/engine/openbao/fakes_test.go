// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type fakeBao struct {
	initialized, sealed, active, running, voter, healthy bool
	progress                                             int
	config                                               int64
	generation                                           int64
	version                                              string
	unreachable                                          bool
	removed                                              bool
	receipt                                              *baospec.Envelope
	initCalls, initRequests, unseals                     int
}
type fakeAgents struct {
	mu                 sync.Mutex
	requestMu          sync.Mutex
	t                  *testing.T
	p                  *Provider
	rc                 *engine.Context
	log                *callLog
	members            map[string]*fakeBao
	servers            map[string]*httptest.Server
	nativeServers      map[string]*httptest.Server
	recoveryServers    map[string]*httptest.Server
	shares             []string
	root, management   string
	tokenValid         bool
	generated          int
	revoked            int
	loseResponse       bool
	initWithoutReceipt bool
	holdVoter          string
	rootProgress       int
	rootOTP            string
	beforeInit         func(string)
	policies           map[string]bool
}

func (a *fakeAgents) member(name string) *fakeBao {
	b := a.members[name]
	if b == nil {
		b = &fakeBao{sealed: true}
		a.members[name] = b
	}
	return b
}
func (a *fakeAgents) redeploy(name string) {
	a.requestMu.Lock()
	defer a.requestMu.Unlock()
	b := a.member(name)
	b.running = true
	b.sealed = true
	b.active = false
	b.config = 0
	b.version = a.rc.Cluster.Spec.Version
}
func (a *fakeAgents) leaderExists() bool {
	for _, b := range a.members {
		if b.active && !b.sealed {
			return true
		}
	}
	return false
}
func (a *fakeAgents) Dial(_ context.Context, _ domain.ClusterRef, name string) (*agentapi.Client, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	server := a.servers[name]
	if server == nil {
		server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			a.requestMu.Lock()
			defer a.requestMu.Unlock()
			b := a.member(name)
			if r.Header.Get("Authorization") != "Bearer test-token" {
				http.Error(w, "unauthenticated", http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/v1/status":
				cfg, err := a.p.MemberConfig(r.Context(), a.rc, name)
				if err != nil {
					http.Error(w, "config unavailable", http.StatusServiceUnavailable)
					return
				}
				var spec baospec.MemberSpec
				_ = json.Unmarshal(cfg.EngineSpec, &spec)
				if b.generation != 0 && b.generation != spec.ProcessGeneration {
					for otherName, other := range a.members {
						if otherName != name && other.voter && other.sealed && !other.removed {
							a.t.Errorf("concurrent restart: %s while %s sealed", name, otherName)
						}
					}
					b.sealed = true
					b.active = false
					b.healthy = false
					a.log.add("restart:" + name)
				}
				b.generation = spec.ProcessGeneration
				b.config = cfg.ConfigVersion
				state := "stopped"
				if b.running {
					state = "running"
				}
				_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: b.config, EngineVersion: "2.7.1", Volume: &agentapi.VolumeStatus{Identity: *cfg.Volume, Verified: true}, Processes: map[string]agentapi.ProcessStatus{"openbao": {State: state}}})
			case "/v1/reload":
				b.config = 0
				_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
			case "/v1/maintenance":
				_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
			case "/v1/processes/openbao/stop":
				b.running = false
				b.active = false
				b.sealed = true
				a.log.add("agent:" + name + ":stop")
				_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
			case "/v1/processes/openbao/start":
				b.running = true
				_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
			case "/v1/openbao/status":
				if b.unreachable {
					http.Error(w, "native process unreachable", http.StatusServiceUnavailable)
					return
				}
				_ = json.NewEncoder(w).Encode(baospec.StatusResponse{NodeID: name, Version: "2.7.1", Initialized: b.initialized, Sealed: b.sealed, Active: b.active, Standby: b.initialized && !b.sealed && !b.active})
			case "/v1/openbao/init":
				b.initRequests++
				if a.beforeInit != nil {
					a.beforeInit(name)
				}
				var req baospec.InitRequest
				if json.NewDecoder(r.Body).Decode(&req) != nil {
					http.Error(w, "bad request", 400)
					return
				}
				if b.receipt != nil {
					_ = json.NewEncoder(w).Encode(baospec.InitResponse{Envelope: *b.receipt})
					return
				}
				if b.initialized || a.initWithoutReceipt {
					b.initialized = true
					http.Error(w, baospec.ErrorInitializedWithoutReceipt, http.StatusConflict)
					return
				}
				b.initCalls++
				b.initialized = true
				a.shares = nil
				count := req.SecretShares
				if req.RecoveryShares > 0 {
					count = req.RecoveryShares
				}
				for i := range count {
					a.shares = append(a.shares, base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("secret-share-%d-unique-material", i))))
				}
				a.root = "initial-root-secret-token"
				material := baospec.InitMaterial{KeysBase64: a.shares, RootToken: a.root}
				if req.RecoveryShares > 0 {
					material.KeysBase64 = nil
					material.RecoveryKeysBase64 = a.shares
					b.sealed = false
					b.active = true
					b.healthy = true
					b.voter = true
				}
				plain, _ := json.Marshal(material)
				env, err := baospec.SealReceipt(rand.Reader, req.RecipientPublicKey, a.rc.Cluster.Name, name, req.ReceiptID, plain)
				if err != nil {
					a.t.Error(err)
					http.Error(w, "receipt failed", 500)
					return
				}
				b.receipt = &env
				if a.loseResponse {
					a.loseResponse = false
					http.Error(w, "lost response", http.StatusServiceUnavailable)
					return
				}
				_ = json.NewEncoder(w).Encode(baospec.InitResponse{Envelope: env})
			case "/v1/openbao/init-ack":
				var req baospec.InitAckRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				if b.receipt != nil && b.receipt.ReceiptID != req.ReceiptID {
					http.Error(w, "wrong receipt", http.StatusConflict)
					return
				}
				b.receipt = nil
				_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
			default:
				http.NotFound(w, r)
			}
		}))
		a.servers[name] = server
		a.t.Cleanup(server.Close)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: ca})
}
func (a *fakeAgents) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	name, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	server := a.nativeServers[name]
	if server == nil {
		cert, err := memberCertificate(ctx, a.rc, "tls/members/"+name+".pem")
		if err != nil {
			a.mu.Unlock()
			return nil, err
		}
		pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
		if err != nil {
			a.mu.Unlock()
			return nil, err
		}
		server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			a.requestMu.Lock()
			defer a.requestMu.Unlock()
			a.serveNative(name, w, r)
		}))
		server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
		server.StartTLS()
		a.nativeServers[name] = server
		a.t.Cleanup(server.Close)
	}
	if port == "8202" {
		if a.recoveryServers == nil {
			a.recoveryServers = map[string]*httptest.Server{}
		}
		recovery := a.recoveryServers[name]
		if recovery == nil {
			cert, err := memberCertificate(ctx, a.rc, "tls/members/"+name+".pem")
			if err != nil {
				a.mu.Unlock()
				return nil, err
			}
			pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
			if err != nil {
				a.mu.Unlock()
				return nil, err
			}
			ca, err := memberCertificate(ctx, a.rc, secretRecoveryCA)
			if err != nil {
				a.mu.Unlock()
				return nil, err
			}
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM([]byte(ca.CertPEM))
			recovery = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Delegate to the current handler, including focused snapshot
				// protocol wrappers installed after initial engine formation.
				a.nativeServers[name].Config.Handler.ServeHTTP(w, r)
			}))
			recovery.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS12}
			recovery.StartTLS()
			a.recoveryServers[name] = recovery
			a.t.Cleanup(recovery.Close)
		}
		server = recovery
	}
	addr := server.Listener.Addr().String()
	a.mu.Unlock()
	return (&net.Dialer{}).DialContext(ctx, network, addr)
}
func (a *fakeAgents) serveNative(name string, w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/v1/sys/generate-root/") && (r.TLS == nil || len(r.TLS.VerifiedChains) == 0) {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	b := a.member(name)
	if b.unreachable {
		http.Error(w, "unreachable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	send := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	seal := func() map[string]any {
		return map[string]any{"initialized": b.initialized || a.leaderExists(), "sealed": b.sealed, "t": 3, "n": 5, "progress": b.progress, "version": b.version}
	}
	switch r.URL.Path {
	case "/v1/sys/seal-status":
		if a.leaderExists() {
			b.initialized = true
		}
		send(seal())
		return
	case "/v1/sys/leader":
		send(map[string]any{"is_self": b.active && !b.sealed, "ha_enabled": true, "leader_address": "https://demom0:8200"})
		return
	case "/v1/sys/health":
		send(map[string]any{"initialized": b.initialized, "sealed": b.sealed, "standby": !b.active, "version": "2.7.1"})
		return
	case "/v1/sys/unseal":
		var req struct {
			Key   string `json:"key"`
			Reset bool   `json:"reset"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Reset {
			b.progress = 0
			send(seal())
			return
		}
		found := false
		for _, share := range a.shares {
			found = found || share == req.Key
		}
		if !found {
			http.Error(w, "wrong share", 400)
			return
		}
		b.unseals++
		b.progress++
		material, err := readUnseal(r.Context(), a.rc)
		if err != nil {
			http.Error(w, "missing material", http.StatusServiceUnavailable)
			return
		}
		if b.progress >= material.Threshold {
			b.sealed = false
			b.initialized = true
			b.active = !a.leaderExists()
			b.voter = true
			b.healthy = name != a.holdVoter
			b.progress = 0
			a.log.add("unsealed:" + name)
		}
		send(seal())
		return
	case "/v1/sys/generate-root/attempt":
		if r.Method == http.MethodDelete {
			a.rootProgress = 0
			w.WriteHeader(204)
			return
		}
		a.generated++
		a.rootOTP = "01234567890123456789012345678901"
		send(map[string]any{"nonce": "root-nonce", "otp": a.rootOTP, "required": 3})
		return
	case "/v1/sys/generate-root/update":
		var req struct {
			Key string `json:"key"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		found := false
		for _, s := range a.shares {
			found = found || s == req.Key
		}
		if !found {
			http.Error(w, "bad share", 400)
			return
		}
		a.rootProgress++
		if a.rootProgress < 3 {
			send(map[string]any{"complete": false})
			return
		}
		root := strings.Repeat("r", len(a.rootOTP))
		encoded := []byte(root)
		for i := range encoded {
			encoded[i] ^= a.rootOTP[i]
		}
		a.root = root
		send(map[string]any{"complete": true, "encoded_token": base64.RawStdEncoding.EncodeToString(encoded)})
		return
	}
	token := r.Header.Get("X-Vault-Token")
	isRoot := token == a.root && token != ""
	if !isRoot && (!a.tokenValid || token != a.management) {
		http.Error(w, "permission denied", http.StatusForbidden)
		return
	}
	if r.URL.Path == "/v1/sys/policies/acl" && r.Method == "LIST" {
		keys := []string{"default", "root"}
		for policy := range a.policies {
			keys = append(keys, policy)
		}
		send(map[string]any{"data": map[string]any{"keys": keys}})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/sys/policies/acl/") {
		policy := strings.TrimPrefix(r.URL.Path, "/v1/sys/policies/acl/")
		switch r.Method {
		case http.MethodPut:
			if a.policies == nil {
				a.policies = map[string]bool{}
			}
			a.policies[policy] = true
		case http.MethodDelete:
			delete(a.policies, policy)
		default:
			http.Error(w, "unsupported policy method", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch r.URL.Path {
	case "/v1/sys/step-down":
		b.active = false
		for _, m := range ordered(state(a.t, a.rc)) {
			other := a.member(m.Name)
			if m.Name != name && other.voter && !other.sealed && !other.unreachable && !other.removed {
				other.active = true
				break
			}
		}
		a.log.add("step-down:" + name)
		w.WriteHeader(204)
	case "/v1/sys/storage/raft/remove-peer":
		var req struct {
			ID string `json:"server_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		a.member(req.ID).removed = true
		a.log.add("remove-peer:" + req.ID)
		w.WriteHeader(204)
	case "/v1/auth/token/lookup-self":
		send(map[string]any{"data": map[string]any{"ttl": 86400}})
	case "/v1/auth/token/renew-self":
		send(map[string]any{})
	case "/v1/auth/token/create-orphan":
		if !isRoot {
			http.Error(w, "root needed", http.StatusForbidden)
			return
		}
		a.management = fmt.Sprintf("management-token-%d", a.generated)
		a.tokenValid = true
		send(map[string]any{"auth": map[string]any{"client_token": a.management}})
	case "/v1/auth/token/revoke-self":
		if isRoot {
			a.revoked++
		}
		w.WriteHeader(204)
	case "/v1/sys/storage/raft/autopilot/configuration":
		w.WriteHeader(204)
	case "/v1/sys/storage/raft/configuration":
		servers := []map[string]any{}
		for n, m := range a.members {
			if m.voter && !m.removed {
				servers = append(servers, map[string]any{"node_id": n, "voter": m.voter, "leader": m.active})
			}
		}
		send(map[string]any{"data": map[string]any{"config": map[string]any{"servers": servers}}})
	case "/v1/sys/storage/raft/autopilot/state":
		servers := map[string]any{}
		voters := []string{}
		healthy := true
		for n, m := range a.members {
			if m.voter && !m.removed {
				status := "voter"
				if m.active {
					status = "leader"
				}
				servers[n] = map[string]any{"name": n, "id": n, "healthy": m.healthy && !m.sealed && !m.unreachable, "status": status}
				voters = append(voters, n)
				healthy = healthy && m.healthy && !m.sealed && !m.unreachable
			}
		}
		send(map[string]any{"data": map[string]any{"healthy": healthy, "servers": servers, "voters": voters}})
	default:
		http.NotFound(w, r)
	}
}
func setup(t *testing.T) (*Provider, *engine.Context, *fakeInfra, *fakeAgents) {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	p := New(ProviderOptions{Store: st, ControlPlaneURL: "https://control-plane.example.com"})
	c := &domain.Cluster{Name: "demo", Generation: 1}
	p.ApplyDefaults(&c.Spec)
	if err = st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	log := &callLog{}
	f := &fakeInfra{services: map[string]*infra.Service{}, done: true, subdomains: map[string]bool{}, failures: map[string]error{}, states: map[string]infra.OpState{}, log: log}
	rc := &engine.Context{Cluster: c, Store: st, Secrets: st, Backend: f, Artifact: fakeArtifact{}, Clock: &testClock{at: time.Now().UTC()}}
	a := &fakeAgents{t: t, p: p, rc: rc, log: log, members: map[string]*fakeBao{}, servers: map[string]*httptest.Server{}, nativeServers: map[string]*httptest.Server{}}
	rc.Agents = a
	rc.Dialer = a
	f.agents = a
	return p, rc, f, a
}
func state(t *testing.T, rc *engine.Context) *internalState {
	t.Helper()
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func syncUntil(t *testing.T, p *Provider, rc *engine.Context, done func(*internalState) bool) {
	t.Helper()
	for range 200 {
		result, err := p.Sync(t.Context(), rc)
		if err != nil {
			t.Fatalf("sync: %v\n%s", err, rc.Cluster.Internal)
		}
		if result.ConvergenceError != nil {
			t.Fatal(result.ConvergenceError)
		}
		if done(state(t, rc)) {
			return
		}
	}
	t.Fatalf("did not converge: %s", rc.Cluster.Internal)
}
func ready(t *testing.T, p *Provider, rc *engine.Context) {
	t.Helper()
	syncUntil(t, p, rc, func(s *internalState) bool {
		return s.Phase == "ready" && rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation
	})
}
