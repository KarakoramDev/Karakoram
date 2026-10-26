// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	infrafake "github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
)

// The real agent's access routes require concrete pgx connections, not a
// replaceable executor. This fixture implements that HTTPS protocol boundary:
// leadership, native object identity, journal receipts and ownership fencing.
// It intentionally does not simulate Temporal/OpenBao runtimes or native SQL.
type bindingAgent struct {
	mu           sync.Mutex
	available    bool
	roles        map[string]pgspec.Role
	oids         map[string]uint32
	receipts     map[string]pgspec.AccessResult
	databases    map[string]pgspec.Database
	drops, keeps []string
	verifiers    []string
}

func (a *bindingAgent) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer binding-agent-token" {
		w.WriteHeader(401)
		return
	}
	if !a.available {
		w.WriteHeader(503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if r.URL.Path == "/v1/postgres/status" {
		_ = enc.Encode(pgspec.StatusResponse{Running: true, Role: "standalone"})
		return
	}
	var req pgspec.AccessRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(400)
		return
	}
	switch r.URL.Path {
	case "/v1/postgres/roles":
		switch req.Operation {
		case "capabilities":
			_ = enc.Encode(pgspec.AccessCapabilities{CredentialReceipts: true})
		case "list":
			out := []pgspec.Role{}
			for _, role := range a.roles {
				out = append(out, role)
			}
			_ = enc.Encode(out)
		case "journal":
			if receipt, ok := a.receipts[req.RequestID]; ok {
				_ = enc.Encode(receipt)
				return
			}
			if req.Role == nil || req.RequestID == "" || !req.Binding {
				w.WriteHeader(400)
				return
			}
			name := req.Role.Name
			if req.ExpectedRoleOID != 0 && req.ExpectedRoleOID != a.oids[name] {
				_ = enc.Encode(pgspec.AccessResult{Outcome: "rejected", Status: 409, Code: "role_identity_changed"})
				return
			}
			switch req.RoleOperation {
			case "create":
				if _, exists := a.roles[name]; exists {
					w.WriteHeader(409)
					return
				}
				a.oids[name] = uint32(17001 + len(a.oids))
				a.roles[name] = req.Role.Role
			case "update":
				if _, exists := a.roles[name]; !exists {
					w.WriteHeader(404)
					return
				}
				a.roles[name] = req.Role.Role
				if !req.Role.Login {
					a.keeps = append(a.keeps, name)
				}
			case "delete":
				delete(a.roles, name)
				a.drops = append(a.drops, name)
			default:
				w.WriteHeader(400)
				return
			}
			if req.Role.Password != nil {
				a.verifiers = append(a.verifiers, *req.Role.Password)
			}
			result := pgspec.AccessResult{Outcome: "applied", RoleOID: a.oids[name]}
			a.receipts[req.RequestID] = result
			_ = enc.Encode(result)
		default:
			w.WriteHeader(400)
		}
	case "/v1/postgres/databases":
		switch req.Operation {
		case "get":
			db, ok := a.databases[req.Name]
			if !ok {
				w.WriteHeader(404)
				return
			}
			_ = enc.Encode(db)
		case "create":
			if req.Database == nil {
				w.WriteHeader(400)
				return
			}
			db := *req.Database
			db.OwnerOID = a.oids[db.Owner]
			a.databases[db.Name] = db
			_ = enc.Encode(map[string]bool{"ok": true})
		case "public-owner":
			db, ok := a.databases[req.Database.Name]
			if !ok || db.Owner != req.Database.Owner {
				w.WriteHeader(409)
				return
			}
			_ = enc.Encode(map[string]bool{"ok": true})
		case "delete":
			delete(a.databases, req.Name)
			_ = enc.Encode(map[string]bool{"ok": true})
		default:
			w.WriteHeader(400)
		}
	default:
		w.WriteHeader(404)
	}
}

type bindingAgentDialer struct{ server *httptest.Server }

func (d bindingAgentDialer) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	if ref.Name != "pgsource" || member != "pgsourcep0" {
		return nil, engine.ErrUnavailable
	}
	u, _ := url.Parse(d.server.URL)
	return agentapi.NewClient(agentapi.ClientOptions{
		BaseURL: d.server.URL, Token: "binding-agent-token",
		CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: d.server.Certificate().Raw}),
		DialInstanceContext: func(ctx context.Context, _, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
		},
	})
}

func TestPostgresBindingsAcrossOpenBaoAndTemporal(t *testing.T) {
	base, _, token := testServer(t)
	ctx := t.Context()
	agent := &bindingAgent{roles: map[string]pgspec.Role{}, oids: map[string]uint32{}, receipts: map[string]pgspec.AccessResult{}, databases: map[string]pgspec.Database{}}
	upstream := httptest.NewTLSServer(agent)
	t.Cleanup(upstream.Close)
	// Match Run's forward-declared server resolver and immediate trigger pattern.
	var s *Server
	manager := pgbinding.New(pgbinding.Options{Store: base.Store,
		Source: func(ctx context.Context, ref domain.ClusterRef) (*engine.Context, pgbinding.Access, error) {
			return s.bindingSource(ctx, ref)
		},
		Trigger: func(ref domain.ClusterRef) {
			if s.Trigger != nil {
				s.Trigger.Trigger(ref)
			}
		},
	})
	providers := engineProviders(engineOptions{Store: base.Store, Bindings: manager})
	for _, p := range providers {
		if p.Name() != "temporal" && p.Name() != "openbao" {
			continue
		}
		// Inspect identity without exporting a production accessor just for tests.
		got := reflect.ValueOf(p).Elem().FieldByName("options").FieldByName("Bindings").Elem().Pointer()
		if got != reflect.ValueOf(manager).Pointer() {
			t.Fatalf("%s received a different manager", p.Name())
		}
	}
	registry, err := engine.NewRegistry(providers...)
	if err != nil {
		t.Fatal(err)
	}
	opts := base.Options
	opts.Registry, opts.Bindings = registry, manager
	opts.Context.Backend = infrafake.New(infrafake.Options{})
	opts.Context.Agents = bindingAgentDialer{upstream}
	s, err = New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if s.postgresBindings != manager {
		t.Fatal("server replaced shared manager")
	}
	apiServer := httptest.NewServer(s.Handler())
	t.Cleanup(apiServer.Close)
	var responses []string
	request := func(method, path, body string, want int) string {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, apiServer.URL+"/api/v1"+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := apiServer.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != want {
			t.Fatalf("%s %s: %d want %d: %s", method, path, resp.StatusCode, want, raw)
		}
		responses = append(responses, string(raw))
		return string(raw)
	}
	request("POST", "/clusters", `{"name":"pgsource","spec":{"provider":"postgres","topology":{"type":"single"},"tls":{"mode":"requireTLS"}}}`, 201)
	// Infrastructure deployment is outside this test. Seed the source's persisted
	// member identity, retaining its REST-created spec and source incarnation.
	source, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "pgsource"})
	if err != nil {
		t.Fatal(err)
	}
	source.Internal = json.RawMessage(`{"postgres":{"uid":"source-runtime","topology":"single","configVersion":1,"formed":true,"members":[{"name":"pgsourcep0","role":"data","index":0,"stage":"joined"}]}}`)
	if err := s.Store.UpdateClusterState(ctx, source); err != nil {
		t.Fatal(err)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: upstream.Certificate().Raw})
	if err := s.Store.SetClusterSecret(ctx, source.Ref(), "tls/ca.crt", ca); err != nil {
		t.Fatal(err)
	}
	request("POST", "/clusters", `{"name":"workflows","spec":{"provider":"temporal","engineOptions":{"persistence":{"postgres":{"cluster":"pgsource"}}}}}`, 201)
	request("POST", "/clusters", `{"name":"vault","spec":{"provider":"openbao","components":{"engine":{"replicas":1}},"engineOptions":{"storage":{"type":"postgresql","postgresql":{"cluster":"pgsource"}}}}}`, 201)
	syncConsumer := func(name string) {
		t.Helper()
		rc, p, err := s.context(ctx, domain.ClusterRef{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		if failure := s.providerContext(rc); failure != nil {
			t.Fatal(failure)
		}
		_, syncErr := p.Sync(ctx, rc)
		// OpenBao deliberately exposes a generic retry error on an unavailable
		// binding; Temporal records SourceUnavailable without failing reconciliation.
		if syncErr != nil && !strings.Contains(syncErr.Error(), "binding is unavailable") && syncErr != engine.ErrUnavailable {
			t.Fatalf("%s sync: %v", name, syncErr)
		}
		if err := s.Store.UpdateClusterState(ctx, rc.Cluster); err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		syncConsumer("workflows")
		syncConsumer("vault")
	}
	rows, err := s.Store.ListPostgresBindings(ctx, source.Ref())
	if err != nil || len(rows) != 2 {
		t.Fatalf("pending rows: %+v %v", rows, err)
	}
	for _, row := range rows {
		if row.State != "pending" {
			t.Fatalf("agent unavailable binding: %+v", row)
		}
	}
	request("GET", "/clusters/pgsource/postgres/bindings", "", 200)
	bao, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "vault"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(bao.Internal), `"bindingId":"pgb-`) {
		t.Fatal("OpenBao saved an ungranted binding identity")
	}
	rc, p, err := s.context(ctx, domain.ClusterRef{Name: "workflows"})
	if err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(ctx, rc)
	if err != nil {
		t.Fatal(err)
	}
	pending := false
	for _, condition := range status.Conditions {
		if condition.Type == "PostgreSQLBinding" {
			pending = condition.Status == "False" && condition.Reason == "SourceUnavailable"
		}
	}
	if !pending {
		t.Fatalf("unavailable source status: %+v", status)
	}
	request("DELETE", "/clusters/pgsource", "", 409)
	agent.mu.Lock()
	agent.available = true
	agent.mu.Unlock()
	// Stop at ready binding receipts, before consumer runtimes need deployment.
	for _, name := range []string{"workflows", "vault"} {
		syncConsumer(name)
	}
	rows, err = s.Store.ListPostgresBindings(ctx, source.Ref())
	if err != nil || len(rows) != 2 {
		t.Fatalf("ready rows: %+v %v", rows, err)
	}
	credentials := []string{}
	for _, row := range rows {
		if row.State != "ready" {
			t.Fatalf("binding did not reach ready: %+v", row)
		}
		conn, err := manager.Resolve(ctx, row.ID)
		if err != nil {
			t.Fatal(err)
		}
		password, err := manager.Password(ctx, conn.PasswordSecretRef)
		if err != nil || password == "" {
			t.Fatalf("missing sealed password: %v", err)
		}
		credentials = append(credentials, password, conn.PasswordSecretRef)
	}
	listed := request("GET", "/clusters/pgsource/postgres/bindings", "", 200)
	for _, name := range []string{"vault", "workflows"} {
		if !strings.Contains(listed, `"name":"`+name+`"`) {
			t.Fatalf("consumer missing from list: %s", listed)
		}
	}
	rotated := rows[0]
	request("POST", fmt.Sprintf("/clusters/pgsource/postgres/bindings/%s/rotate", rotated.ID), "", 200)
	after, err := s.Store.GetPostgresBinding(ctx, rotated.ID)
	if err != nil || after.CredentialGeneration != rotated.CredentialGeneration+1 {
		t.Fatalf("rotation generation: %+v %v", after, err)
	}
	connection, err := manager.Resolve(ctx, rotated.ID)
	if err != nil {
		t.Fatal(err)
	}
	password, err := manager.Password(ctx, connection.PasswordSecretRef)
	if err != nil || password == "" {
		t.Fatalf("missing rotated password: %v", err)
	}
	credentials = append(credentials, password)
	request("GET", "/clusters/workflows", "", 200)
	request("GET", "/clusters/vault", "", 200)
	request("GET", "/events", "", 200)
	// Temporal drops its owned databases and role; OpenBao retains its database
	// and disables login, matching keepVolumes rather than silently dropping data.
	for _, consumer := range []struct {
		name string
		keep bool
	}{{"workflows", false}, {"vault", true}} {
		request("DELETE", "/clusters/"+consumer.name+fmt.Sprintf("?keepVolumes=%t", consumer.keep), "", 202)
		for attempt := range 30 {
			rc, p, err := s.context(ctx, domain.ClusterRef{Name: consumer.name})
			if err != nil {
				t.Fatal(err)
			}
			if failure := s.providerContext(rc); failure != nil {
				t.Fatal(failure)
			}
			done, err := p.Cleanup(ctx, rc, rc.Cluster.DeleteOptions)
			if err != nil {
				t.Fatalf("%s cleanup: %v", consumer.name, err)
			}
			if err := s.Store.UpdateClusterState(ctx, rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if done {
				break
			}
			if attempt == 29 {
				t.Fatalf("%s cleanup did not finish", consumer.name)
			}
		}
		if !consumer.keep {
			request("DELETE", "/clusters/pgsource", "", 409)
		}
	}
	rows, err = s.Store.ListPostgresBindings(ctx, source.Ref())
	if err != nil || len(rows) != 0 {
		t.Fatalf("bindings not released: %+v %v", rows, err)
	}
	agent.mu.Lock()
	if len(agent.drops) != 1 || len(agent.keeps) != 1 || len(agent.databases) != 1 || agent.databases["openbao"].Name != "openbao" {
		t.Errorf("cleanup ownership: drops=%v keeps=%v databases=%v", agent.drops, agent.keeps, agent.databases)
	}
	agent.mu.Unlock()
	request("DELETE", "/clusters/pgsource", "", 202)
	request("GET", "/clusters/pgsource/postgres/bindings", "", 200)
	request("GET", "/events", "", 200)
	agent.mu.Lock()
	credentials = append(credentials, agent.verifiers...)
	agent.mu.Unlock()
	for _, raw := range responses {
		for _, secret := range credentials {
			if strings.Contains(raw, secret) {
				t.Fatal("credential material leaked in HTTP response or event")
			}
		}
		for _, forbidden := range []string{`"password":`, `"passwordSecretRef":`, `SCRAM-SHA-256$`} {
			if strings.Contains(raw, forbidden) {
				t.Fatalf("credential field leaked: %s", forbidden)
			}
		}
	}
}
