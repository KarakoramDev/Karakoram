// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestDefaultsPresetsAndDependencies(t *testing.T) {
	p := New(postgres.Options{})
	spec := domain.ClusterSpec{Components: domain.Components{Extra: map[string]domain.ComponentSpec{"auth": {Replicas: 0}}}}
	p.ApplyDefaults(&spec)
	if spec.Provider != "supabase" || spec.Components.Extra["auth"].Replicas != 0 || spec.Components.Extra["rest"].Replicas != 1 {
		t.Fatal("absent and explicitly disabled components were conflated")
	}
	info := p.Describe()
	if len(info.Presets) != 3 || len(info.Versions) == 0 {
		t.Fatal("incorrect catalog")
	}
	for _, version := range info.Versions {
		if version.Major != "17" {
			t.Fatal("Supabase advertises a non-17 PostgreSQL version")
		}
	}
	for _, preset := range info.Presets {
		extras := preset.Components
		delete(extras.Extra, "proxy")
		if err := stackComponents.Validate(extras); err != nil {
			t.Fatalf("preset %s: %v", preset.Name, err)
		}
	}
	spec = domain.ClusterSpec{}
	p.ApplyDefaults(&spec)
	image := spec.Components.Extra["imgproxy"]
	image.Replicas = 1
	spec.Components.Extra["imgproxy"] = image
	delete(spec.Components.Extra, "proxy")
	if err := stackComponents.Validate(spec.Components); err == nil {
		t.Fatal("imgproxy admitted without storage")
	}
}

func TestAnalyticsMinimumMemory(t *testing.T) {
	for _, test := range []struct {
		name     string
		replicas int
		minRAM   float64
		maxRAM   float64
		wantErr  bool
	}{
		{"too-small", 1, 0.5, 0.5, true},
		{"unreserved-headroom", 1, 0.5, 1, true},
		{"minimum", 1, 1, 1, false},
		{"larger", 1, 2, 2, false},
		{"disabled", 0, 0.5, 0.5, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, rc, _ := setupProvider(t)
			component := rc.Cluster.Spec.Components.Extra["analytics"]
			component.Replicas = test.replicas
			component.Resources.MinRAM, component.Resources.MaxRAM = test.minRAM, test.maxRAM
			rc.Cluster.Spec.Components.Extra["analytics"] = component
			err := p.Validate(context.Background(), nil, rc.Cluster)
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "analytics requires at least 1 GiB minRam") {
					t.Fatalf("expected Logflare startup memory rejection, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestOptionsRejectUnknownAndUnsafeInput(t *testing.T) {
	for _, raw := range []string{`null`, `{"unknown":true}`, `{"auth":{"smtp":{"password":"secret"}}}`, `{"siteUrl":"ftp://example.com"}`, `{"siteUrl":"https://user:pass@example.com"}`, `{"additionalRedirectUrls":["javascript:alert(1)"]}`, `{"extensions":["untrusted"]}`, `{"parameters":{"wal_level":"minimal"}}`, `{"rest":{"schemas":["public,private"]}}`, `{"storage":{"fileSizeLimit":-1}}`} {
		if _, err := ParseOptions(json.RawMessage(raw)); err == nil {
			t.Errorf("admitted %s", raw)
		}
	}
	options, err := ParseOptions(json.RawMessage(`{"auth":{"autoconfirm":true},"siteUrl":"https://app.example.com","extensions":["pg_cron"]}`))
	if err != nil || !options.Auth.Autoconfirm || options.Auth.JWTExpiry != 3600 {
		t.Fatal("valid options rejected")
	}
}

type testBackend struct {
	infra.Backend
	services     map[string]*infra.Service
	deployed     map[string]bool
	lookupErrors map[string]error
	stopped      map[string]bool
	gatewayHosts map[string]int
}

func (*testBackend) Name() string { return "test" }
func (*testBackend) Capabilities() infra.Capabilities {
	return infra.Capabilities{HorizontalScaling: true, Placement: true, PublicTCP: true}
}
func (*testBackend) Layout() infra.Layout {
	return infra.Layout{AppDir: "/srv/app", VolumeRoot: "/srv/data", User: "app", PrivateAddressFamily: infra.AddressFamilyIPv4}
}
func (b *testBackend) GetService(_ context.Context, name string) (*infra.Service, error) {
	if err := b.lookupErrors[name]; err != nil {
		return nil, err
	}
	return b.services[name], nil
}
func (b *testBackend) SetEnvVars(_ context.Context, name string, vars map[string]string) error {
	for k, v := range vars {
		b.services[name].Ownership[k] = v
	}
	return nil
}
func (b *testBackend) ImportServices(_ context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	ids := map[string]string{}
	for _, spec := range specs {
		ownership := map[string]string{}
		for k, v := range spec.Env {
			ownership[k] = v
		}
		for k, v := range spec.Vars {
			ownership[k] = v
		}
		ids[spec.Hostname] = spec.Hostname + "-id"
		b.services[spec.Hostname] = &infra.Service{ID: ids[spec.Hostname], Hostname: spec.Hostname, Kind: spec.Kind, Ownership: ownership, Resources: spec.Resources}
	}
	return infra.Op{Services: ids}, nil
}
func (b *testBackend) Deploy(_ context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if err := d.Validate(); err != nil {
		return infra.Op{}, err
	}
	_, err := io.Copy(io.Discard, d.Artifact.Reader)
	b.deployed[name] = true
	return infra.Op{}, err
}
func (*testBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return infra.OpState{Done: true}, nil
}
func (b *testBackend) Containers(_ context.Context, name string) ([]infra.Container, error) {
	if !b.deployed[name] {
		return nil, nil
	}
	// The fixture's TLS agent server models one live instance per runtime.
	// ForInstance retains its dynamic port and selects the member by bearer token.
	return []infra.Container{{ID: name + "-instance", Hostname: "127.0.0.1", IPv4: "127.0.0.1", Status: infra.ContainerRunning}}, nil
}
func (*testBackend) DeletePublicPorts(context.Context, string) error { return nil }
func (b *testBackend) DeleteServiceByID(_ context.Context, name, id string) (infra.Op, error) {
	if s := b.services[name]; s != nil && s.ID != id {
		return infra.Op{}, infra.ErrConflict
	}
	delete(b.services, name)
	return infra.Op{}, nil
}
func (b *testBackend) SetResources(_ context.Context, name string, r domain.Resources) (infra.Op, error) {
	b.services[name].Resources = r
	return infra.Op{}, nil
}
func (b *testBackend) SetContainerCount(_ context.Context, name string, min, max int) (infra.Op, error) {
	b.services[name].MinContainers = min
	b.services[name].MaxContainers = max
	return infra.Op{}, nil
}

type testArtifact struct{}

func (testArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("binary")), nil
}

type testAgents struct {
	url string
	ca  []byte
}

func (a testAgents) Dial(_ context.Context, _ domain.ClusterRef, name string) (*agentapi.Client, error) {
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.url, CAPEM: a.ca, Token: name})
}
func setupProvider(t *testing.T) (*Provider, *engine.Context, *testBackend) {
	t.Helper()
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	p := New(postgres.Options{Store: st})
	cluster := &domain.Cluster{Name: "demo", Namespace: "default", Generation: 1}
	p.ApplyDefaults(&cluster.Spec)
	backend := &testBackend{services: map[string]*infra.Service{}, deployed: map[string]bool{}}
	rc := &engine.Context{Cluster: cluster, Secrets: st, Store: st, Backend: backend, Artifact: testArtifact{}}
	backend.gatewayHosts = map[string]int{}
	for _, name := range sbspec.GatewayUpstreams {
		backend.gatewayHosts[name] = 1
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		config, err := p.MemberConfig(r.Context(), rc, name)
		switch r.URL.Path {
		case "/v1/status":
			if err != nil {
				http.Error(w, "config unavailable", http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: config.ConfigVersion})
		case "/v1/postgres/status":
			role := "standalone"
			if strings.HasSuffix(name, "px") {
				role = "proxy"
			}
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: !backend.stopped[name], Role: role, Version: cluster.Spec.Version})
		case "/v1/supabase/status":
			status := sbspec.Status{Ready: true}
			if strings.HasSuffix(name, codes["gateway"]) {
				status.GatewayUpstreamHosts = backend.gatewayHosts
			}
			json.NewEncoder(w).Encode(status)
		case "/v1/logs":
			_ = json.NewEncoder(w).Encode(agentapi.LogsResponse{
				Lines: []agentapi.LogLine{{Process: r.URL.Query().Get("process"), Stream: "stdout", Message: "component ready"}},
			})
		default:
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		}
	}))
	t.Cleanup(server.Close)
	rc.Agents = testAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	return p, rc, backend
}
func convergeProvider(t *testing.T, p *Provider, rc *engine.Context) {
	t.Helper()
	for range 150 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
		status, err := p.Status(context.Background(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase == domain.PhaseReady && rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
			rc.Cluster.Status = status
			return
		}
	}
	t.Fatal("provider did not converge")
}
func TestPgsodiumDoesNotPreloadWithoutRootKeyManagement(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"extensions":["pgsodium","pg_cron","pg_net","pgaudit","pg_stat_monitor"]}`)
	options, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil || len(options.Extensions) != 5 {
		t.Fatalf("plain pgsodium extension must remain available: %v", err)
	}
	config, err := p.MemberConfig(context.Background(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	preloads := map[string]bool{}
	for _, extension := range spec.Postgres.SharedPreloadLibraries {
		preloads[extension] = true
	}
	if preloads["pgsodium"] {
		t.Fatal("pgsodium root key management must not be enabled")
	}
	for _, extension := range []string{"pg_stat_statements", "pg_cron", "pgaudit", "pg_stat_monitor"} {
		if !preloads[extension] {
			t.Fatalf("required preload %s missing", extension)
		}
	}
	bundle := nativeartifacts.Artifact{
		Extensions: []string{"pgsodium", "pg_net", "unflagged"},
		ExtensionFlags: map[string]nativeartifacts.ExtensionFlags{
			"pg_net":        {Preload: true},
			"pgsodium":      {Preload: false, UnavailableFeatures: []string{"server-key-management", "key-id", "tce"}},
			"not-qualified": {Preload: true},
		},
	}
	selected := []string{"pgsodium", "pg_net", "unflagged", "not-qualified", "pg_net"}
	qualified := selectedExtensionPreloads(selected, bundle)
	if len(qualified) != 1 || qualified[0] != "pg_net" {
		t.Fatal("preloads must follow qualified flags, excluding crypto-only pgsodium and duplicates")
	}
	if got := selectedExtensionPreloads(selected, nativeartifacts.Artifact{}); len(got) != 0 {
		t.Fatal("absent bundle metadata must not enable built extension preloads")
	}
}

func TestComponentLifecycleAndSecretIsolation(t *testing.T) {
	p, rc, b := setupProvider(t)
	convergeProvider(t, p, rc)
	if len(b.services) != 6 {
		t.Fatalf("wanted data+volume+proxy+3 components, got %d", len(b.services))
	}
	c, err := p.Credentials(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	config, err := p.MemberConfig(context.Background(), rc, "demoau")
	if err != nil {
		t.Fatal(err)
	}
	var spec sbspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.DataPeers[0] != "demop0" || !spec.DatabaseTLS || spec.Auth.JWTExpiry != 3600 || len(spec.Upstreams) != 3 || spec.DBRoles["supabase_auth_admin"] == "" {
		t.Fatal("incomplete Supabase member payload")
	}
	if len(spec.DBRoles) != 1 || spec.DBRoles["postgres"] != "" || spec.DBRoles["authenticator"] != "" || spec.ServiceRoleKey != "" {
		t.Fatal("Auth received unrelated database or API credentials")
	}
	for _, secret := range []string{c.JWTSecret, c.ServiceRoleKey, c.DashboardPassword, spec.DBRoles["supabase_auth_admin"]} {
		if strings.Contains(string(rc.Cluster.Internal), secret) {
			t.Fatal("credential leaked to Internal")
		}
	}
	for _, m := range rc.Cluster.Status.Members {
		if _, ok := codes[m.Role]; ok && (m.Version != "" || m.ComponentVersion == "") {
			t.Fatal("component pollutes aggregate database version")
		}
	}
	component := rc.Cluster.Spec.Components.Extra["rest"]
	component.Replicas = 2
	rc.Cluster.Spec.Components.Extra["rest"] = component
	convergeProvider(t, p, rc)
	if b.services["demors"].MaxContainers != 2 {
		t.Fatal("component did not scale")
	}
	component.Replicas = 0
	rc.Cluster.Spec.Components.Extra["rest"] = component
	convergeProvider(t, p, rc)
	if b.services["demors"] != nil {
		t.Fatal("disabled component still deployed")
	}
}
func TestFunctionsLimitsAndConcurrentUpdates(t *testing.T) {
	p, rc, _ := setupProvider(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for _, name := range []string{"first", "second"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.PutFunction(ctx, rc, name, map[string]string{"index.ts": "export default {}"}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	names, err := p.ListFunctions(ctx, rc)
	if err != nil || len(names) != 2 {
		t.Fatal("concurrent function update lost")
	}
	for _, files := range []map[string]string{{"../index.ts": "bad"}, {"index.ts": strings.Repeat("x", (1<<20)+1)}} {
		if err := p.PutFunction(ctx, rc, "unsafe", files); err == nil {
			t.Fatal("unsafe source admitted")
		}
	}
	if err = p.DeleteFunction(ctx, rc, "first"); err != nil {
		t.Fatal(err)
	}
	if _, err = p.GetFunction(ctx, rc, "first"); err == nil {
		t.Fatal("deleted source still available")
	}
}
func TestRotationRetainsIndependentSecrets(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	before, err := loadCredentials(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "rotation", Kind: "rotate-jwt-secret"}
	done := false
	for range 50 {
		result, err := p.RunOperation(context.Background(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		if result.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("key rotation did not finish")
	}
	after, err := loadCredentials(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if after.Keys.JWTSecret == before.Keys.JWTSecret || after.Keys.DashboardPassword != before.Keys.DashboardPassword || after.Keys.SecretKeyBase != before.Keys.SecretKeyBase || after.Roles["supabase_admin"] != before.Roles["supabase_admin"] {
		t.Fatal("rotation changed the wrong credentials")
	}
	if _, err = p.RunOperation(context.Background(), rc, op); err != nil {
		t.Fatal(err)
	}
	again, _ := loadCredentials(context.Background(), rc)
	if again.Keys.JWTSecret != after.Keys.JWTSecret {
		t.Fatal("retry regenerated keys")
	}
}

func TestStorageSMTPAndFunctionRendering(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	ctx := context.Background()
	storage := &domain.BackupStorage{
		Name: "objects", Namespace: "default", Type: "s3",
		EndpointURL: "https://objects.example.com", Bucket: "registered-bucket", Region: "region-1",
		ForcePathStyle: true, VerifyTLS: true,
		Credentials: domain.StorageCredentials{AccessKeyID: "private-access", SecretAccessKey: "private-secret"},
	}
	if err := rc.Store.(*store.Store).CreateBackupStorage(ctx, storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{
		"storage":{"backendStorage":"objects","fileSizeLimit":1024},
		"siteUrl":"https://app.example.com","additionalRedirectUrls":["https://app.example.com/callback"],
		"auth":{"smtp":{"host":"mail.example.com","port":587,"user":"sender"}},
		"functions":{"verifyJwt":false}
	}`)
	if err := p.SetSMTPPassword(ctx, rc, "private-mail"); err != nil {
		t.Fatal(err)
	}
	if err := p.PutFunction(ctx, rc, "hello", map[string]string{"index.ts": "private function source"}); err != nil {
		t.Fatal(err)
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := p.desiredSpec(ctx, rc, s.Members["auth"])
	if err != nil {
		t.Fatal(err)
	}
	if sp.Storage != (sbspec.Storage{}) || sp.Auth.SMTP.Password != "private-mail" ||
		sp.Auth.SMTP.Port != 587 || sp.SiteURL != "https://app.example.com" || sp.VerifyJWT {
		t.Fatal("Auth projection lost mail settings or retained S3 credentials")
	}
	storageSpec, err := p.desiredSpec(ctx, rc, &componentMember{Component: "storage"})
	if err != nil {
		t.Fatal(err)
	}
	if storageSpec.Storage.Bucket != storage.Bucket || storageSpec.Storage.TenantID != "default-demo" ||
		storageSpec.Storage.SecretKey != "private-secret" || storageSpec.Auth.SMTP.Password != "" ||
		len(storageSpec.DBRoles) != 1 || storageSpec.DBRoles["supabase_storage_admin"] == "" {
		t.Fatal("Storage projection lost its credentials or retained unrelated secrets")
	}
	gateway := s.Members["gateway"]
	gatewaySpec, err := p.desiredSpec(ctx, rc, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SetSMTPPassword(ctx, rc, "changed-mail"); err != nil {
		t.Fatal(err)
	}
	updatedGateway, err := p.desiredSpec(ctx, rc, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if updatedGateway.RestartToken != gatewaySpec.RestartToken {
		t.Fatal("unrelated SMTP secret changed gateway digest")
	}
	if err := p.publishConfig(ctx, rc, gateway, updatedGateway); err != nil {
		t.Fatal(err)
	}
	published, err := p.MemberConfig(ctx, rc, gateway.Name)
	if err != nil {
		t.Fatal(err)
	}
	var cached sbspec.Spec
	if err := json.Unmarshal(published.EngineSpec, &cached); err != nil {
		t.Fatal(err)
	}
	if len(cached.DBRoles) != 0 || cached.Auth.SMTP.Password != "" || cached.Storage.SecretKey != "" || cached.JWTSecret != "" || cached.PatroniPassword != "" {
		t.Fatal("published member cache contains unrelated secrets")
	}
	for _, private := range []string{"private-mail", "private function source", "private-secret", "private-access"} {
		if strings.Contains(string(rc.Cluster.Internal), private) {
			t.Fatal("sealed content leaked into Internal")
		}
	}
	rc.Cluster.Spec.TLS.Mode = "disabled"
	sp, err = p.desiredSpec(ctx, rc, s.Members["auth"])
	if err != nil || sp.DatabaseTLS {
		t.Fatal("disabled database TLS was ignored")
	}
}

func TestSyncDoesNotObserveGenerationBeforeComponents(t *testing.T) {
	p, rc, _ := setupProvider(t)
	ctx := context.Background()
	for range 100 {
		if _, err := p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
		s, err := loadState(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if len(s.Members) > 0 {
			if rc.Cluster.Status.ObservedGeneration != 0 {
				t.Fatal("database readiness prematurely observed the Supabase generation")
			}
			return
		}
	}
	t.Fatal("components never started")
}

func TestFunctionCountAndSourceBoundaries(t *testing.T) {
	p, rc, _ := setupProvider(t)
	ctx := context.Background()
	for i := range 100 {
		if err := p.PutFunction(ctx, rc, fmt.Sprintf("function%d", i), map[string]string{"index.ts": "export default {}"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.PutFunction(ctx, rc, "overflow", map[string]string{"index.ts": "export default {}"}); err == nil {
		t.Fatal("101st function admitted")
	}
	if err := p.PutFunction(ctx, rc, "function0", map[string]string{"index.ts": strings.Repeat("<", 1<<20)}); err != nil {
		t.Fatal("existing function or exact source-size boundary rejected")
	}
	if err := p.PutFunction(ctx, rc, "main", map[string]string{"index.ts": "export default {}"}); err == nil {
		t.Fatal("reserved main-worker name admitted")
	}
}

type failedComponentBackend struct{ *testBackend }

func (b failedComponentBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "failed-component" {
		return infra.OpState{Failed: true}, nil
	}
	return b.testBackend.OpStatus(ctx, op)
}

func TestComponentFailureClearsPendingWithoutDone(t *testing.T) {
	p, rc, backend := setupProvider(t)
	convergeProvider(t, p, rc)
	state, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"import", "deploy", "scale", "resize", "public", "delete"} {
		t.Run(action, func(t *testing.T) {
			member := *state.Members["rest"]
			state.Pending = &componentPending{Op: infra.Op{ID: "failed-component"}, Component: "rest", Action: action}
			rc.Backend = failedComponentBackend{backend}
			busy, err := observeComponent(t.Context(), rc, state)
			if !busy || err == nil || state.Pending != nil {
				t.Fatalf("failed operation remained pending: busy=%v pending=%+v err=%v", busy, state.Pending, err)
			}
			current := state.Members["rest"]
			if current == nil || current.Stage != member.Stage || current.ServiceID != member.ServiceID || current.Replicas != member.Replicas || current.Resources != member.Resources {
				t.Fatal("failed operation applied its requested component state")
			}
		})
	}
}

type blockedDeployBackend struct {
	*testBackend
	pollError error
}

func (b blockedDeployBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "unready-deploy" {
		return infra.OpState{}, b.pollError
	}
	return b.testBackend.OpStatus(ctx, op)
}

func TestCleanupSupersedesUnreadyComponentDeployment(t *testing.T) {
	p, rc, backend := setupProvider(t)
	convergeProvider(t, p, rc)
	state, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	state.Pending = &componentPending{
		Op: infra.Op{ID: "unready-deploy"}, Component: "rest", Action: "deploy",
	}
	if err := saveState(rc.Cluster, state); err != nil {
		t.Fatal(err)
	}
	rc.Backend = blockedDeployBackend{testBackend: backend}
	if _, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if backend.services["demors"] != nil {
		t.Fatal("cleanup waited for component application readiness")
	}
}

func TestOptionalDeploymentDoesNotBlockReplacementProvisioning(t *testing.T) {
	for _, pollError := range []error{nil, infra.ErrUnavailable} {
		t.Run(fmt.Sprint(pollError), func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			rc.Cluster.Spec.Topology.Type = "ha"
			rc.Cluster.Spec.Components.Engine.Replicas = 3
			rc.Cluster.Generation = 2
			rc.Cluster.Status.ObservedGeneration = 1
			// Resume a durable replacement at join after its etcd learner was
			// admitted, but before Core.Sync has provisioned its runtime/volume.
			rc.Cluster.Internal = json.RawMessage(`{"postgres":{
				"uid":"replacement-fixture","topology":"ha","formed":true,
				"nextIndex":4,"configVersion":1,"bootstrapGeneration":1,
				"members":[
					{"name":"demop0","role":"data","index":0,"stage":"joined","etcdState":"existing"},
					{"name":"demop1","role":"data","index":1,"stage":"joined","etcdState":"existing"},
					{"name":"demop2","role":"data","index":2,"stage":"joined","etcdState":"existing"},
					{"name":"demop3","role":"data","index":3,"stage":"planned","volume":"demop3v","etcdState":"existing","etcdLearner":true}
				],
				"replacement":{"operation":"replace-one","old":"demop1","new":"demop3","stage":"join"}
			}}`)
			op := &domain.Operation{Kind: domain.OperationReplaceMember, Checkpoint: json.RawMessage(`{"stage":"join"}`)}
			if !p.Core.SyncDuringOperation(op) {
				t.Fatal("replacement join must allow core sync")
			}
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			state.Members["rest"] = &componentMember{Name: "demors", Component: "rest", Stage: engine.ComponentImported}
			pending := &componentPending{Op: infra.Op{ID: "unready-deploy"}, Component: "rest", Action: "deploy"}
			state.Pending = pending
			if err := saveState(rc.Cluster, state); err != nil {
				t.Fatal(err)
			}
			rc.Backend = blockedDeployBackend{testBackend: backend, pollError: pollError}
			result, err := p.Sync(t.Context(), rc)
			if !errors.Is(err, pollError) {
				t.Fatalf("component observation error lost: got %v, want %v", err, pollError)
			}
			if backend.services["demop3"] == nil || backend.services["demop3v"] == nil {
				t.Fatal("optional deployment blocked replacement runtime/volume provisioning")
			}
			if backend.services["demors"] != nil {
				t.Fatal("unfinished operation did not serialize optional mutations")
			}
			state, err = loadState(rc.Cluster)
			if err != nil || !reflect.DeepEqual(state.Pending, pending) || state.Members["rest"].Stage != engine.ComponentImported {
				t.Fatalf("optional receipt or stage changed: %+v, %v", state, err)
			}
			if result.RequeueAfter == 0 || rc.Cluster.Status.ObservedGeneration != 1 {
				t.Fatal("unfinished optional work lost its requeue or advanced the applied generation")
			}
			// A simultaneous core error must also remain visible to callers.
			backend.lookupErrors = map[string]error{"demop3": infra.ErrConflict}
			_, err = p.Sync(t.Context(), rc)
			if !errors.Is(err, infra.ErrConflict) || pollError != nil && !errors.Is(err, pollError) {
				t.Fatalf("core/component errors not retained together: %v", err)
			}
		})
	}
}

func TestComponentProcessLogs(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	if _, err := p.MemberLogs(context.Background(), rc, "demoau", "rest", 10); err == nil {
		t.Fatal("cross-component process accepted")
	}
	logs, err := p.MemberLogs(context.Background(), rc, "demoau", "auth", 10)
	if err != nil || len(logs) != 1 || logs[0].Process != "auth" || logs[0].Member != "demoau" {
		t.Fatal("component logs unavailable")
	}
}

type publicBackend struct {
	*testBackend
	enabled bool
}

func (b *publicBackend) Capabilities() infra.Capabilities {
	caps := b.testBackend.Capabilities()
	caps.PublicHTTPS = true
	return caps
}

func (b *publicBackend) SetSubdomainAccess(_ context.Context, name string, enabled bool) (infra.Op, error) {
	if enabled && !b.deployed[name] {
		return infra.Op{}, fmt.Errorf("HTTP port is not deployed")
	}
	b.enabled = enabled
	return infra.Op{}, nil
}

func (b *publicBackend) SubdomainURLs(context.Context, string) ([]string, error) {
	if b.enabled {
		return []string{"https://gateway.example.com"}, nil
	}
	return nil, nil
}

func TestPublicGatewayDeploysHTTPBeforePublishingURLs(t *testing.T) {
	p, rc, backend := setupProvider(t)
	public := &publicBackend{testBackend: backend}
	rc.Backend = public
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"gateway":{"publicHttps":true}}`)
	if err := engine.CheckCapabilities(p, rc.Cluster.Spec, backend.Name(), backend.Capabilities()); err == nil {
		t.Fatal("public gateway admitted on backend without HTTPS")
	}
	convergeProvider(t, p, rc)
	config, err := p.MemberConfig(context.Background(), rc, "demoau")
	if err != nil {
		t.Fatal(err)
	}
	var spec sbspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if !public.enabled || spec.APIExternalURL != "https://gateway.example.com" {
		t.Fatal("Auth did not receive the effective public gateway URL")
	}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{}`)
	rc.Cluster.Generation++
	convergeProvider(t, p, rc)
	if public.enabled {
		t.Fatal("public gateway was not disabled")
	}
}

func TestComponentIdentitySurvivesLostImportCheckpoint(t *testing.T) {
	p, rc, backend := setupProvider(t)
	ctx := context.Background()
	for range 100 {
		if _, err := p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
		state, err := loadState(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if state.Members["gateway"] == nil {
			continue
		}
		if backend.services["demogw"] != nil {
			t.Fatal("infrastructure imported before identity was persisted")
		}
		planned := append(json.RawMessage(nil), rc.Cluster.Internal...)
		token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), "demogw")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
		if backend.services["demogw"] == nil {
			t.Fatal("runtime was not imported")
		}
		// Simulate a restart after backend import but before its receipt persisted.
		rc.Cluster.Internal = planned
		if _, err = p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
		after, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), "demogw")
		if err != nil || after.Token != token.Token {
			t.Fatal("restart replaced the imported runtime's ownership token")
		}
		return
	}
	t.Fatal("component identity was never allocated")
}

func TestSupabaseDataMembersTrustRealtimeOutputPlugin(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	config, err := p.MemberConfig(context.Background(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Postgres == nil || spec.Postgres.Parameters["wal_level"] != "logical" {
		t.Fatal("Supabase database must enable logical decoding")
	}
	trusted := map[string]bool{}
	for _, name := range strings.Split(spec.Postgres.Parameters["output_plugin_libraries"], ",") {
		trusted[strings.TrimSpace(name)] = true
	}
	for _, name := range []string{"pgoutput", "test_decoding", "wal2json"} {
		if !trusted[name] {
			t.Fatalf("Supabase database does not trust required output plugin %s", name)
		}
	}
	if len(trusted) != 3 {
		t.Fatal("Supabase database unexpectedly trusts additional output plugins")
	}
}

func TestPublicGatewayRevocationPrecedesCoreHealth(t *testing.T) {
	for _, coreFailure := range []string{"unhealthy", "unreachable"} {
		t.Run(coreFailure, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			public := &publicBackend{testBackend: backend}
			rc.Backend = public
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"gateway":{"publicHttps":true}}`)
			convergeProvider(t, p, rc)
			if coreFailure == "unhealthy" {
				backend.stopped = map[string]bool{"demop0": true}
			} else {
				agents := rc.Agents.(testAgents)
				agents.url = "https://127.0.0.1:1"
				rc.Agents = agents
			}
			status, err := p.Core.Status(t.Context(), rc)
			if err == nil && status.Phase == domain.PhaseReady {
				t.Fatal("regression requires a core that is not ready")
			}
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{}`)
			rc.Cluster.Generation++
			// Core reconciliation still runs and may report the unavailable
			// member; that must not stop the earlier route revocation.
			_, _ = p.Sync(t.Context(), rc)
			if public.enabled {
				t.Fatal("public route survived revocation while PostgreSQL was degraded")
			}
			// Apply the durable revocation receipt even if core remains down.
			_, _ = p.Sync(t.Context(), rc)
			state, err := loadState(rc.Cluster)
			if err != nil || state.Members["gateway"].Public || state.Pending != nil {
				t.Fatal("revocation receipt was not applied", err)
			}
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"gateway":{"publicHttps":true}}`)
			_, _ = p.Sync(t.Context(), rc)
			if public.enabled {
				t.Fatal("new public exposure bypassed the core readiness gate")
			}
		})
	}
}

func TestDeletedJoinedComponentIsRecreatedWithDurableIdentity(t *testing.T) {
	for _, component := range []string{"rest", "gateway"} {
		t.Run(component, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			convergeProvider(t, p, rc)
			name := rc.Cluster.Name + codes[component]
			token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), name)
			if err != nil {
				t.Fatal(err)
			}
			config, err := p.MemberConfig(t.Context(), rc, name)
			if err != nil {
				t.Fatal(err)
			}
			delete(backend.services, name)
			delete(backend.deployed, name)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			state, err := loadState(rc.Cluster)
			if err != nil || state.Members[component].Stage != engine.ComponentPlanned || state.Pending == nil || state.Pending.Action != "import" {
				t.Fatal("confirmed absence did not restart provisioning", err)
			}
			convergeProvider(t, p, rc)
			after, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), name)
			if err != nil || after.Token != token.Token {
				t.Fatal("service recreation replaced durable identity", err)
			}
			current, err := p.MemberConfig(t.Context(), rc, name)
			if err != nil || current.ConfigVersion <= config.ConfigVersion || !backend.deployed[name] {
				t.Fatal("recreated service was not redeployed with current configuration", err)
			}
		})
	}
}

func TestJoinedComponentObservationFailsClosed(t *testing.T) {
	for _, observation := range []string{"lookup-error", "conflicting-marker", "replacement"} {
		t.Run(observation, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			convergeProvider(t, p, rc)
			service := backend.services["demors"]
			switch observation {
			case "lookup-error":
				backend.lookupErrors = map[string]error{"demors": infra.ErrUnavailable}
			case "conflicting-marker":
				service.Ownership[engine.OwnerVariable] = "another-owner"
			case "replacement":
				service.ID = "replacement-id"
				service.Ownership = map[string]string{engine.OwnerVariable: "another-owner"}
			}
			_, err := p.Sync(t.Context(), rc)
			want := engine.ErrConflict
			if observation == "lookup-error" {
				want = infra.ErrUnavailable
			}
			if !errors.Is(err, want) {
				t.Fatalf("observation %s: got %v, want %v", observation, err, want)
			}
			state, err := loadState(rc.Cluster)
			if err != nil || state.Pending != nil || state.Members["rest"].Stage != engine.ComponentJoined {
				t.Fatal("unproven absence/replacement mutated provisioning state", err)
			}
			if observation != "lookup-error" && service.Ownership[engine.OwnerVariable] != "another-owner" {
				t.Fatal("conflicting ownership marker was overwritten")
			}
		})
	}
}

func TestRealtimeAdminSecretMigrationAndProjection(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	before, err := loadCredentials(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	before.RealtimeAdminSecret = ""
	if err := storeCredentials(t.Context(), rc, before); err != nil {
		t.Fatal(err)
	}
	spec, err := p.desiredSpec(t.Context(), rc, &componentMember{Component: "realtime"})
	if err != nil {
		t.Fatal(err)
	}
	after, err := loadCredentials(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.RealtimeAdminSecret) < 32 || spec.RealtimeAdminSecret == before.Keys.JWTSecret || spec.JWTSecret != before.Keys.JWTSecret || spec.RealtimeAdminSecret != after.RealtimeAdminSecret || after.Keys.AnonKey != before.Keys.AnonKey {
		t.Fatal("admin secret migration changed tenant credentials or was not sealed independently")
	}
	if err := ensureCredentials(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	stable, err := loadCredentials(t.Context(), rc)
	if err != nil || stable.RealtimeAdminSecret != after.RealtimeAdminSecret {
		t.Fatal("credential migration regenerated administration secret", err)
	}
	state, _ := loadState(rc.Cluster)
	gateway, err := p.desiredSpec(t.Context(), rc, state.Members["gateway"])
	if err != nil || gateway.RealtimeAdminSecret != "" {
		t.Fatal("gateway received Realtime administration secret", err)
	}
}

func TestStudioDiscoveryAndRuntimeExposeOnlyAuthenticatedPort(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	rc.Cluster.Spec.Components.Extra["studio"] = domain.ComponentSpec{Replicas: 1}
	gateway, err := p.desiredSpec(t.Context(), rc, &componentMember{Component: "gateway"})
	if err != nil || gateway.Upstreams["studio"] != "demosd:3002" {
		t.Fatal("gateway discovery does not target authenticated Studio ingress", err)
	}
	runtime, err := p.runtimeSpec(rc, &componentMember{Component: "studio", Name: "demosd"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, port := range runtime.Ports {
		if port.Port == 3000 {
			t.Fatal("raw Studio port exposed in runtime")
		}
		found = found || port.Port == 3002
	}
	if !found {
		t.Fatal("authenticated Studio ingress missing from runtime")
	}
}

func TestStudioRuntimePortUpgradeForcesRedeploy(t *testing.T) {
	for _, oldPort := range []int{0, 3000} {
		t.Run(fmt.Sprintf("port=%d", oldPort), func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			for _, name := range []string{"meta", "studio"} {
				desired := rc.Cluster.Spec.Components.Extra[name]
				desired.Replicas = 1
				rc.Cluster.Spec.Components.Extra[name] = desired
			}
			convergeProvider(t, p, rc)
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			state.Members["studio"].RuntimePort = oldPort
			if err := saveState(rc.Cluster, state); err != nil {
				t.Fatal(err)
			}
			backend.deployed["demosd"] = false
			convergeProvider(t, p, rc)
			state, err = loadState(rc.Cluster)
			if err != nil || state.Members["studio"].RuntimePort != 3002 || !backend.deployed["demosd"] {
				t.Fatal("existing Studio runtime was not redeployed onto authenticated port", err)
			}
		})
	}
}
