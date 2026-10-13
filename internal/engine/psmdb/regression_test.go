// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pki"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type memberDialer map[string]testDialer

func (d memberDialer) Dial(ctx context.Context, cluster, member string) (*agentapi.Client, error) {
	return d[member].Dial(ctx, cluster, member)
}
func responder(t *testing.T, handler http.HandlerFunc) testDialer {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	return testDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
}

func TestBootstrapFollowsPrimaryAndAcceptsVerifiedResume(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	s, _ := load(rc.Cluster)
	s.UID = "bootstrap"
	plan(rc.Cluster, s)
	for _, m := range s.Members {
		m.Stage = "healthy"
	}
	calls := []string{}
	d := memberDialer{}
	for _, m := range s.Members {
		name := m.Name
		d[name] = responder(t, func(w http.ResponseWriter, r *http.Request) {
			calls = append(calls, name)
			result := agentapi.BootstrapResponse{Initialized: true}
			if name == "demom0" {
				result.Primary = "demom1:27017"
			}
			_ = json.NewEncoder(w).Encode(result)
		})
	}
	rc.Agents = d
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Sets["rs0"].Stage != "users" || !slices.Equal(calls, []string{"demom0", "demom1"}) {
		t.Fatalf("stage=%s calls=%v", s.Sets["rs0"].Stage, calls)
	}
}

func TestConfigReplicaSetBootstrapsBeforeRouterDeployment(t *testing.T) {
	p, rc, f, _ := setup(t)
	rc.Cluster.Spec.Topology = domain.Topology{Type: "sharded", Shards: 1}
	p.ApplyDefaults(&rc.Cluster.Spec)
	s, _ := load(rc.Cluster)
	s.UID = "sharded"
	plan(rc.Cluster, s)
	for _, m := range s.Members {
		m.Stage = "healthy"
		if m.Role == agentapi.RoleRouter {
			m.Stage = "imported"
		}
	}
	rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
		var req agentapi.BootstrapRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.ReplSetName != "cfg" {
			t.Errorf("bootstrapped %s before config set", req.ReplSetName)
		}
		_ = json.NewEncoder(w).Encode(agentapi.BootstrapResponse{Initialized: true})
	})
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if len(f.deployments) != 0 || s.Sets["cfg"].Stage != "users" {
		t.Fatalf("router deployed before bootstrap: deployments=%d cfg=%s", len(f.deployments), s.Sets["cfg"].Stage)
	}
	s.Sets["cfg"].Stage = "ready"
	_ = save(rc.Cluster, s)
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	if len(f.deployments) != 1 {
		t.Fatal("router did not deploy after config set ready")
	}
}

func TestUnownedRuntimeAndVolumeCollisionsAreRejected(t *testing.T) {
	for _, volume := range []bool{false, true} {
		t.Run(map[bool]string{false: "runtime", true: "volume"}[volume], func(t *testing.T) {
			p, rc, f, _ := setup(t)
			s, _ := load(rc.Cluster)
			s.UID = "collision"
			plan(rc.Cluster, s)
			m := s.Members[0]
			name := m.Name
			if volume {
				name = m.Volume
			}
			f.services[name] = &infra.Service{ID: "foreign", Hostname: name}
			_ = save(rc.Cluster, s)
			if _, err := p.Sync(context.Background(), rc); !errors.Is(err, engine.ErrConflict) {
				t.Fatalf("collision: %v", err)
			}
			if f.mutations != 0 {
				t.Fatal("mutated an unowned service")
			}
			if _, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepBackups: true}); !errors.Is(err, engine.ErrConflict) {
				t.Fatalf("cleanup collision: %v", err)
			}
			if f.mutations != 0 || len(f.unexposed) != 0 {
				t.Fatal("cleanup touched an unowned service")
			}
		})
	}
}

func TestMatchingOwnershipAndRetainedIDsAllowAdoption(t *testing.T) {
	p, rc, f, _ := setup(t)
	ctx := context.Background()
	s, _ := load(rc.Cluster)
	s.UID = "owned"
	plan(rc.Cluster, s)
	if err := p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	m := s.Members[0]
	token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Name, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	f.services[m.Name] = &infra.Service{ID: "runtime-id", Hostname: m.Name, Ownership: ownershipEnv(rc.Cluster.Name, m.Name, token.Token)}
	f.services[m.Volume] = &infra.Service{ID: "volume-id", Hostname: m.Volume}
	if err := rc.Store.CreateRetainedVolume(ctx, &domain.RetainedVolume{Cluster: rc.Cluster.Name, Member: m.Name, Hostname: m.Volume, ServiceID: "volume-id"}); err != nil {
		t.Fatal(err)
	}
	_ = save(rc.Cluster, s)
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	m = s.Members[0]
	if m.Stage != "imported" || m.ServiceID != "runtime-id" || m.VolumeID != "volume-id" {
		t.Fatal(m)
	}
}

func TestLegacyThreeMemberPlanAdoptsVerifiedDeployedResources(t *testing.T) {
	p, rc, f, _ := setup(t)
	ctx := context.Background()
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	s, _ := load(rc.Cluster)
	s.UID = "legacy"
	plan(rc.Cluster, s)
	if err := p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	for i, m := range s.Members {
		m.Stage = []string{"imported", "deployed", "active"}[i]
		token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Name, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		env := ownershipEnv(rc.Cluster.Name, m.Name, token.Token)
		env["KARAKORAM_DATA_DIR"] = "/mnt/" + m.Volume
		f.services[m.Name] = &infra.Service{ID: m.Name + "-id", Hostname: m.Name, Ownership: env}
		// The deployed 8ba371a importer wrote no env to local-storage services.
		f.services[m.Volume] = &infra.Service{ID: m.Volume + "-id", Hostname: m.Volume}
	}
	_ = save(rc.Cluster, s)
	if _, err := p.Cleanup(ctx, rc, domain.DeleteOptions{KeepBackups: true, KeepVolumes: true}); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	for _, m := range s.Members {
		if m.ServiceID != m.Name+"-id" || m.VolumeID != m.Volume+"-id" {
			t.Fatalf("legacy IDs not migrated: %+v", m)
		}
	}
	// The first runtime is gone: durable volume IDs permit the next cleanup.
	if _, err := p.Cleanup(ctx, rc, domain.DeleteOptions{KeepBackups: true, KeepVolumes: true}); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyVolumeAdoptionRequiresAuthenticatedRuntimeMount(t *testing.T) {
	for _, variant := range []string{"wrong-token", "wrong-mount", "planned"} {
		t.Run(variant, func(t *testing.T) {
			p, rc, f, _ := setup(t)
			ctx := context.Background()
			s, _ := load(rc.Cluster)
			s.UID = "legacy-negative"
			plan(rc.Cluster, s)
			if err := p.ensureSecrets(ctx, rc, s); err != nil {
				t.Fatal(err)
			}
			m := s.Members[0]
			m.Stage = "active"
			token, _ := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Name, m.Name)
			env := ownershipEnv(rc.Cluster.Name, m.Name, token.Token)
			env["KARAKORAM_DATA_DIR"] = "/mnt/" + m.Volume
			switch variant {
			case "wrong-token":
				env["KARAKORAM_AGENT_TOKEN"] = "foreign-token"
			case "wrong-mount":
				env["KARAKORAM_DATA_DIR"] = "/mnt/foreign"
			case "planned":
				m.Stage = "planned"
			}
			f.services[m.Name] = &infra.Service{ID: "runtime-id", Hostname: m.Name, Ownership: env}
			f.services[m.Volume] = &infra.Service{ID: "volume-id", Hostname: m.Volume}
			_ = save(rc.Cluster, s)
			if _, err := p.Sync(ctx, rc); !errors.Is(err, engine.ErrConflict) {
				t.Fatalf("adopted unverified legacy volume: %v", err)
			}
			if f.mutations != 0 {
				t.Fatal("mutated unverified resources")
			}
		})
	}
}

func TestManagedOverridePolicyMatchesAgent(t *testing.T) {
	p, rc, _, _ := setup(t)
	for _, text := range []string{"operationProfiling:\n  slowOpThresholdMs: 5\n", "storage:\n  wiredTiger:\n    engineConfig:\n      cacheSizeGB: 3\n", "storage: null\n", "net.port: 1\n", "security:\n  transitionToAuth: true\n", "security.transitionToAuth: true\n"} {
		rc.Cluster.Spec.EngineConfig = text
		if err := p.Validate(context.Background(), nil, rc.Cluster); !errors.Is(err, engine.ErrInvalid) {
			t.Fatalf("accepted %q: %v", text, err)
		}
		if _, err := agentapi.ParseEngineConfig(text); err == nil {
			t.Fatalf("agent accepted %q", text)
		}
	}
	rc.Cluster.Spec.EngineConfig = "operationProfiling:\n  mode: slowOp\nnet:\n  maxIncomingConnections: 500\n"
	if err := p.Validate(context.Background(), nil, rc.Cluster); err != nil {
		t.Fatal(err)
	}
}

func TestRenewsMemberAndAgentCertificatesBeforeExpiry(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	if err := p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	caCert, _ := secret(ctx, rc, "tls/ca.crt")
	caKey, _ := secret(ctx, rc, "tls/ca.key")
	ca, err := pki.LoadCA(caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	before := s.ConfigVersion
	for _, kind := range []string{"members", "agents"} {
		cert, key, err := ca.Issue(pki.CertRequest{CommonName: s.Members[0].Name, DNSNames: []string{s.Members[0].Name}, Validity: time.Hour, Server: true, Client: kind == "members"})
		if err != nil {
			t.Fatal(err)
		}
		if err := putSecret(ctx, rc, "tls/"+kind+"/"+s.Members[0].Name+".pem", append(cert, key...)); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"members", "agents"} {
		active, err := secret(ctx, rc, "tls/"+kind+"/"+s.Members[0].Name+".pem")
		if err != nil {
			t.Fatal(err)
		}
		renew, err := pki.NeedsRenewal(active, 30*24*time.Hour, time.Now())
		if err != nil || !renew {
			t.Fatalf("%s active certificate changed before disruption check: %v", kind, err)
		}
		cert, err := secret(ctx, rc, "tls/"+kind+"/"+s.Members[0].Name+".pending.pem")
		if err != nil {
			t.Fatal(err)
		}
		renew, err = pki.NeedsRenewal(cert, 30*24*time.Hour, time.Now())
		if err != nil || renew {
			t.Fatalf("%s not renewed: %v %v", kind, renew, err)
		}
	}
	if s.ConfigVersion <= before {
		t.Fatal("renewal did not schedule distribution")
	}
	stable := s.ConfigVersion
	if err := p.ensureSecrets(ctx, rc, s); err != nil || s.ConfigVersion != stable {
		t.Fatalf("unnecessary renewal: %v", err)
	}
}

func TestProjectPortAllocationIncludesExternalRoutes(t *testing.T) {
	p, rc, f, _ := setup(t)
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	f.publicPorts = []infra.PublicPortRoute{{Hostname: "foreign", PublicPort: infra.PublicPort{Port: 27017}}}
	port, err := p.allocatePublicPort(context.Background(), rc, s, s.Members[0])
	if err != nil || port != 27018 {
		t.Fatal(port, err)
	}
	s.Members[0].PublicPort = port
	f.publicPorts = append(f.publicPorts, infra.PublicPortRoute{Hostname: "another-controller", PublicPort: infra.PublicPort{Port: port}})
	next, err := p.allocatePublicPort(context.Background(), rc, s, s.Members[0])
	if err != nil || next != 27019 {
		t.Fatal(next, err)
	}
}

func TestShardedExposureRemovesInternalRoutesAndPublishesOnlyRouter(t *testing.T) {
	p, rc, f, _ := setup(t)
	rc.Cluster.Spec.Topology = domain.Topology{Type: "sharded", Shards: 1}
	p.ApplyDefaults(&rc.Cluster.Spec)
	rc.Cluster.Spec.Exposure.Public = true
	rc.Cluster.Spec.Exposure.Allowlist = []string{"2001:db8::/64"}
	s, _ := load(rc.Cluster)
	s.UID = "exposure"
	plan(rc.Cluster, s)
	for _, m := range s.Members {
		m.Stage = "active"
		m.Version = rc.Cluster.Spec.Version
		m.Resources = runtimeResources(component(rc.Cluster.Spec, m).Resources)
		m.Disk = component(rc.Cluster.Spec, m).Resources
		m.Containers = component(rc.Cluster.Spec, m).Replicas
		if m.Role != agentapi.RoleRouter {
			m.PublicPort = 28000 + m.Index
			m.PublicHash = hash(rc.Cluster.Spec.Exposure)
		}
	}
	for _, set := range s.Sets {
		set.Stage = "ready"
		set.Added = true
	}
	rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "mongos": {State: "running"}}, ConfigVersion: 100})
	})
	_ = save(rc.Cluster, s)
	for range 12 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(f.exposed, []string{"demor"}) {
		t.Fatalf("public services %v", f.exposed)
	}
	s, _ = load(rc.Cluster)
	for _, m := range s.Members {
		if m.Role != agentapi.RoleRouter && (m.PublicPort != 0 || !slices.Contains(f.unexposed, m.Name)) {
			t.Fatalf("internal route retained: %s", m.Name)
		}
	}
}

func TestBackupOperationInvalidatesPBMHashAndMissingDeleteCompletes(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	st := rc.Store.(*store.Store)
	s, _ := load(rc.Cluster)
	s.PBMHash = "primary-hash"
	s.PBMPrefix = "karakoram/demo-id"
	_ = save(rc.Cluster, s)
	if err := st.CreateBackupStorage(ctx, &domain.BackupStorage{Name: "alternate", Type: "s3"}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateBackup(ctx, &domain.Backup{Name: "snapshot", Cluster: rc.Cluster.Name, Storage: "alternate", Type: "logical"}); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snapshot"}`), Checkpoint: json.RawMessage(`{"step":"done"}`)}
	result, err := p.RunOperation(ctx, rc, op)
	if err != nil || !result.Done {
		t.Fatal(result, err)
	}
	s, _ = load(rc.Cluster)
	if s.PBMHash != "" {
		t.Fatal("PBM hash remained applied")
	}
	op.Kind = domain.OperationDeleteBackup
	op.Payload = json.RawMessage(`{"backup":"already-deleted"}`)
	result, err = p.RunOperation(ctx, rc, op)
	if err != nil || !result.Done {
		t.Fatal(result, err)
	}
}

func TestCloneValidationAndStorageResolutionUseRetainedBackup(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	st := rc.Store.(*store.Store)
	if err := st.CreateBackupStorage(ctx, &domain.BackupStorage{Name: "retained", Type: "s3"}); err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Name: "retained-snapshot", Cluster: "deletedsource", Storage: "retained", PBMPrefix: "old/prefix", SourceTopology: rc.Cluster.Spec.Topology, SourceVersion: rc.Cluster.Spec.Version, Phase: domain.PhaseSucceeded}
	if err := st.CreateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: b.Cluster, Backup: b.Name}
	if err := p.Validate(ctx, nil, rc.Cluster); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	s.PBMPrefix = "target/prefix"
	env, err := p.pbmEnv(ctx, rc, s)
	if err != nil || env.Storage.Name != "retained" {
		t.Fatal(env.Storage.Name, err)
	}
}

func TestOperationSyncPolicyPermitsOnlyReplacementStages(t *testing.T) {
	p, _, _, _ := setup(t)
	for _, kind := range []string{domain.OperationReplaceMember, domain.OperationRestore, domain.OperationRestart, domain.OperationBackup} {
		for _, stage := range []string{"", "join", "remove", "done"} {
			cp, _ := json.Marshal(operationState{Stage: stage})
			got := p.SyncDuringOperation(&domain.Operation{Kind: kind, Phase: domain.PhaseRunning, Checkpoint: cp})
			want := kind == domain.OperationReplaceMember && (stage == "join" || stage == "remove")
			if got != want {
				t.Fatalf("%s/%s sync=%v", kind, stage, got)
			}
		}
	}
}

func TestCanonicalMemberRoles(t *testing.T) {
	for state, want := range map[string]string{"PRIMARY": "primary", "SECONDARY": "secondary", "ARBITER": "arbiter", "STARTUP2": "startup", "RECOVERING": "recovering", "ROLLBACK": "unknown", "DOWN": "unknown", "": "unknown"} {
		if got := memberRole(state); got != want {
			t.Fatalf("%s: %s", state, got)
		}
	}
}

func TestExplicitInPlaceRestoreUsesBackupRecordedStorage(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	st := rc.Store.(*store.Store)
	for _, name := range []string{"primary", "alternate"} {
		if err := st.CreateBackupStorage(ctx, &domain.BackupStorage{Name: name, Type: "s3", EndpointURL: "https://s3.example.test", Bucket: "bucket", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}); err != nil {
			t.Fatal(err)
		}
	}
	rc.Cluster.Spec.Backup.Storage = "primary"
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	s.PBMPrefix = "target-prefix"
	_ = save(rc.Cluster, s)
	if err := st.CreateBackup(ctx, &domain.Backup{Name: "alternate-base", Cluster: rc.Cluster.Name, Storage: "alternate", PBMPrefix: "recorded-prefix", Type: "logical", PBMName: "snapshot", Phase: domain.PhaseSucceeded}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRestore(ctx, &domain.Restore{Name: "restore", Cluster: rc.Cluster.Name, Source: domain.RestoreSource{SourceCluster: rc.Cluster.Name, Backup: "alternate-base"}}); err != nil {
		t.Fatal(err)
	}
	rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
	})
	result, err := p.RunOperation(ctx, rc, &domain.Operation{ID: "restore", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)})
	if err != nil {
		t.Fatal(err)
	}
	var cp struct{ Storage, Prefix string }
	if err := json.Unmarshal(result.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Storage != "alternate" || cp.Prefix != "recorded-prefix" {
		t.Fatalf("wrong in-place source: %s", result.Checkpoint)
	}
}
