// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package reconciler

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

// The real provider sees an already deployed formation. Only platform inventory
// is synthetic; Sync, Status, PITR and the Manager's retry/publication paths run
// their production implementations against real TLS agent HTTP clients.
type mysqlBudgetBackend struct {
	infra.Backend
	slow  *atomic.Bool
	reads *atomic.Int32
}

func (b mysqlBudgetBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	if err := b.delay(ctx); err != nil {
		return nil, err
	}
	kind := infra.KindRuntime
	if strings.HasSuffix(name, "v") {
		kind = infra.KindVolume
	}
	return &infra.Service{ID: name + "-id", Hostname: name, Kind: kind, Status: infra.StatusRunning}, nil
}

func (b mysqlBudgetBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if err := b.delay(ctx); err != nil {
		return nil, err
	}
	return []infra.Container{{ID: name + "-instance", Hostname: name + "-instance", Status: infra.ContainerRunning}}, nil
}

func (b mysqlBudgetBackend) delay(ctx context.Context) error {
	if b.slow == nil || !b.slow.Load() {
		return nil
	}
	if b.reads != nil {
		b.reads.Add(1)
	}
	timer := time.NewTimer(300 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type mysqlBudgetAgents struct {
	url string
	ca  []byte
}

func (a mysqlBudgetAgents) Dial(_ context.Context, _ domain.ClusterRef, member string) (*agentapi.Client, error) {
	endpoint, err := url.Parse(a.url)
	if err != nil {
		return nil, err
	}
	return agentapi.NewClient(agentapi.ClientOptions{
		BaseURL: a.url, CAPEM: a.ca, Token: member,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, endpoint.Host)
		},
	})
}

func TestMySQLManagerReconcileObservationBudget(t *testing.T) {
	for _, old := range []bool{true, false} {
		for _, slow := range []string{"status", "galera-status", "proxy-status"} {
			t.Run(fmt.Sprintf("old-agent=%t/slow-%s", old, slow), func(t *testing.T) {
				testMySQLManagerObservationBudget(t, old, slow, false, false)
			})
		}
	}
	t.Run("old-agent/unsupported-galera-status", func(t *testing.T) {
		testMySQLManagerObservationBudget(t, true, "proxy-status", true, false)
	})
	t.Run("new-agent/formed-deployed-join", func(t *testing.T) {
		testMySQLManagerObservationBudget(t, false, "proxy-status", false, true)
	})
}

func testMySQLManagerObservationBudget(t *testing.T, old bool, slow string, unsupportedGalera, joinDeployed bool) {
	const group = "11111111-2222-3333-4444-555555555555"
	p := mysql.New(mysql.Options{Provider: "pxc"})
	m, st := newTestManager(t, p)
	var slowEnabled atomic.Bool
	var platformReads atomic.Int32
	backend := mysqlBudgetBackend{Backend: fake.New(fake.Options{}), slow: &slowEnabled, reads: &platformReads}
	m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: backend}
	m.opts.Context.ObservationEpoch = "budget-test-process"
	c := &domain.Cluster{Name: "leasepxc", Generation: 1, Spec: domain.ClusterSpec{Provider: "pxc", Topology: domain.Topology{Type: "cluster"}}}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Backup.PITR.Enabled = true
	storage := &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://objects.example.test", Bucket: "backups", Region: "region", Prefix: "prefix", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := st.CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	c.Spec.Backup.Storage = storage.Name
	c.Status = domain.ClusterStatus{Phase: domain.PhaseDegraded, ObservedGeneration: 1}
	members := make([]map[string]any, 0, 4)
	for i := range 4 {
		name, role, component := fmt.Sprintf("%sm%d", c.Name, i), "data", c.Spec.Components.Engine
		if i == 3 {
			name, role, component = c.Name+"mx", "proxy", c.Spec.Components.Extra["proxy"]
		}
		members = append(members, map[string]any{
			"name": name, "role": role, "index": i, "serviceId": name + "-id", "stage": "joined",
			"version": c.Spec.Version, "resources": component.Resources, "replicas": 1,
			"configVersion": 1, "initialized": role == "data",
			"serverUUID": fmt.Sprintf("aaaaaaaa-bbbb-cccc-dddd-%012d", i+1),
		})
		if role == "data" {
			members[len(members)-1]["volume"] = name + "v"
			members[len(members)-1]["volumeId"] = name + "v-id"
		}
		c.Status.Members = append(c.Status.Members, domain.MemberStatus{Name: name, Health: true})
	}
	detected := time.Now().UTC().Add(-time.Hour)
	incident := map[string]any{"detectedAt": detected, "safeThrough": detected.Add(-time.Minute), "members": []string{c.Name + "m2"}, "audited": true}
	internal := map[string]any{"mysql": map[string]any{
		"uid": "existing-fixture", "nextIndex": 3, "configVersion": 1, "topology": "cluster", "members": members,
		"formation":     map[string]any{"groupName": group, "formationEpoch": 1, "seed": c.Name + "m0", "state": "formed"},
		"gtidIncidents": []any{incident},
	}}
	var err error
	if c.Internal, err = json.Marshal(internal); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	// These members are already deployed: their agent credentials and retained
	// volume identities must exist before Sync observes infrastructure recovery.
	for _, member := range members {
		name := member["name"].(string)
		if err := st.SetAgentToken(t.Context(), &domain.AgentToken{Namespace: c.Ref().Namespace, Cluster: c.Name, Member: name, Token: name, Hash: secrets.Hash(name)}); err != nil {
			t.Fatal(err)
		}
	}
	journal, err := json.Marshal(map[string]any{"incidents": []any{incident}})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetClusterSecret(t.Context(), c.Ref(), "mysql/gtid/history", journal); err != nil {
		t.Fatal(err)
	}
	// Existing initialized PXC formations require their shared traffic bundle.
	ca, err := pki.NewCA("existing PXC traffic", 0)
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := ca.Issue(pki.CertRequest{CommonName: group, DNSNames: []string{c.Name + "m0", c.Name + "m1", c.Name + "m2"}, Server: true, Client: true})
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string][]byte{
		"tls/galera/ca.crt": ca.CertPEM, "tls/galera/ca.key": ca.KeyPEM,
		"tls/galera/server.crt": cert, "tls/galera/server.key": key,
	} {
		if err := st.SetClusterSecret(t.Context(), c.Ref(), name, value); err != nil {
			t.Fatal(err)
		}
	}
	var mutations atomic.Int32
	var mu sync.Mutex
	calls := make(map[string]int)
	release := make(chan struct{})
	publishedBeforeInvalidation := make(chan domain.ClusterStatus, 1)
	var capturePublication sync.Once
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		member := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		calls[member+" "+r.URL.Path]++
		count := calls[member+" "+r.URL.Path]
		mu.Unlock()
		// Proxy logical and pinned calls share the server's member/path token,
		// so count2 there is still the FIRST sweep. Data metadata has one
		// transport identity and unambiguously identifies the post-join sweep.
		if joinDeployed && slowEnabled.Load() && member == c.Name+"m0" && r.URL.Path == "/v1/status" && count == 2 {
			capturePublication.Do(func() {
				latest, _, err := st.GetClusterObservation(t.Context(), c.Ref())
				if err != nil {
					t.Errorf("capture pre-Sync publication: %v", err)
				} else {
					publishedBeforeInvalidation <- latest.Status
				}
			})
		}
		if slowEnabled.Load() && r.URL.Path == "/v1/mysql/"+slow {
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			case <-release:
				return
			}
		}
		switch r.URL.Path {
		case "/v1/status":
			build := agentapi.AgentBuild{Version: "new", Commit: "new"}
			if old {
				build = agentapi.AgentBuild{Version: "old", Commit: "old"}
			}
			_ = json.NewEncoder(w).Encode(agentapi.Status{Agent: build, MySQLRestoreCertification: !old, ConfigVersion: 100000,
				Processes: map[string]agentapi.ProcessStatus{"mysqld": {PID: 1}, "proxysql": {PID: 1}}})
		case "/v1/mysql/status":
			status := myspec.StatusResponse{MemberState: "Synced", MemberRole: "PRIMARY", GTIDExecuted: group + ":1-10"}
			if !old {
				status.GroupMembers = 3
				status.ServerUUID = mysqlBudgetNativeUUID(member)
			}
			_ = json.NewEncoder(w).Encode(status)
		case "/v1/mysql/galera-status":
			if slowEnabled.Load() && unsupportedGalera {
				http.NotFound(w, r)
				return
			}
			status := myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, ClusterSize: 3, StateUUID: group, LastCommitted: 10, GTIDExecuted: group + ":1-10"}
			if !old {
				status.ServerUUID = mysqlBudgetNativeUUID(member)
			}
			_ = json.NewEncoder(w).Encode(status)
		case "/v1/mysql/proxy-status":
			_ = json.NewEncoder(w).Encode(myspec.ProxyStatusResponse{Backends: []myspec.ProxyBackend{{Hostgroup: myspec.ProxyWriterGroup, Host: c.Name + "m2", Port: 3306, Status: "ONLINE"}}})
		case "/v1/mysql/archive-status":
			_ = json.NewEncoder(w).Encode(myspec.BinlogArchiveStatus{})
		default:
			mutations.Add(1)
			t.Errorf("unexpected native/mutating action %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	// Release handlers before server.Close even when an assertion fails.
	t.Cleanup(func() { close(release) })
	agents := mysqlBudgetAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	m.opts.Context.Agents = agents
	rc := m.opts.Context
	rc.Cluster, rc.Backend, rc.Namespace = c, backend, c.Ref().Namespace
	// Provision local credential/config receipts before injecting RPC latency.
	// This is the real provider, not a replacement Sync implementation.
	if _, err := p.Sync(t.Context(), &rc); err != nil {
		t.Fatalf("prepare existing formation: %v", err)
	}
	if joinDeployed {
		if err := json.Unmarshal(c.Internal, &internal); err != nil {
			t.Fatal(err)
		}
		formation := internal["mysql"].(map[string]any)
		for _, value := range formation["members"].([]any) {
			member := value.(map[string]any)
			if member["name"] == c.Name+"m0" {
				member["stage"] = "deployed"
			}
		}
		if c.Internal, err = json.Marshal(internal); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	completed := time.Now().UTC().Add(-time.Minute)
	backupStarted := detected.Add(time.Minute)
	backup := &domain.Backup{
		Namespace: c.Ref().Namespace, Cluster: c.Name, Name: "old-full", OperationID: "finished-full",
		Storage: storage.Name, Type: "full", Provider: "pxc", Phase: domain.PhaseSucceeded,
		SourceVersion: c.Spec.Version, SourceTopology: c.Spec.Topology,
		StartedAt: &backupStarted, CompletedAt: &completed,
		MySQL: &domain.MySQLBackup{Label: "old-full", Type: "full", ChainRootLabel: "old-full", Repository: "prefix/pxc/default/leasepxc-existing-fixture", GTIDSet: group + ":1-10", ToLSN: 10, ServerVersion: c.Spec.Version, ToolVersion: "8.4.0", EndTime: completed, ClusterUID: "existing-fixture", PhysicalIdentity: "existing-source"},
	}
	if err := st.CreateBackup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "finished-full", Namespace: c.Ref().Namespace, Cluster: c.Name, Kind: domain.OperationBackup, Target: domain.OperationTarget{Kind: "backup", Name: backup.Name}, Phase: domain.PhaseFailed, LastError: "historical acknowledgement failed", CompletedAt: &completed, Checkpoint: json.RawMessage(`{"step":"complete","generation":1}`)}
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	clear(calls)
	mu.Unlock()
	slowEnabled.Store(true)
	// Bound a regression failure too; this is not the product's pass budget.
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	started := time.Now()
	_, reconcileErr := m.reconcile(ctx, c.Ref())
	elapsed := time.Since(started)
	if elapsed >= 10*time.Second {
		t.Fatalf("actual Manager.reconcile took %s (error %v), want <10s", elapsed, reconcileErr)
	}
	if reconcileErr != nil {
		t.Fatalf("unexpected fixture/provider error instead of bounded observation: %v", reconcileErr)
	}
	sweeps := 1
	if joinDeployed {
		sweeps = 2
	}
	if n := platformReads.Load(); n < 8 || n > int32(12*sweeps) {
		t.Fatalf("platform reads per measured pass = %d, want one member sweep plus pinned proxy inventory", n)
	}
	mu.Lock()
	total := 0
	for action, count := range calls {
		total += count
		limit := 1
		// Pinned instance and logical router reads must remain independent.
		// The test HTTP server aggregates those two transport identities.
		if strings.HasPrefix(action, c.Name+"mx ") {
			limit = 2
		}
		// Failed optional Galera attempts may retry within their finite budget.
		if strings.HasSuffix(action, "/galera-status") {
			limit = 4
		}
		limit *= sweeps
		if count > limit {
			t.Errorf("unbounded repeated member action %s: %d calls", action, count)
		}
	}
	archiveCalls := calls[c.Name+"m2 /v1/mysql/archive-status"]
	mu.Unlock()
	if total == 0 || total > 24*sweeps {
		t.Errorf("agent actions per Manager pass = %d, want bounded real observations", total)
	}
	if archiveCalls != 1 {
		t.Fatalf("archive status calls = %d, want one proven writer observation and no unbudgeted resample", archiveCalls)
	}
	if mutations.Load() != 0 {
		t.Fatal("quiet reconciliation attempted native mutations")
	}
	stored, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Status.Members) != 4 || stored.Status.ObservedAt.IsZero() {
		t.Fatalf("bounded reconcile did not publish an actual complete member observation: %+v", stored.Status)
	}
	for _, member := range stored.Status.Members {
		expectedHealth := !joinDeployed || member.Name != c.Name+"m0"
		if member.Health != expectedHealth || !member.Agent.Healthy {
			t.Fatalf("optional quorum work withdrew proven member health: %+v", member)
		}
	}
	if stored.Status.Backup.LastBackup == nil || !stored.Status.Backup.LastBackup.Equal(completed) {
		t.Fatal("actual successful full inventory was not observed")
	}
	var state struct {
		MySQL struct {
			Members []struct {
				Name  string `json:"name"`
				Stage string `json:"stage"`
			} `json:"members"`
			Incidents []struct {
				DetectedAt time.Time `json:"detectedAt"`
				BaselineAt time.Time `json:"baselineAt"`
			} `json:"gtidIncidents"`
		} `json:"mysql"`
	}
	if err := json.Unmarshal(stored.Internal, &state); err != nil {
		t.Fatal(err)
	}
	if len(state.MySQL.Incidents) != 1 || !state.MySQL.Incidents[0].DetectedAt.Equal(detected) || !state.MySQL.Incidents[0].BaselineAt.IsZero() {
		t.Fatal("read-only reconciliation erased or acknowledged the old GTID latch")
	}
	if joinDeployed {
		joined := false
		for _, member := range state.MySQL.Members {
			if member.Name == c.Name+"m0" && member.Stage == "joined" {
				joined = true
			}
		}
		if !joined {
			t.Fatal("formed deployed member did not complete joined bookkeeping within the bounded pass")
		}
	}
	if joinDeployed {
		var previous domain.ClusterStatus
		select {
		case previous = <-publishedBeforeInvalidation:
		case <-time.After(time.Second):
			t.Fatal("identity invalidation did not exercise a second actual native sweep")
		}
		if !stored.Status.ObservedAt.Equal(previous.ObservedAt) || stored.Status.ObservationEpoch != previous.ObservationEpoch {
			t.Fatal("budget deferral renewed an invalidated observation")
		}
		for i, member := range stored.Status.Members {
			if member.Name != previous.Members[i].Name || member.Health != previous.Members[i].Health ||
				member.Agent.Healthy != previous.Members[i].Agent.Healthy || !member.Agent.LastSeen.Equal(previous.Members[i].Agent.LastSeen) {
				t.Fatalf("budget exhaustion changed last-published member facts: %+v, previous %+v", member, previous.Members[i])
			}
		}
		unknown := false
		for _, condition := range stored.Status.Conditions {
			unknown = unknown || condition.Type == "Healthy" && condition.Status == "Unknown" && condition.Reason == "ObservationBudgetExhausted"
		}
		if !unknown {
			t.Fatal("deferred native proof was advertised as a current observation")
		}
	}
	finished, err := st.GetOperation(t.Context(), op.ID)
	if err != nil || finished.Phase != domain.PhaseFailed {
		t.Fatalf("terminal backup history changed: %+v %v", finished, err)
	}
	retained, err := st.GetBackup(t.Context(), c.Ref(), backup.Name)
	if err != nil || retained.Phase != domain.PhaseSucceeded {
		t.Fatalf("completed full resource changed with failed terminal operation: %+v %v", retained, err)
	}
	t.Logf("actual Manager pass: %s, %d agent actions, error=%v", elapsed, total, reconcileErr)
}

func mysqlBudgetNativeUUID(member string) string {
	switch {
	case strings.HasSuffix(member, "m0"):
		return "aaaaaaaa-bbbb-cccc-dddd-000000000001"
	case strings.HasSuffix(member, "m1"):
		return "aaaaaaaa-bbbb-cccc-dddd-000000000002"
	case strings.HasSuffix(member, "m2"):
		return "aaaaaaaa-bbbb-cccc-dddd-000000000003"
	default:
		return "aaaaaaaa-bbbb-cccc-dddd-000000000004"
	}
}

func TestMySQLManagerOfflineRecoveryOutlastsObservationPass(t *testing.T) {
	const group = "11111111-2222-3333-4444-555555555555"
	p := mysql.New(mysql.Options{})
	m, st := newTestManager(t, p)
	backend := mysqlBudgetBackend{Backend: fake.New(fake.Options{})}
	m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: backend}
	c := &domain.Cluster{Name: "recovergr", Generation: 1, Spec: domain.ClusterSpec{Provider: "mysql", Topology: domain.Topology{Type: "group"}}}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Backup.PITR.Enabled = false
	c.Status = domain.ClusterStatus{Phase: domain.PhaseDegraded, ObservedGeneration: 1}
	var members []map[string]any
	for i := range 4 {
		name, role, component := fmt.Sprintf("%sm%d", c.Name, i), "data", c.Spec.Components.Engine
		if i == 3 {
			name, role, component = c.Name+"mx", "proxy", c.Spec.Components.Extra["proxy"]
		}
		members = append(members, map[string]any{
			"name": name, "role": role, "index": i, "serviceId": name + "-id", "stage": "joined",
			"version": c.Spec.Version, "resources": component.Resources, "replicas": 1,
			"configVersion": 1, "initialized": role == "data", "serverUUID": mysqlBudgetNativeUUID(name),
		})
		if role == "data" {
			members[len(members)-1]["volume"] = name + "v"
			members[len(members)-1]["volumeId"] = name + "v-id"
		}
	}
	internal := map[string]any{"mysql": map[string]any{
		"uid": "offline-manager-fixture", "nextIndex": 3, "configVersion": 1, "topology": "group", "members": members,
		"formation": map[string]any{"groupName": group, "formationEpoch": 1, "seed": c.Name + "m0", "state": "formed"},
	}}
	var err error
	if c.Internal, err = json.Marshal(internal); err != nil {
		t.Fatal(err)
	}
	if err = st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		name := member["name"].(string)
		if err := st.SetAgentToken(t.Context(), &domain.AgentToken{Namespace: c.Ref().Namespace, Cluster: c.Name, Member: name, Token: name, Hash: secrets.Hash(name)}); err != nil {
			t.Fatal(err)
		}
	}
	var armed atomic.Bool
	var probes atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		switch r.URL.Path {
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 100000})
		case "/v1/mysql/status":
			status := myspec.StatusResponse{ServerUUID: mysqlBudgetNativeUUID(name), MemberState: "ONLINE", MemberRole: "PRIMARY", GroupMembers: 3, DataIdentity: myspec.DataIdentity{Initialized: true, FormationEpoch: 1}}
			if armed.Load() {
				status.MemberState, status.MemberRole, status.GroupMembers = "OFFLINE", "", 0
			}
			json.NewEncoder(w).Encode(status)
		case "/v1/mysql/offline-gtid":
			probes.Add(1)
			if name == c.Name+"m0" {
				timer := time.NewTimer(5 * time.Second)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-r.Context().Done():
					return
				}
			}
			json.NewEncoder(w).Encode(myspec.OfflineGTIDResponse{Stopped: true, ServerUUID: mysqlBudgetNativeUUID(name), GTIDExecuted: group + ":1-9", DataIdentity: myspec.DataIdentity{Initialized: true}})
		case "/v1/mysql/proxy-status":
			json.NewEncoder(w).Encode(myspec.ProxyStatusResponse{})
		default:
			t.Errorf("unexpected native action %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	m.opts.Context.Agents = mysqlBudgetAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	rc := m.opts.Context
	rc.Cluster, rc.Backend, rc.Namespace = c, backend, c.Ref().Namespace
	if _, err = p.Sync(t.Context(), &rc); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(c.Internal, &internal); err != nil {
		t.Fatal(err)
	}
	owned := internal["mysql"].(map[string]any)
	owned["formation"].(map[string]any)["reformation"] = map[string]any{"state": "observing"}
	for _, value := range owned["members"].([]any) {
		member := value.(map[string]any)
		if member["role"] == "data" {
			member["offline"] = true
		}
	}
	if c.Internal, err = json.Marshal(internal); err != nil {
		t.Fatal(err)
	}
	if err = st.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err = m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	updated, err := st.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(updated.Internal, &internal); err != nil {
		t.Fatal(err)
	}
	formation := internal["mysql"].(map[string]any)["formation"].(map[string]any)
	if probes.Load() != 3 || formation["state"] != "bootstrapping" || formation["seed"] != c.Name+"m0" {
		t.Fatalf("Manager Sync failed offline recovery: probes=%d formation=%v", probes.Load(), formation)
	}
}
