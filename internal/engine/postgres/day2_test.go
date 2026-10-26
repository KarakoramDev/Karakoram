// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestHAScaleLearnersAndNeverRemoveWriter(t *testing.T) {
	p, rc, b := setup(t, "ha")
	converge(t, p, rc)
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	rc.Cluster.Generation++
	s := converge(t, p, rc)
	if len(s.data()) != 5 || len(b.etcd) != 5 {
		t.Fatal("scale-out did not create five voters")
	}
	var mutations []string
	for _, action := range b.actions {
		if strings.HasPrefix(action, "add-learner:") || strings.HasPrefix(action, "promote:") {
			mutations = append(mutations, action)
		}
	}
	want := []string{"add-learner:demop3:", "promote::demop3-etcd", "add-learner:demop4:", "promote::demop4-etcd"}
	if !reflect.DeepEqual(mutations, want) {
		t.Fatalf("unsafe learner flow: %v", mutations)
	}
	b.leader = "demop4"
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	rc.Cluster.Generation++
	s = converge(t, p, rc)
	if len(s.data()) != 3 || len(b.etcd) != 3 || b.leader == "demop4" {
		t.Fatal("scale-in did not preserve a writable quorum")
	}
	for _, retired := range []string{"demop3", "demop4"} {
		if b.services[retired] != nil || b.services[retired+"v"] != nil {
			t.Fatal("retired member infrastructure leaked")
		}
	}
	if s.NextIndex != 5 {
		t.Fatal("retired identities became reusable")
	}
}

func TestReadyPreservesLearnerIdentity(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.data()[1]
	m.EtcdLearner = true
	id := m.EtcdID
	b.statusOverrides = map[string]pgspec.StatusResponse{
		m.Name: {Role: "replica", Running: true, Streaming: true},
	}
	ok, err := ready(t.Context(), rc, m)
	if err != nil || !ok {
		t.Fatalf("streaming learner not ready: %v", err)
	}
	if id == "" || m.EtcdID != id {
		t.Fatalf("learner status erased known identity: %q -> %q", id, m.EtcdID)
	}
}

func TestPromoteLearnerRecoversMissingIdentity(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.newDataMember(rc.Cluster)
	if err := p.addLearner(t.Context(), rc, s, m); err != nil {
		t.Fatal(err)
	}
	id := m.EtcdID
	m.Stage = "joined"
	m.EtcdID = ""
	if err := p.promoteLearner(t.Context(), rc, s, m); err != nil {
		t.Fatal(err)
	}
	if m.EtcdID != id || m.EtcdLearner || b.etcd[id].Learner {
		t.Fatalf("learner identity/promotion not recovered: %+v", m)
	}
}

type unavailableEtcdVoterAgents struct {
	fakeAgents
	calls      []string
	rpcFailure bool
}

func (a *unavailableEtcdVoterAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	a.calls = append(a.calls, name)
	if name == "demop1" {
		if !a.rpcFailure {
			return nil, errors.New("voter unavailable")
		}
		return agentapi.NewClient(agentapi.ClientOptions{
			BaseURL: a.url, CAPEM: a.ca, Token: name,
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("membership endpoint unavailable")
			},
		})
	}
	return a.fakeAgents.Dial(ctx, ref, name)
}

func TestReplacementMembershipUsesReachableVoter(t *testing.T) {
	for _, failure := range []string{"dial", "membership", "learner"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			b.leader = "demop2"
			m := s.newDataMember(rc.Cluster)
			s.Replacement = &replacementState{Old: "demop0", New: m.Name}
			agents := &unavailableEtcdVoterAgents{fakeAgents: rc.Agents.(fakeAgents), rpcFailure: failure == "membership"}
			rc.Agents = agents
			if failure == "learner" {
				s.find("demop1").EtcdLearner = true
			}
			if err := p.addLearner(t.Context(), rc, s, m); err != nil {
				t.Fatal(err)
			}
			m.Stage = "joined"
			if err := p.promoteLearner(t.Context(), rc, s, m); err != nil {
				t.Fatal(err)
			}
			if err := removeEtcdMember(t.Context(), rc, s, s.find("demop0")); err != nil {
				t.Fatal(err)
			}
			want := []string{"demop1", "demop2", "demop1", "demop2", "demop1", "demop2"}
			if failure == "learner" {
				want = []string{"demop2", "demop2", "demop2"}
			}
			if !reflect.DeepEqual(agents.calls, want) {
				t.Fatalf("membership did not skip predecessor/learner or fall through to reachable voter: %v", agents.calls)
			}
			if m.EtcdLearner || m.EtcdID == "" || len(b.etcd) != 3 {
				t.Fatal("replacement membership did not converge through the surviving voter")
			}
		})
	}
}

func TestReplacementResumesEveryCheckpoint(t *testing.T) {
	for _, target := range []string{"demop1", "demop0"} {
		t.Run(target, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			converge(t, p, rc)
			payload, _ := json.Marshal(map[string]string{"member": target})
			// A learner cannot serve its local member-list RPC; PostgreSQL
			// readiness still succeeds without an etcd identity in status.
			b.statusOverrides = map[string]pgspec.StatusResponse{
				"demop3": {Role: "replica", Running: true, Streaming: true},
			}
			op := &domain.Operation{ID: "replace-one", Kind: domain.OperationReplaceMember, Payload: payload}
			seen := map[string]bool{}
			done := false
			for range 200 {
				// Reconstructing the provider and decoding private state on every
				// pass models loss of all process-local state at every checkpoint.
				p = New(p.options)
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				if result.Checkpoint != nil {
					op.Checkpoint = append(json.RawMessage(nil), result.Checkpoint...)
				}
				seen[result.Step] = true
				if result.Done {
					done = true
					break
				}
				if p.SyncDuringOperation(op) {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !done {
				t.Fatal("replacement did not finish")
			}
			for _, checkpoint := range []string{"join", "fence", "remove-etcd", "promote", "retire", "done"} {
				if !seen[checkpoint] {
					t.Fatalf("checkpoint %s was not exercised", checkpoint)
				}
			}
			// The injected response represents the learner, not the promoted
			// voter. Re-enable full DCS/lineage observations after cutover.
			delete(b.statusOverrides, "demop3")
			s := converge(t, p, rc)
			if s.find(target) != nil || s.find("demop3") == nil || len(s.data()) != 3 || len(b.etcd) != 3 {
				t.Fatal("replacement did not cut over membership")
			}
			if b.services[target] != nil || b.services[target+"v"] != nil {
				t.Fatal("replacement leaked predecessor infrastructure")
			}
			if b.leader == target {
				t.Fatal("replacement removed the current writer")
			}
		})
	}
}

func TestMinorUpgradePinsAndAdmission(t *testing.T) {
	p, rc, _ := setup(t, "single")
	rc.Cluster.Spec.Version = "17.10.1"
	converge(t, p, rc)
	old := *rc.Cluster
	rc.Cluster.Spec.Version = "17.11.1"
	if err := p.Validate(t.Context(), &old, rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Generation++
	s := converge(t, p, rc)
	if s.data()[0].Version != "17.11.1" {
		t.Fatal("minor upgrade did not converge")
	}
	for _, test := range []struct {
		from, to string
		allowed  bool
	}{
		{"17.10.1", "17.11.1", true},
		{"16.15.1", "17.10.1", true},
		{"17.11.1", "18.6.1", true},
		{"17.11.1", "17.10.1", false},
		{"17.11.1", "16.15.1", false},
		{"16.15.1", "18.6.1", false},
		{"17.11.1", "17.11.1", false},
	} {
		if got := upgradeAllowed(test.from, test.to); got != test.allowed {
			t.Fatalf("upgrade %s -> %s = %v", test.from, test.to, got)
		}
	}
	if strings.Join(PrepareCommands("17.10.1"), "\n") == strings.Join(PrepareCommands("17.11.1"), "\n") {
		t.Fatal("minor catalog versions install identical package pins")
	}
}

func TestMajorUpgradeRequiresBackup(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	rc.Cluster.Spec.Version = "16.15.1"
	converge(t, p, rc)
	rc.Cluster.Spec.Version = "17.11.1"
	if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("unprotected major upgrade accepted: %v", err)
	}
	s, _ := load(rc.Cluster)
	if s.Upgrade != nil {
		t.Fatal("maintenance began before a backup was available")
	}
}

func TestMajorUpgradeResumesEveryCheckpoint(t *testing.T) {
	p, rc, b := setup(t, "ha")
	rc.Cluster.Spec.Version = "16.15.1"
	initial := converge(t, p, rc)
	proxyID := initial.find("demopx").ServiceID
	b.actions, b.reloads = nil, nil
	st := rc.Store.(*store.Store)
	storage := &domain.BackupStorage{
		Name: "archive", Namespace: "default", Type: "s3",
		EndpointURL: "https://objects.example.com", Bucket: "backups", Region: "region-1",
	}
	if err := st.CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = storage.Name
	if err := st.CreateBackup(t.Context(), &domain.Backup{
		Namespace: "default", Cluster: "demo", Name: "before-upgrade",
		Phase: domain.PhaseSucceeded, Type: "full", Storage: storage.Name,
		SourceVersion: "16.15.1", Provider: "postgres", Postgres: &domain.PostgresBackup{Label: "20261017-010000F"},
	}); err != nil {
		t.Fatal(err)
	}
	backupConfig, err := loadBackupState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	backupConfig.Replicas = true
	if err := saveBackupState(rc.Cluster, backupConfig); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Version = "17.11.1"
	rc.Cluster.Generation++
	seen := map[string]bool{}
	for range 200 {
		p = New(p.options)
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		backupConfig, err := loadBackupState(rc.Cluster)
		if err != nil || backupConfig.Replicas {
			t.Fatal("major upgrade retained the previous-major replica backup method")
		}
		s, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.Upgrade != nil {
			seen[s.Upgrade.Stage] = true
		} else if seen["finalize"] {
			break
		}
	}
	for _, checkpoint := range []string{"prepare", "pause", "stop-replicas", "stop", "check", "link", "dcs-reset", "primary", "replicas", "stanza", "backup", "finalize"} {
		if !seen[checkpoint] {
			t.Fatalf("upgrade checkpoint %s not exercised", checkpoint)
		}
	}
	s := converge(t, p, rc)
	for _, m := range s.data() {
		if m.Version != "17.11.1" || m.Offline {
			t.Fatal("upgraded member remained on old version or offline")
		}
	}
	proxy := s.find("demopx")
	if proxy.ServiceID != proxyID || proxy.Version != rc.Cluster.Spec.Version || proxy.Stage != "joined" || !slices.Contains(b.reloads, proxy.Name) || slices.Contains(b.actions, "deploy:"+proxy.Name) {
		t.Fatal("major upgrade must converge proxy metadata without redeployment", proxy, b.actions, b.reloads)
	}
	config, err := p.MemberConfig(t.Context(), rc, proxy.Name)
	if err != nil {
		t.Fatal(err)
	}
	var proxySpec pgspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &proxySpec); err != nil {
		t.Fatal(err)
	}
	if proxySpec.Major != 17 || proxySpec.Version != rc.Cluster.Spec.Version || proxySpec.Proxy == nil || len(proxySpec.Proxy.Servers) != len(s.data()) {
		t.Fatal("major upgrade did not publish the new proxy metadata and backends")
	}
	actions := strings.Join(b.actions, "\n")
	check, run, reset := strings.Index(actions, "upgrade-check"), strings.Index(actions, "upgrade-run"), strings.Index(actions, "dcs-reset")
	if check < 0 || run <= check || reset <= run {
		t.Fatal("unsafe major upgrade action order")
	}
	backups, err := st.ListBackups(t.Context(), rc.Cluster.Ref())
	if err != nil || len(backups) != 2 {
		t.Fatalf("fresh full backup was not queued exactly once: %d %v", len(backups), err)
	}
}

func TestTLSStagingDoesNotPublishUntilMemberTurn(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	old, err := secret(t.Context(), rc, "tls/servers/demop0.crt")
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureCertificates(t.Context(), rc, s, true); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	config, err := p.MemberConfig(t.Context(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.ServerCert.CertPEM != string(old) {
		t.Fatal("renewal published a certificate before its rolling reload")
	}
	converge(t, p, rc)
	fresh, err := secret(t.Context(), rc, "tls/servers/demop0.crt")
	if err != nil || string(fresh) == string(old) {
		t.Fatal("renewal did not activate staged certificate")
	}
	if _, err := secret(t.Context(), rc, "tls/servers/demop0.pending.crt"); !missing(err) {
		t.Fatal("activated certificate retained staging data")
	}
}

func TestMetricsCatalogCoversAgentVocabulary(t *testing.T) {
	metrics := New(Options{}).Metrics()
	if len(metrics) != len(pgspec.MetricNames) {
		t.Fatal("incomplete metrics vocabulary")
	}
	for i, descriptor := range metrics {
		if descriptor.Exposition != pgspec.MetricNames[i] || descriptor.Name == "" || descriptor.Scale != 1 {
			t.Fatal("metric descriptor does not match agent exposition")
		}
		if strings.HasSuffix(descriptor.Exposition, "_bytes") && descriptor.Unit != "bytes" || strings.HasSuffix(descriptor.Exposition, "_seconds") && descriptor.Unit != "seconds" {
			t.Fatal("metric units do not match agent units")
		}
		if strings.HasSuffix(descriptor.Exposition, "_total") && descriptor.Unit != "operations/second" {
			t.Fatal("counter rates must be labeled per second")
		}
	}
}

type rejectedVolumeBackend struct{ *fakeBackend }

func (b rejectedVolumeBackend) DeleteServiceByID(ctx context.Context, name, id string) (infra.Op, error) {
	if strings.HasSuffix(name, "v") {
		return infra.Op{}, infra.ErrRejected
	}
	return b.fakeBackend.DeleteServiceByID(ctx, name, id)
}

func TestRetirementWaitsForProxyBackendRemovalAcknowledgement(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.find("demop2")
	m.Removing = true
	priorVersion := s.find("demopx").ConfigVersion
	b.appliedVersions = map[string]int64{"demopx": priorVersion}
	b.reloadFailures = 1
	done, err := p.retireMember(t.Context(), rc, s, m)
	if err != nil || done || b.services[m.Name] == nil {
		t.Fatalf("retired runtime before publishing proxy exclusion: done=%v err=%v", done, err)
	}
	drainVersion := m.ProxyDrainVersion
	if drainVersion <= priorVersion {
		t.Fatal("proxy exclusion did not get a new configuration version")
	}
	config, err := p.MemberConfig(t.Context(), rc, "demopx")
	if err != nil {
		t.Fatal(err)
	}
	var proxySpec pgspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &proxySpec); err != nil {
		t.Fatal(err)
	}
	if config.ConfigVersion != drainVersion || slices.Contains(proxySpec.Proxy.Servers, m.Name) || len(proxySpec.Proxy.Servers) != 2 {
		t.Fatalf("published proxy configuration retained retiring backend: %+v", proxySpec.Proxy)
	}
	// Resume from the published checkpoint, including a lost/failed reload.
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m = s.find(m.Name)
	if _, err := p.retireMember(t.Context(), rc, s, m); err == nil || b.services[m.Name] == nil {
		t.Fatal("failed proxy reload did not retain the old runtime")
	}
	if s.ConfigVersion != drainVersion || m.ProxyDrainVersion != drainVersion {
		t.Fatal("resuming retirement republished the drain checkpoint")
	}
	if done, err := p.retireMember(t.Context(), rc, s, m); err != nil || done || b.services[m.Name] == nil {
		t.Fatalf("reload response was mistaken for applied-version acknowledgement: done=%v err=%v", done, err)
	}
	if b.appliedVersions["demopx"] != drainVersion {
		t.Fatal("proxy did not acknowledge the exclusion")
	}
	if _, err := p.retireMember(t.Context(), rc, s, m); err != nil || b.services[m.Name] != nil {
		t.Fatalf("acknowledged proxy exclusion did not permit runtime retirement: %v", err)
	}
}

func TestRetirementTransfersRejectedVolumeToJanitor(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.data()[2]
	m.Removing = true
	rc.Backend = rejectedVolumeBackend{b}
	for range 10 {
		if busy, err := p.observePending(t.Context(), rc, s); busy || err != nil {
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		done, err := p.retireMember(t.Context(), rc, s, m)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	orphan, err := rc.Store.GetOrphanedVolume(t.Context(), "default", "demop2v")
	if err != nil || orphan.ServiceID != "demop2v-id" || s.find("demop2") != nil {
		t.Fatal("rejected owned volume was lost instead of journaled")
	}
}

func TestHAResourceAndMinorRollsMoveWriterLast(t *testing.T) {
	for _, change := range []string{"resources", "minor"} {
		t.Run(change, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			rc.Cluster.Spec.Version = "17.10.1"
			converge(t, p, rc)
			b.actions = nil
			prefix := "resize:"
			if change == "minor" {
				rc.Cluster.Spec.Version = "17.11.1"
				prefix = "deploy:"
			} else {
				rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
			}
			rc.Cluster.Generation++
			converge(t, p, rc)
			var order []string
			for _, action := range b.actions {
				if strings.HasPrefix(action, prefix) && !strings.HasSuffix(action, "px") {
					order = append(order, strings.TrimPrefix(action, prefix))
				}
			}
			if len(order) != 3 || order[0] == "demop0" || order[2] != "demop0" || b.leader == "" {
				t.Fatalf("unsafe rolling order %v, writer %s", order, b.leader)
			}
		})
	}
}

func TestSinglePendingRestartRestartsPostgresBeforeReady(t *testing.T) {
	_, rc, b := setup(t, "single")
	p := New(Options{})
	s := converge(t, p, rc)
	m := s.data()[0]
	status, err := live(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	status.PendingRestart = true
	b.statusOverrides = map[string]pgspec.StatusResponse{m.Name: status}
	ok, err := ready(t.Context(), rc, m)
	if err != nil || ok {
		t.Fatalf("pending postmaster change reported ready: ready=%v err=%v", ok, err)
	}
	if !slices.Contains(b.actions, "/v1/processes/postgres/restart:"+m.Name) {
		t.Fatal("single PostgreSQL was not restarted through the supervisor")
	}
	ok, err = ready(t.Context(), rc, m)
	if err != nil || !ok {
		t.Fatalf("restarted PostgreSQL did not become ready: ready=%v err=%v", ok, err)
	}
}

func TestDiskGrowthRequiresBackendCapability(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc)
	rc.Cluster.Spec.Components.Engine.Resources.MaxDisk = 6
	rc.Cluster.Generation++
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("disk growth was attempted without backend capability")
	}
	if b.services["demop0v"].Resources.MaxDisk != 5 {
		t.Fatal("unsupported disk growth mutated the owned volume")
	}
}

func TestHASuspendResumePreservesEtcdIdentities(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	uid := s.UID
	volumes := map[string]string{}
	for _, m := range s.data() {
		volumes[m.Volume] = m.VolumeID
	}
	done := false
	for range 40 {
		var err error
		done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done || len(b.services) != 3 {
		t.Fatal("HA suspension did not retain exactly its three data volumes")
	}
	s = converge(t, p, rc)
	if s.UID != uid {
		t.Fatal("resume changed the DCS cluster identity")
	}
	for _, m := range s.data() {
		if volumes[m.Volume] != m.VolumeID {
			t.Fatal("resume replaced a persistent PGDATA/etcd volume")
		}
	}
	retained, err := rc.Store.ListRetainedVolumes(t.Context(), rc.Cluster.Ref())
	if err != nil || len(retained) != 0 {
		t.Fatal("resumed volumes were still classified as retained")
	}
}

func TestStatusSurfacesReplicaAndQuorumFailures(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.statusOverrides = map[string]pgspec.StatusResponse{}
	for _, m := range s.data() {
		status, err := live(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		status.EtcdHealthy = false
		if m.Name == "demop1" {
			status.LagBytes = 32 << 20
		}
		if m.Name == "demop2" {
			status.Role, status.Running, status.Streaming = "stopped", false, false
		}
		b.statusOverrides[m.Name] = status
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	conditions := map[string]domain.Condition{}
	for _, condition := range status.Conditions {
		conditions[condition.Type] = condition
	}
	if conditions["EtcdQuorum"].Status != "False" || conditions["Replication"].Status != "False" || status.Phase == domain.PhaseReady {
		t.Fatal("quorum and replication failures were hidden")
	}
	if !strings.Contains(conditions["Replication"].Message, "demop1") || !strings.Contains(conditions["Replication"].Message, "demop2") {
		t.Fatal("replica condition does not identify affected members")
	}
}

func TestReinitRejectsWriterAndWaitsForStreaming(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	converge(t, p, rc)
	op := &domain.Operation{Kind: "reinit-member", Payload: json.RawMessage(`{"member":"demop0"}`)}
	if _, err := p.RunOperation(t.Context(), rc, op); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal("primary reinitialization was accepted")
	}
	op.Payload = json.RawMessage(`{"member":"demop1"}`)
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil || result.Done || len(result.Checkpoint) == 0 {
		t.Fatal("replica reinitialization did not checkpoint its request")
	}
	op.Checkpoint = result.Checkpoint
	result, err = p.RunOperation(t.Context(), rc, op)
	if err != nil || !result.Done {
		t.Fatal("streaming rebuilt replica did not finish reinitialization")
	}
}

func TestPublishedReloadRetriesAfterLostRequest(t *testing.T) {
	p, rc, b := setup(t, "single")
	s := converge(t, p, rc)
	b.appliedVersions = map[string]int64{
		"demop0": s.find("demop0").ConfigVersion,
		"demopx": s.find("demopx").ConfigVersion,
	}
	b.reloadFailures = 1
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"parameters":{"work_mem":"8MB"}}`)
	rc.Cluster.Generation++
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("failed reload request was not reported")
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	// Proxies receive their capacity/configuration publication before data.
	if s.find("demopx").Stage != "deployed" {
		t.Fatal("published reload lost its pending acknowledgement checkpoint")
	}
	converge(t, p, rc)
	if b.appliedVersions["demop0"] != s.ConfigVersion || b.appliedVersions["demopx"] != s.ConfigVersion {
		t.Fatal("reconciliation did not retry the lost reload")
	}
}

func TestCertificateRenewalThresholdAndProvidedRotation(t *testing.T) {
	p, rc, _ := setup(t, "single")
	s := converge(t, p, rc)
	crt, err := secret(t.Context(), rc, "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	key, err := secret(t.Context(), rc, "tls/ca.key")
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(crt, key)
	if err != nil {
		t.Fatal(err)
	}
	short, shortKey, err := ca.Issue(pki.CertRequest{
		CommonName: "demop0", DNSNames: []string{"demop0", "localhost"},
		Validity: 29 * 24 * time.Hour, Server: true, Client: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for suffix, value := range map[string][]byte{".crt": short, ".key": shortKey} {
		if err := putSecret(t.Context(), rc, "tls/servers/demop0"+suffix, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if !s.find("demop0").TLSPending {
		t.Fatal("certificate inside the thirty-day renewal window was not staged")
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	converge(t, p, rc)
	rc.Cluster.Spec.TLS.CA = "userProvided"
	// This pair is valid but does not cover either the data or proxy identity.
	bad, badKey, err := ca.Issue(pki.CertRequest{
		CommonName: "foreign", DNSNames: []string{"foreign"}, Server: true, Client: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, "tls/user.crt", bad); err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, "tls/user.key", badKey); err != nil {
		t.Fatal(err)
	}
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureCertificates(t.Context(), rc, s, false); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal("provided certificate missing member SANs was accepted")
	}
	valid, validKey, err := ca.Issue(pki.CertRequest{
		CommonName: "demo", DNSNames: []string{"demop0", "demopx", "localhost"},
		IPs:    []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		Server: true, Client: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, "tls/user.crt", valid); err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, "tls/user.key", validKey); err != nil {
		t.Fatal(err)
	}
	converge(t, p, rc)
	active, err := secret(t.Context(), rc, "tls/servers/demop0.crt")
	if err != nil || string(active) != string(valid) {
		t.Fatal("provided certificate pair did not roll into active configuration")
	}
}
