// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const withdrawnPXCVersion = "8.4.11-11.1"
const replacementPXCVersion = "8.4.11-12.1"

type withdrawnEvents struct {
	events []domain.Event
	err    error
}

func (e *withdrawnEvents) Record(_ context.Context, event domain.Event) error {
	if e.err != nil {
		return e.err
	}
	if event.Reason == "WithdrawnVersionReplaced" {
		e.events = append(e.events, event)
	}
	return nil
}

type withdrawnDeploymentBackend struct {
	*fakeBackend
	t    *testing.T
	data int
	rc   *engine.Context
}

func assertReplacementPrepare(t *testing.T, runtime infra.RuntimeSpec) {
	t.Helper()
	commands := strings.Join(runtime.Prepare, "\n")
	if strings.Contains(commands, "Percona-XtraDB-Cluster_8.4.11-11.1_") {
		t.Fatal("deployment renders withdrawn upstream artifact")
	}
	if runtime.Volume != "" {
		for _, want := range []string{"Percona-XtraDB-Cluster_8.4.11-12.1_Linux.x86_64.glibc2.35-minimal.tar.gz", "8ae35d537c467320fd16e6c5858a4701430923f01da8dc6a9a377e653e70bbc4", "https://downloads.percona.com/downloads/Percona-XtraDB-Cluster-84/"} {
			if !strings.Contains(commands, want) {
				t.Fatalf("replacement preparation missing %s", want)
			}
		}
	}
}

func (b *withdrawnDeploymentBackend) Deploy(ctx context.Context, name string, deployment infra.Deployment) (infra.Op, error) {
	assertReplacementPrepare(b.t, deployment.Runtime)
	if b.rc != nil {
		s, _ := load(b.rc.Cluster)
		if s.Rolling != nil && s.Rolling.To != b.rc.Cluster.Spec.Version {
			b.t.Fatal("deployment changed the raw maintenance target", s.Rolling)
		}
	}
	if deployment.Runtime.Volume != "" {
		b.data++
	}
	return b.fakeBackend.Deploy(ctx, name, deployment)
}

// Model an already-running release without ever downloading its withdrawn bytes.
func retainWithdrawnMembers(t *testing.T, rc *engine.Context, b *fakeBackend) {
	t.Helper()
	rc.Cluster.Spec.Version = withdrawnPXCVersion
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		m.Version = withdrawnPXCVersion
		st := b.native[m.Name]
		st.Version = withdrawnPXCVersion
		b.native[m.Name] = st
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
}

func TestPXCWithdrawnValidationAndCatalog(t *testing.T) {
	p := New(Options{Provider: "pxc"})
	c := &domain.Cluster{Name: "sample"}
	p.ApplyDefaults(&c.Spec)
	info := p.Describe()
	if c.Spec.Version != replacementPXCVersion || !info.Versions[0].Default || info.Versions[0].Version != replacementPXCVersion || info.Versions[1].Status != "deprecated" || info.Versions[1].Default {
		t.Fatal(c.Spec.Version, info.Versions)
	}
	advertised := false
	for _, path := range info.UpgradePaths {
		advertised = advertised || path.From == withdrawnPXCVersion && path.To == replacementPXCVersion
	}
	if !advertised {
		t.Fatal("replacement upgrade path not advertised")
	}
	c.Spec.Version = withdrawnPXCVersion
	if err := p.Validate(t.Context(), nil, c); err == nil || !strings.Contains(err.Error(), replacementPXCVersion) {
		t.Fatal("withdrawn create must name replacement", err)
	}
	// A data source must not bypass withdrawal admission, even if it is invalid.
	c.Spec.DataSource = &domain.DataSource{}
	if err := p.Validate(t.Context(), nil, c); err == nil || !strings.Contains(err.Error(), replacementPXCVersion) {
		t.Fatal("withdrawn restore create must name replacement", err)
	}
	c.Spec.DataSource = nil
	old := *c
	if err := p.Validate(t.Context(), &old, c); err != nil {
		t.Fatal("retained specification rejected", err)
	}
	c.Spec.Version = replacementPXCVersion
	if err := p.Validate(t.Context(), &old, c); err != nil {
		t.Fatal("replacement upgrade rejected", err)
	}
	if deployVersion("mysql", withdrawnPXCVersion) != withdrawnPXCVersion || deployVersion("pxc", "8.0.46-38.1") != "8.0.46-38.1" {
		t.Fatal("withdrawal mapping escaped PXC 8.4 scope")
	}
}

func TestPXCWithdrawnSpecDoesNotRoll(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "all-withdrawn", true: "mixed"}[mixed], func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			retainWithdrawnMembers(t, rc, b)
			s, _ := load(rc.Cluster)
			if mixed {
				s.data()[0].Version = replacementPXCVersion
			}
			if changed, err := p.reconcileRolling(t.Context(), rc, s); err != nil || changed || s.Rolling != nil {
				t.Fatal("control-plane upgrade must not roll existing members", changed, err, s.Rolling)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			backend := &withdrawnDeploymentBackend{fakeBackend: b, t: t}
			rc.Backend = backend
			for range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				current, _ := load(rc.Cluster)
				if current.Rolling != nil || backend.data != 0 {
					t.Fatal("unchanged withdrawn specification caused a rollout", current.Rolling)
				}
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, condition := range status.Conditions {
				found = found || condition.Type == "VersionWithdrawn" && condition.Status == "True" && strings.Contains(condition.Message, replacementPXCVersion) && strings.Contains(condition.Message, "upgrade-agents is blocked")
			}
			if !found {
				t.Fatal("missing withdrawal condition", status.Conditions)
			}
		})
	}
}

func TestPXCWithdrawnDeploymentsUseReplacement(t *testing.T) {
	for _, action := range []string{"scale-up", "replace-member", "explicit-upgrade", "resource-roll"} {
		t.Run(action, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			retainWithdrawnMembers(t, rc, b)
			events := &withdrawnEvents{}
			rc.Events = events
			backend := &withdrawnDeploymentBackend{fakeBackend: b, t: t, rc: rc}
			rc.Backend = backend
			rc.Cluster.Generation++
			switch action {
			case "scale-up":
				rc.Cluster.Spec.Components.Engine.Replicas = 5
				converge(t, p, rc, b)
			case "replace-member":
				runDay2Operation(t, rc, b, &domain.Operation{ID: "withdrawn-replace", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"demom1"}`)})
			case "explicit-upgrade":
				rc.Cluster.Spec.Version = replacementPXCVersion
				converge(t, p, rc, b)
			case "resource-roll":
				rc.Cluster.Spec.Components.Engine.Resources.MaxCPU = 2
				converge(t, p, rc, b)
			}
			if backend.data == 0 {
				t.Fatal("no data member deployment exercised")
			}
			s, _ := load(rc.Cluster)
			if s.Rolling != nil {
				t.Fatal("replacement version wedged maintenance", s.Rolling)
			}
			for _, m := range s.data() {
				if action == "resource-roll" || action == "explicit-upgrade" || m.Name == "demom3" || m.Name == "demom4" {
					if m.Version != replacementPXCVersion {
						t.Fatal("redeployed member retains withdrawn version", m)
					}
				}
			}
			for _, m := range s.Members {
				if m.Role == "proxy" && m.Version != rc.Cluster.Spec.Version {
					t.Fatal("proxy version was substituted instead of kept raw", m)
				}
			}
			if len(events.events) == 0 {
				t.Fatal("missing replacement event")
			}
			for _, event := range events.events {
				if event.Reason != "WithdrawnVersionReplaced" || event.Type != "Normal" || !strings.Contains(event.Message, withdrawnPXCVersion) || !strings.Contains(event.Message, replacementPXCVersion) || !strings.Contains(event.Message, "Percona withdrew") {
					t.Fatal("incorrect replacement event", event)
				}
			}
		})
	}
}

func TestPXCWithdrawnEventFailureRetriesBeforeChangingIdentity(t *testing.T) {
	p, rc, _ := setupPXC(t)
	events := &withdrawnEvents{err: errors.New("event unavailable")}
	rc.Events = events
	m := &member{Name: "member", Role: "data", Version: withdrawnPXCVersion}
	if err := p.setDeployVersion(t.Context(), rc, m, m.Version); err == nil || m.Version != withdrawnPXCVersion {
		t.Fatal("event failure silently advanced replacement", m, err)
	}
	events.err = nil
	if err := p.setDeployVersion(t.Context(), rc, m, m.Version); err != nil || m.Version != replacementPXCVersion || len(events.events) != 1 {
		t.Fatal("retry lost the replacement event", m, events.events, err)
	}
	if err := p.setDeployVersion(t.Context(), rc, m, withdrawnPXCVersion); err != nil || len(events.events) != 1 {
		t.Fatal("completed replacement recorded a duplicate event", events.events, err)
	}
	proxy := &member{Name: "proxy", Role: "proxy", Version: withdrawnPXCVersion}
	if err := p.setDeployVersion(t.Context(), rc, proxy, withdrawnPXCVersion); err != nil || proxy.Version != withdrawnPXCVersion || len(events.events) != 1 {
		t.Fatal("data-only replacement changed a proxy", proxy, err)
	}
}

func TestPXCWithdrawnProxyResourceCheckpointReplay(t *testing.T) {
	for _, stage := range []string{"resource-scale", "resource-recovery-scale", "resource-recovery-submit"} {
		t.Run(stage, func(t *testing.T) {
			p, rc, base := setupPXC(t)
			converge(t, p, rc, base)
			retainWithdrawnMembers(t, rc, base)
			s, _ := load(rc.Cluster)
			b, _ := proxyScaleFixture(t, rc, s)
			b.servingResources = true
			m := s.find(b.proxy)
			target := desiredComponent(rc, m)
			target.Resources.MinCPU += 2
			target.Resources.MaxCPU += 2
			rc.Cluster.Spec.Components.Extra["proxy"] = target
			s.Rolling = &rollingState{From: withdrawnPXCVersion, To: withdrawnPXCVersion, Stage: stage, Order: []string{m.Name}, Targets: map[string]domain.ComponentSpec{m.Name: target}}
			if stage != "resource-scale" {
				failed := desiredComponent(rc, m)
				failed.Resources.MinCPU--
				failed.Resources.MaxCPU--
				s.Rolling.ProxyRecovery = &failed
			}
			s = replayAdmissionState(t, rc, s)
			rc.Cluster.Spec.Version = replacementPXCVersion
			if _, err := p.reconcileRolling(t.Context(), rc, s); err == nil || !strings.Contains(err.Error(), "cannot change version during MySQL maintenance") {
				t.Fatal("mid-checkpoint version change escaped the raw maintenance guard", err)
			}
			if s.Rolling.To != withdrawnPXCVersion || b.resources != 0 || b.deploys != 0 {
				t.Fatal("rejected version change mutated maintenance", s.Rolling)
			}
			rc.Cluster.Spec.Version = withdrawnPXCVersion
			for range 12 {
				p = New(Options{Provider: "pxc"})
				if _, err := p.reconcileRolling(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if s.Rolling != nil && s.Rolling.To != withdrawnPXCVersion {
					t.Fatal("legacy maintenance target was rewritten", s.Rolling)
				}
				s = replayAdmissionState(t, rc, s)
				base.apply(t, p, rc)
				if s.Rolling == nil {
					break
				}
			}
			if s.Rolling != nil || b.resources != 1 || b.deploys != 0 || b.stops[b.proxy] != 0 || s.find(b.proxy).Version != withdrawnPXCVersion {
				t.Fatal("legacy resource checkpoint failed to preserve serving identity", s.Rolling, b.resources, b.deploys, b.stops)
			}
		})
	}
}

func TestPXCWithdrawnNewProxyResourceRollPreservesServing(t *testing.T) {
	p, rc, base := setupPXC(t)
	converge(t, p, rc, base)
	retainWithdrawnMembers(t, rc, base)
	s, _ := load(rc.Cluster)
	// Already-replaced data members under the retained specification must not
	// turn this resource change into a rejected 12.1-to-11.1 downgrade.
	for _, m := range s.data() {
		m.Version = replacementPXCVersion
	}
	s = replayAdmissionState(t, rc, s)
	b, _ := proxyScaleFixture(t, rc, s)
	b.servingResources = true
	setProxyScaleTarget(rc, 1, true)
	planned := false
	proxyFinished := false
	for range 15 {
		p = New(Options{Provider: "pxc"})
		if _, err := p.reconcileRolling(t.Context(), rc, s); err != nil {
			t.Fatal(err)
		}
		if s.Rolling != nil {
			planned = true
			if s.Rolling.To != withdrawnPXCVersion {
				t.Fatal("resource roll changed the raw maintenance target", s.Rolling)
			}
			proxyFinished = s.Rolling.Index > 0
		}
		s = replayAdmissionState(t, rc, s)
		base.apply(t, p, rc)
		if proxyFinished {
			break
		}
	}
	if !planned || !proxyFinished || b.resources != 1 || b.deploys != 0 || b.stops[b.proxy] != 0 || s.find(b.proxy).Version != withdrawnPXCVersion {
		t.Fatal("withdrawal mapping disrupted a serving proxy resource roll", s.Rolling, b.resources, b.deploys, b.stops)
	}
}

func TestPXCWithdrawnBackupRestoresIntoNewReplacementCluster(t *testing.T) {
	_, rc, _ := setupPXC(t)
	p := New(Options{Provider: "pxc", Store: rc.Store})
	c := &domain.Cluster{Namespace: "default", Name: "recover"}
	p.ApplyDefaults(&c.Spec)
	storage := &domain.BackupStorage{Namespace: "default", Name: "recovery", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups", Region: "region", VerifyTLS: true}
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	c.Spec.Backup.Storage = storage.Name
	c.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "saved"}
	backup := &domain.Backup{Namespace: "default", Cluster: "source", Name: "saved", Provider: "pxc", SourceVersion: withdrawnPXCVersion, Phase: domain.PhaseSucceeded, Type: "full", Storage: storage.Name, MySQL: &domain.MySQLBackup{ServerVersion: withdrawnPXCVersion, ToolVersion: "8.4.0-7"}}
	if err := rc.Store.CreateBackup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if c.Spec.Version != replacementPXCVersion {
		t.Fatal("restore target did not default to the replacement", c.Spec.Version)
	}
	if err := p.Validate(t.Context(), nil, c); err != nil {
		t.Fatal("same-major withdrawn-release backup rejected for new replacement cluster", err)
	}
	backup.Provider = "mysql"
	if err := rc.Store.UpdateBackup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(t.Context(), nil, c); err == nil {
		t.Fatal("cross-provider backup provenance bypassed restore admission")
	}
}

func TestPXCWithdrawnMajorUpgradeBackupAdmission(t *testing.T) {
	for _, target := range []string{withdrawnPXCVersion, replacementPXCVersion} {
		t.Run(target, func(t *testing.T) {
			p, rc, base := setupPXC(t)
			rc.Cluster.Spec.Version = "8.0.46-38.1"
			converge(t, p, rc, base)
			rc.Cluster.Spec.Version = target
			rc.Cluster.Generation++
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ := load(rc.Cluster)
			u := s.Rolling
			if u == nil || !u.Major || u.To != target || u.From != "8.0.46-38.1" {
				t.Fatal("major upgrade changed its raw version identities", u)
			}
			if !p.UpgradeBackupReady(t.Context(), rc.Cluster, rc.Secrets) {
				t.Fatal("8.0 to 8.4 target blocked prerequisite backup admission")
			}
			rc.Cluster.Spec.Components.Engine.Resources.MaxCPU++
			if p.UpgradeBackupReady(t.Context(), rc.Cluster, rc.Secrets) {
				t.Fatal("unrelated pending resource change admitted")
			}
			rc.Cluster.Spec.Components.Engine.Resources.MaxCPU--
			if err := requireUpgradeBackup(t.Context(), rc, u); err == nil {
				t.Fatal("major upgrade bypassed its required backup")
			}
			rc.Cluster.Spec.Backup.Storage = "fixture"
			started := u.RequestedAt.Add(time.Second)
			completed := started.Add(time.Second)
			backup := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "upgrade", Provider: "pxc", SourceVersion: u.From, Phase: domain.PhaseSucceeded, Type: "full", Storage: "fixture", StartedAt: &started, CompletedAt: &completed, MySQL: &domain.MySQLBackup{Label: "full-upgrade"}}
			if err := rc.Store.CreateBackup(t.Context(), backup); err != nil {
				t.Fatal(err)
			}
			if err := requireUpgradeBackup(t.Context(), rc, u); err != nil {
				t.Fatal("fresh source-version backup did not satisfy major upgrade", err)
			}
		})
	}
}

func TestPXCWithdrawnImportKeepsProxyVersionRaw(t *testing.T) {
	p, rc, base := setupPXC(t)
	// A pre-existing 11.1 create may first reconcile after the CP is upgraded.
	rc.Cluster.Spec.Version = withdrawnPXCVersion
	events := &withdrawnEvents{}
	rc.Events = events
	rc.Backend = &withdrawnDeploymentBackend{fakeBackend: base, t: t, rc: rc}
	converge(t, p, rc, base)
	s, _ := load(rc.Cluster)
	for _, m := range s.Members {
		want := withdrawnPXCVersion
		if m.Role == "data" {
			want = replacementPXCVersion
		}
		if m.Version != want {
			t.Fatal("import version mapping escaped data-member installation", m)
		}
	}
	if len(events.events) != 3 {
		t.Fatal("only the three data imports should record replacements", events.events)
	}
}

func TestPXCWithdrawnLegacyRollingDeployReplacesDataMember(t *testing.T) {
	p, rc, base := setupPXC(t)
	converge(t, p, rc, base)
	retainWithdrawnMembers(t, rc, base)
	s, _ := load(rc.Cluster)
	m := s.find("demom1")
	m.Offline = true
	s.Rolling = &rollingState{From: withdrawnPXCVersion, To: withdrawnPXCVersion, Stage: "deploy", Order: []string{m.Name}, Targets: map[string]domain.ComponentSpec{m.Name: desiredComponent(rc, m)}}
	s = replayAdmissionState(t, rc, s)
	events := &withdrawnEvents{err: errors.New("event unavailable")}
	rc.Events = events
	backend := &withdrawnDeploymentBackend{fakeBackend: base, t: t, rc: rc}
	rc.Backend = backend
	if _, err := p.reconcileRolling(t.Context(), rc, s); err == nil || s.find(m.Name).Version != withdrawnPXCVersion || backend.data != 0 {
		t.Fatal("failed event publication advanced legacy deployment", err)
	}
	events.err = nil
	if _, err := p.reconcileRolling(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	s = replayAdmissionState(t, rc, s)
	if s.find(m.Name).Version != replacementPXCVersion || s.Rolling.To != withdrawnPXCVersion || len(events.events) != 1 {
		t.Fatal("legacy checkpoint did not retain raw target and replacement event", s.Rolling, events.events)
	}
	if _, err := p.reconcileRolling(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if backend.data != 1 || s.Rolling.Stage != "deployed" || s.Rolling.To != withdrawnPXCVersion {
		t.Fatal("legacy data deployment did not submit replacement preparation", s.Rolling)
	}
}

func TestPXCWithdrawnAgentUpgradeRefusedBeforeTouchingMembers(t *testing.T) {
	for _, checkpoint := range []string{"new", "legacy-deploy"} {
		t.Run(checkpoint, func(t *testing.T) {
			p, rc, b := setupAgentRollout(t, "cluster")
			retainWithdrawnMembers(t, rc, b.fakeBackend)
			s, _ := load(rc.Cluster)
			before, _ := json.Marshal(s.Members)
			op := mysqlAgentRolloutOperation(false)
			if checkpoint == "legacy-deploy" {
				target, err := engine.AgentUpgradeTarget(op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint, _ = json.Marshal(agentUpgradeCheckpoint{Target: target, Order: []string{"demom1"}, Member: "demom1", Stage: "deploy", BeforeBuilds: map[string]string{"demom1": "old"}})
			}
			_, err := p.RunOperation(t.Context(), rc, op)
			if !errors.Is(err, engine.ErrInvalid) {
				t.Fatal("withdrawn agent upgrade must fail terminally rather than retry", err)
			}
			for _, text := range []string{"PXC member", withdrawnPXCVersion, "Percona withdrew", "set spec.version: " + replacementPXCVersion, "rolling upgrade also installs the current agent", "retry upgrade-agents"} {
				if !strings.Contains(err.Error(), text) {
					t.Fatal("refusal omitted recovery guidance", err)
				}
			}
			s, _ = load(rc.Cluster)
			after, _ := json.Marshal(s.Members)
			if string(before) != string(after) || len(b.names) != 0 {
				t.Fatal("refused upgrade touched member identity or submitted a deployment", b.names)
			}
			config, err := p.MemberConfig(t.Context(), rc, "demom1")
			if err != nil {
				t.Fatal(err)
			}
			var native myspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &native); err != nil {
				t.Fatal(err)
			}
			if native.Version != withdrawnPXCVersion || !strings.Contains(native.ServerDir, withdrawnPXCVersion) {
				t.Fatal("refused upgrade changed the installed launcher", native.ServerDir)
			}
			// Terminal failure cleanup must remain able to settle an old owner.
			finishAgentRollout(t, rc, b, op, true)
			if len(b.names) != 0 {
				t.Fatal("failure cleanup submitted the withdrawn deployment", b.names)
			}
		})
	}
}

func TestPXCWithdrawnLegacyImportedMemberUsesReplacement(t *testing.T) {
	p, rc, base := setupPXC(t)
	converge(t, p, rc, base)
	retainWithdrawnMembers(t, rc, base)
	s, _ := load(rc.Cluster)
	m := s.find("demom1")
	m.Stage, m.Initialized, m.DeployOp = "imported", false, nil
	base.deployed[m.Name] = false
	replayAdmissionState(t, rc, s)
	events := &withdrawnEvents{}
	rc.Events = events
	backend := &withdrawnDeploymentBackend{fakeBackend: base, t: t, rc: rc}
	rc.Backend = backend
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.find(m.Name).Version != replacementPXCVersion || backend.data != 0 || len(events.events) != 1 {
		t.Fatal("legacy import did not checkpoint the replacement before deployment", s.find(m.Name), events.events)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if backend.data != 1 {
		t.Fatal("legacy imported member did not deploy available replacement")
	}
}
