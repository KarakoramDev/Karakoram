// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestSuspendResumePreservesDataIdentity(t *testing.T) {
	p, rc, b := setup(t, "single")
	s := converge(t, p, rc)
	uid := s.UID
	volumeID := s.data()[0].VolumeID
	done := false
	for range 30 {
		var err error
		done, err = p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("suspension did not finish")
	}
	if len(b.services) != 1 || b.services["demop0v"].ID != volumeID {
		t.Fatal("suspension changed the retained volume")
	}
	s = converge(t, p, rc)
	if s.UID != uid || s.data()[0].VolumeID != volumeID {
		t.Fatal("resume changed data identity")
	}
}
func TestInternalPreservesOtherProvidersState(t *testing.T) {
	p, rc, _ := setup(t, "single")
	rc.Cluster.Internal = json.RawMessage(`{"wrapper":{"generation":7}}`)
	converge(t, p, rc)
	if !strings.Contains(string(rc.Cluster.Internal), `"generation":7`) {
		t.Fatal("wrapper state overwritten")
	}
}
func TestPreparationSuppressesDistroCluster(t *testing.T) {
	commands := strings.Join(PrepareCommands("17.11.1"), "\n")
	policy := strings.Index(commands, "policy-rc.d")
	cluster := strings.Index(commands, "create_main_cluster = false")
	install := strings.Index(commands, "percona-postgresql-17=")
	if policy < 0 || cluster < 0 || install < policy || install < cluster {
		t.Fatal("PostgreSQL installation can start an unmanaged cluster")
	}
	if !strings.Contains(commands, "2:17.11-1.noble") || !strings.Contains(commands, "sha256sum -c") {
		t.Fatal("package preparation is not pinned")
	}
}

func TestPreparationRetainsExactServerCandidate(t *testing.T) {
	for version, pin := range packagePins {
		commands := strings.Join(PrepareCommands(version), "\n")
		m := strconv.Itoa(major(version))
		preference := "Package: percona-postgresql-" + m + "\\nPin: version " + pin + "\\nPin-Priority: 1001\\n"
		pinned := strings.Index(commands, preference)
		install := strings.Index(commands, "apt-get install -y percona-postgresql-"+m+"=")
		if pinned < 0 || install < pinned || !strings.Contains(commands, "/etc/apt/preferences.d/karakoram-postgresql-"+m) {
			t.Fatalf("%s must persist its server candidate before apt resolves dependencies", version)
		}
	}
}

func TestPerconaBootstrapUsesRootPrivateVerifiedStaging(t *testing.T) {
	command := PrepareCommands("17.11.1")[1]
	const prefix = "sudo sh -c '"
	if !strings.HasPrefix(command, prefix) || !strings.HasSuffix(command, "'") {
		t.Fatal("bootstrap download, verification and install must share one privileged shell")
	}
	script := strings.ReplaceAll(command[len(prefix):len(command)-1], "'\"'\"'", "'")
	for _, want := range []string{
		"set -eu\numask 077\nstage=$(mktemp -d /tmp/karakoram-artifact.XXXXXXXXXX)",
		"trap 'rm -rf -- \"$stage\"' EXIT HUP INT TERM",
		"cd \"$stage\"\nset -C",
		"test ! -e \"$file\" && test ! -L \"$file\"",
		"curl -fsSL 'https://repo.percona.com/apt/percona-release_latest.noble_all.deb' > \"$file\"",
		"10a419065480e4cd4ad6d1036e742832203e79ff537461115361ec2f7ff62e36  percona-release.deb",
		"sha256sum -c -",
		"apt-get install -y \"$stage/$file\"",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("bootstrap lacks protected staging step %q", want)
		}
	}
	if strings.Contains(script, "/tmp/percona-release.deb") || strings.Index(script, "sha256sum -c -") > strings.Index(script, "apt-get install -y") {
		t.Fatal("privileged installer can consume unverified or runtime-writable bytes")
	}
}
func TestProxyPreparationSuppressesPackagedDaemon(t *testing.T) {
	p, rc, _ := setup(t, "single")
	s := converge(t, p, rc)
	runtime, err := p.runtimeSpec(rc, s.find("demopx"))
	if err != nil {
		t.Fatal(err)
	}
	commands := strings.Join(runtime.Prepare, "\n")
	install := strings.Index(commands, `apt-get install -y "$stage/$file"`)
	suppress := strings.Index(commands, "if [ -d /run/systemd/system ]; then sudo systemctl disable --now proxysql.service && sudo systemctl mask proxysql.service; fi")
	if install < 0 || suppress < install {
		t.Fatal("proxy preparation must stop and mask the packaged daemon after installation, only on systemd runtimes")
	}
	if policy := strings.Index(commands, "policy-rc.d"); policy < 0 || policy > install {
		t.Fatal("proxy installation must suppress init-script startup before installation")
	}
}

func TestProxyPreparationIndependentOfPostgresVersion(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	var prepared string
	for _, version := range []string{"16.14.1", "16.15.1", "17.11.1", "18.6.1"} {
		rc.Cluster.Spec.Version = version
		runtime, err := p.runtimeSpec(rc, s.find("demopx"))
		if err != nil {
			t.Fatal(err)
		}
		commands := strings.Join(runtime.Prepare, "\n")
		if strings.Contains(commands, "percona-postgresql") || strings.Contains(commands, "ppg-") {
			t.Fatal("proxy installs PostgreSQL-version-specific packages", version)
		}
		if prepared != "" && commands != prepared {
			t.Fatal("PostgreSQL release changed proxy preparation", version)
		}
		prepared = commands
	}
}

func TestMinorUpgradeReloadsProxyWithoutRedeployment(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		for _, replicas := range []int{1, 2} {
			t.Run(topology+"/"+strconv.Itoa(replicas), func(t *testing.T) {
				p, rc, b := setup(t, topology)
				rc.Cluster.Spec.Version = "16.14.1"
				proxyComponent := rc.Cluster.Spec.Components.Extra["proxy"]
				proxyComponent.Replicas = replicas
				rc.Cluster.Spec.Components.Extra["proxy"] = proxyComponent
				s := converge(t, p, rc)
				proxyID, proxyVersion := s.find("demopx").ServiceID, s.find("demopx").ConfigVersion
				b.actions, b.reloads = nil, nil
				rc.Cluster.Spec.Version = "16.15.1"
				rc.Cluster.Generation++
				for range 100 {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					s, err := load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					proxy := s.find("demopx")
					if proxy.Stage == "imported" || proxy.Stage == "deploying" || proxy.DeployOp != nil || proxy.ServiceID != proxyID {
						t.Fatal("version-only change redeployed proxy", proxy)
					}
					if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
						break
					}
				}
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				proxy := s.find("demopx")
				if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation || proxy.Stage != "joined" || proxy.Version != rc.Cluster.Spec.Version || proxy.ConfigVersion <= proxyVersion || proxy.Replicas != replicas {
					t.Fatal("minor upgrade did not converge proxy metadata", proxy)
				}
				if !slices.Contains(b.reloads, proxy.Name) || slices.Contains(b.actions, "deploy:"+proxy.Name) {
					t.Fatal("proxy metadata did not use hot reload", b.actions, b.reloads)
				}
				var deployed []string
				for _, action := range b.actions {
					if strings.HasPrefix(action, "deploy:") {
						deployed = append(deployed, strings.TrimPrefix(action, "deploy:"))
					}
				}
				want := []string{"demop0"}
				if topology == "ha" {
					want = []string{"demop1", "demop2", "demop0"}
					if b.leader != "demop1" {
						t.Fatal("minor upgrade did not switch away from the old primary", b.leader)
					}
				}
				if !slices.Equal(deployed, want) {
					t.Fatal("unexpected data-member package rollout", deployed, want)
				}
				config, err := p.MemberConfig(t.Context(), rc, proxy.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec pgspec.Spec
				if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if config.EngineVersion != rc.Cluster.Spec.Version || spec.Version != rc.Cluster.Spec.Version || spec.Major != 16 || spec.Proxy == nil || len(spec.Proxy.Servers) != len(s.data()) || len(spec.Proxy.Users) == 0 || spec.Proxy.ServerCert == nil || !spec.Proxy.BackendTLS {
					t.Fatal("version-only reload lost proxy configuration")
				}
			})
		}
	}
}
func TestProxyMemberConfig(t *testing.T) {
	p, rc, _ := setup(t, "single")
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"proxy":{"readWriteSplit":true,"readerPort":true}}`)
	converge(t, p, rc)
	config, err := p.MemberConfig(context.Background(), rc, "demopx")
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Role != "proxy" || spec.Proxy == nil || !spec.Proxy.ReadWriteSplit || !spec.Proxy.ReaderPort || spec.Proxy.Monitor.Username != "kk_monitor" || len(spec.Proxy.Users) != 1 || spec.Proxy.Users[0].Password == "" || spec.Proxy.ServerCert == nil {
		t.Fatal("incomplete proxy payload")
	}
}
func TestProxyPublishesOnlyJoinedRunningBackends(t *testing.T) {
	for _, scenario := range []string{"planned", "imported", "deploying", "deployed", "stopped", "failed", "missing-runtime", "missing-identity", "removing", "reload", "active", "joined"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			m := s.find("demop1")
			switch scenario {
			case "active":
				b.services[m.Name].Status = infra.StatusActive
			case "stopped":
				b.services[m.Name].Status = infra.StatusStopped
			case "failed":
				b.services[m.Name].Status = infra.StatusFailed
			case "missing-runtime":
				delete(b.services, m.Name)
			case "missing-identity":
				m.ServiceID = ""
			case "removing":
				m.Removing = true
			case "reload":
				m.Stage = "deployed"
			default:
				m.Stage = scenario
				if scenario != "joined" {
					m.ProxyReady = false
				}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			config, err := p.MemberConfig(t.Context(), rc, "demopx")
			if err != nil {
				t.Fatal(err)
			}
			var proxy pgspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &proxy); err != nil {
				t.Fatal(err)
			}
			if slices.Contains(proxy.Proxy.Servers, m.Name) != (scenario == "joined" || scenario == "reload" || scenario == "active") {
				t.Fatalf("unsafe backend publication for %s: %v", scenario, proxy.Proxy.Servers)
			}
			if !slices.Contains(proxy.Proxy.Servers, "demop0") || !slices.Contains(proxy.Proxy.Servers, "demop2") {
				t.Fatal("unready member removed unrelated healthy backends")
			}
			config, err = p.MemberConfig(t.Context(), rc, "demop0")
			if err != nil {
				t.Fatal(err)
			}
			var data pgspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &data); err != nil {
				t.Fatal(err)
			}
			if len(data.Peers) != 3 {
				t.Fatal("proxy publication filter changed data-member bootstrap/DCS peers")
			}
		})
	}
}

// activeServiceBackend models the other neutral operational service status:
// adapters can report ACTIVE rather than RUNNING for a deployed runtime.
type activeServiceBackend struct{ *fakeBackend }

func (b activeServiceBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	service, err := b.fakeBackend.GetService(ctx, name)
	if err != nil || service == nil || service.Kind != infra.KindRuntime || service.Status != infra.StatusRunning {
		return service, err
	}
	observed := *service
	observed.Status = infra.StatusActive
	return &observed, nil
}

func TestProxyJoinPublicationDuringBootstrapAndConversion(t *testing.T) {
	for _, runtimeStatus := range []string{infra.StatusRunning, infra.StatusActive} {
		for _, topology := range []string{"single", "ha", "single-to-ha"} {
			t.Run(runtimeStatus+"/"+topology, func(t *testing.T) {
				initial := topology
				if topology == "single-to-ha" {
					initial = "single"
				}
				p, rc, _ := setup(t, initial)
				if runtimeStatus == infra.StatusActive {
					rc.Backend = activeServiceBackend{rc.Backend.(*fakeBackend)}
				}
				if topology == "single-to-ha" {
					converge(t, p, rc)
					rc.Cluster.Spec.Topology.Type = "ha"
					rc.Cluster.Spec.Components.Engine.Replicas = 3
					rc.Cluster.Generation++
				}
				joins := 0
				sawEmpty := false
				completed := false
				for range 120 {
					before, err := load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					s, err := load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					config, err := p.MemberConfig(t.Context(), rc, "demopx")
					if err != nil {
						t.Fatal(err)
					}
					var proxy pgspec.Spec
					if err := json.Unmarshal(config.EngineSpec, &proxy); err != nil {
						t.Fatal(err)
					}
					sawEmpty = sawEmpty || len(proxy.Proxy.Servers) == 0
					for _, m := range s.data() {
						prior := before.find(m.Name)
						if m.Stage != "joined" || prior != nil && prior.Stage == "joined" {
							continue
						}
						if prior != nil && prior.ProxyReady {
							if s.ConfigVersion != before.ConfigVersion {
								t.Fatal("config-only reload caused another join publication")
							}
							continue
						}
						joins++
						if s.ConfigVersion <= before.ConfigVersion || config.ConfigVersion != s.ConfigVersion || !slices.Contains(proxy.Proxy.Servers, m.Name) {
							t.Fatalf("joined backend not published at a new proxy version: %s version=%d prior=%d proxy=%d servers=%v", m.Name, s.ConfigVersion, before.ConfigVersion, config.ConfigVersion, proxy.Proxy.Servers)
						}
					}
					status, err := p.Status(t.Context(), rc)
					if err != nil {
						t.Fatal(err)
					}
					if status.Phase == domain.PhaseReady {
						completed = true
						break
					}
				}
				if !completed || !sawEmpty || joins != rc.Cluster.Spec.Components.Engine.Replicas {
					t.Fatalf("bootstrap/conversion publication did not converge: completed=%v empty=%v joins=%d", completed, sawEmpty, joins)
				}
			})
		}
	}
}

func TestIPv4HBAFamily(t *testing.T) {
	p, rc, _ := setup(t, "single")
	converge(t, p, rc)
	config, err := p.MemberConfig(context.Background(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	json.Unmarshal(config.EngineSpec, &spec)
	if len(spec.Postgres.ClientCIDRs) != 1 || spec.Postgres.ClientCIDRs[0] != "0.0.0.0/0" {
		t.Fatal("backend private family ignored")
	}
}

type ipv6Backend struct{ *fakeBackend }

func (b ipv6Backend) Layout() infra.Layout {
	l := b.fakeBackend.Layout()
	l.PrivateAddressFamily = infra.AddressFamilyIPv6
	return l
}

func TestIPv6HBAFamily(t *testing.T) {
	p, rc, b := setup(t, "single")
	rc.Backend = ipv6Backend{b}
	converge(t, p, rc)
	config, err := p.MemberConfig(context.Background(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if len(spec.Postgres.ClientCIDRs) != 1 || spec.Postgres.ClientCIDRs[0] != "::/0" {
		t.Fatal("backend private family ignored")
	}
}

func TestRepositoryRendering(t *testing.T) {
	p, rc, _ := setup(t, "single")
	st := rc.Store.(*store.Store)
	storage := &domain.BackupStorage{Name: "archive", Namespace: "default", Type: "s3", EndpointURL: "https://objects.example.com:9444", Bucket: "backups", Prefix: "tenant/prefix", Region: "region-1", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "example-access-key", SecretAccessKey: "example-secret-key"}}
	if err := st.CreateBackupStorage(context.Background(), storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = storage.Name
	rc.Cluster.Spec.Backup.PITR.OplogSpanMinutes = 2
	rc.Cluster.Spec.Backup.PITR.Enabled = true
	s := converge(t, p, rc)
	config, err := p.MemberConfig(context.Background(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Backup == nil || spec.Backup.Repository.Path != "/tenant/prefix/postgres/default/demo-"+s.UID || spec.Backup.Repository.Stanza != "kk" || spec.Backup.ArchiveTimeoutSeconds != 120 || spec.Backup.Repository.S3.Port != 9444 || spec.Backup.Repository.S3.SecretKey != "example-secret-key" {
		t.Fatal("incomplete backup repository payload")
	}
	if strings.Contains(string(rc.Cluster.Internal), "example-secret-key") || strings.Contains(string(rc.Cluster.Internal), "example-access-key") {
		t.Fatal("storage credentials leaked into state")
	}
}

func TestHAIncreasingSharedMemoryRestartsReplicasBeforeWriter(t *testing.T) {
	p, rc, b := setup(t, "ha")
	converge(t, p, rc)
	b.actions = nil // Measure restarts, not phase configuration publication.
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"parameters":{"max_connections":"200"}}`)
	rc.Cluster.Generation++
	converge(t, p, rc)
	var restarts []string
	for _, action := range b.actions {
		if strings.HasPrefix(action, "/v1/postgres/restart:") {
			restarts = append(restarts, strings.TrimPrefix(action, "/v1/postgres/restart:"))
		}
	}
	if len(restarts) != 3 || restarts[0] == "demop0" || restarts[len(restarts)-1] != "demop0" || b.leader == "" {
		t.Fatalf("unsafe restart sequence: %v leader %s", restarts, b.leader)
	}
}

func TestSingleToHAAdoptsExistingVolume(t *testing.T) {
	p, rc, _ := setup(t, "single")
	s := converge(t, p, rc)
	volume := s.data()[0].VolumeID
	rc.Cluster.Spec.Topology.Type = "ha"
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	rc.Cluster.Generation++
	for range 2 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"demop0", "demop1", "demop2"} {
		config, err := p.MemberConfig(t.Context(), rc, name)
		if err != nil {
			t.Fatal(err)
		}
		var spec pgspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if spec.Bootstrap == nil || spec.Bootstrap.SeedMember != "demop0" {
			t.Fatal("conversion allowed an empty replica to bootstrap before the original member")
		}
	}
	s = converge(t, p, rc)
	if len(s.data()) != 3 || s.data()[0].VolumeID != volume || s.Topology != "ha" {
		t.Fatal("conversion lost existing data member identity")
	}
}

type blockedDeploymentBackend struct{ *fakeBackend }

func (b blockedDeploymentBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "waiting-deployment" {
		return infra.OpState{Done: false}, nil
	}
	return b.fakeBackend.OpStatus(ctx, op)
}
func TestCleanupSupersedesUnreadyDeployment(t *testing.T) {
	p, rc, b := setup(t, "single")
	s := converge(t, p, rc)
	s.data()[0].DeployOp = &infra.Op{ID: "waiting-deployment"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Backend = blockedDeploymentBackend{b}
	if _, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if b.services["demop0"] != nil {
		t.Fatal("cleanup waited for deployment readiness")
	}
}

func TestStatusDoesNotDialUndeployedMembers(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := &state{ConfigVersion: 1}
	s.allocate(rc.Cluster)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Agents = struct{ engine.AgentDialer }{}
	status, err := p.Status(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Members) != 4 || status.Phase == domain.PhaseReady {
		t.Fatal("planned members were observed as ready")
	}
}

type failedDeploymentBackend struct{ *fakeBackend }

func (b failedDeploymentBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "failed-deployment" {
		return infra.OpState{Failed: true}, nil
	}
	return b.fakeBackend.OpStatus(ctx, op)
}

func TestFailedDeploymentClearsReceiptForRetry(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.data()[1]
	m.Stage = "deploying"
	m.DeployOp = &infra.Op{ID: "failed-deployment"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Backend = failedDeploymentBackend{b}
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("failed deployment was treated as pending")
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if m = s.find(m.Name); m.Stage != "imported" || m.DeployOp != nil {
		t.Fatalf("failed deployment was not reset for retry: %+v", m)
	}
	rc.Backend = b
	converge(t, p, rc)
}

func TestFailedUpgradeDeploymentClearsReceiptForRetry(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	m := s.data()[1]
	m.DeployOp = &infra.Op{ID: "failed-deployment"}
	s.Upgrade = &upgradeState{Stage: "prepare", Order: []string{m.Name}, Waiting: true}
	rc.Backend = failedDeploymentBackend{b}
	if _, err := p.upgradeDeploy(t.Context(), rc, s); err == nil {
		t.Fatal("failed upgrade deployment was treated as pending")
	}
	if m.DeployOp != nil || s.Upgrade.Waiting {
		t.Fatalf("failed upgrade deployment was not reset for retry: %+v", s.Upgrade)
	}
}

type progressingDeploymentBackend struct {
	*fakeBackend
	observed map[string]bool
}

func (b progressingDeploymentBackend) OpStatus(_ context.Context, op infra.Op) (infra.OpState, error) {
	b.observed[op.ID] = true
	return infra.OpState{Done: false}, nil
}
func TestAllDeploymentsProgressBeforeQuorumReadiness(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	s.Members[0].Stage = "deployed"
	for _, m := range s.Members[1:] {
		m.Stage = "deploying"
		m.DeployOp = &infra.Op{ID: m.Name}
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	progress := progressingDeploymentBackend{fakeBackend: b, observed: map[string]bool{}}
	rc.Backend = progress
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	if len(progress.observed) != 3 {
		t.Fatalf("only %d pending deployments advanced before quorum readiness", len(progress.observed))
	}
}
