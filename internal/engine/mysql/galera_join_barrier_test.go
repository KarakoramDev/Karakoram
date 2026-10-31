// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func joinBarrierState() *state {
	return &state{Topology: "cluster", Formation: formation{State: "formed", Seed: "m0"}, Members: []*member{
		{Name: "m2", Role: "data", Index: 2, Stage: "deployed"},
		{Name: "router", Role: "proxy", Stage: "joined"},
		{Name: "m0", Role: "data", Index: 0, Stage: "joined", Initialized: true},
		{Name: "m1", Role: "data", Index: 1, Stage: "deployed"},
	}}
}

func TestPXCJoinBarrierUsesDeterministicSyncedReceipts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		change  func(*state)
		allowed []string
	}{
		{"first follower only", func(*state) {}, []string{"m0", "m1"}},
		{"unformed holds data", func(s *state) { s.Formation.State = "unformed" }, nil},
		{"bootstrap seed only", func(s *state) { s.Formation.State = "bootstrapping" }, []string{"m0"}},
		{"arbitrary bootstrap seed", func(s *state) { s.Formation.State, s.Formation.Seed = "bootstrapping", "m2" }, []string{"m2"}},
		{"historical certified bootstrap needs no new client fence", func(s *state) {
			s.Formation.State, s.Formation.FormationEpoch = "bootstrapping", 2
			s.Formation.GTIDCertification = &pxcFormationCertification{Owner: "already-admitted", Epoch: 1, Checkpoint: backupCheckpoint{GTIDCertification: &pxcRestoreCertification{Certified: true}}}
		}, []string{"m0"}},
		{"arbitrary seed follower order", func(s *state) {
			s.Formation.Seed = "m2"
			s.find("m0").Stage = "deployed"
			s.find("m2").Stage = "joined"
		}, []string{"m0", "m2"}},
		{"next after Synced", func(s *state) { s.find("m1").Stage = "joined" }, []string{"m0", "m1", "m2"}},
		{"already joined later member stays running", func(s *state) { s.find("m2").Stage = "joined" }, []string{"m0", "m1", "m2"}},
		{"ordinary restart all", func(s *state) { s.find("m1").Stage, s.find("m2").Stage = "joined", "joined" }, []string{"m0", "m1", "m2"}},
		{"offline member stays held", func(s *state) { s.find("m1").Offline = true }, []string{"m0", "m2"}},
		{"offline seed stays held", func(s *state) { s.find("m0").Offline = true }, []string{"m1"}},
		{"removing new identity stays held", func(s *state) { s.find("m1").Removing = true }, []string{"m0", "m2"}},
		{"graceful retirement retains joined member", func(s *state) { s.find("m0").Removing = true }, []string{"m0", "m1"}},
		{"earlier planned reserves position", func(s *state) { s.find("m1").Stage = "planned" }, []string{"m0"}},
		{"earlier import reserves position", func(s *state) { s.find("m1").Stage = "imported" }, []string{"m0"}},
		{"earlier deployment reserves position", func(s *state) { s.find("m1").Stage = "deploying" }, []string{"m0"}},
		{"index tie uses name", func(s *state) { s.find("m2").Index = 1 }, []string{"m0", "m1"}},
		{"non-PXC topology unaffected", func(s *state) { s.Topology = "group" }, []string{"m0", "m1", "m2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := joinBarrierState()
			tc.change(s)
			for _, m := range s.Members {
				if m.Role != "data" {
					continue
				}
				if got, want := pxcJoinAllowed(s, m), slices.Contains(tc.allowed, m.Name); got != want {
					t.Fatalf("member %s allowed=%t want=%t", m.Name, got, want)
				}
			}
		})
	}
}

func joinBarrierConfigFixture(t *testing.T) (*Core, *engine.Context, *state) {
	t.Helper()
	p, rc, _ := setupPXC(t)
	// One production Sync allocates identities and sealed PKI without needing
	// to finish the formation whose process-release policy is under test.
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.Pending = nil
	s.Formation.State = "formed"
	s.Formation.GTIDCertification = nil
	s.RestoreGTIDFence = ""
	for _, m := range s.Members {
		m.Stage, m.ServiceID = "deployed", m.Name+"-id"
		m.ServerUUID = fmt.Sprintf("11111111-2222-3333-4444-%012d", m.Index+1)
	}
	seed := s.find(s.Formation.Seed)
	seed.Stage, seed.Initialized = "joined", true
	// Configuration rendering consumes durable Synced launch receipts. It must
	// not recursively query an agent that is waiting for this same config.
	rc.Agents = nil
	return p, rc, s
}

func assertPXCJoinConfig(t *testing.T, p *Core, rc *engine.Context, s *state, allowed ...string) {
	t.Helper()
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		if m.Role != "data" {
			continue
		}
		config, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec myspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if err := spec.Validate(); err != nil {
			t.Fatalf("real member validator rejected %s: %v", m.Name, err)
		}
		if running, want := slices.Contains(config.DesiredProcesses, "mysqld"), slices.Contains(allowed, m.Name); running != want {
			t.Fatalf("member %s desired=%v want-running=%t", m.Name, config.DesiredProcesses, want)
		}
	}
}

func TestPXCJoinBarrierMemberConfigSequencesInitialAndRestoreCohorts(t *testing.T) {
	p, rc, s := joinBarrierConfigFixture(t)
	data := s.data()
	seed, first, second := data[0], data[1], data[2]
	s.Formation.State = "unformed"
	seed.Initialized = false
	assertPXCJoinConfig(t, p, rc, s)
	s.Formation.State = "bootstrapping"
	assertPXCJoinConfig(t, p, rc, s, seed.Name)
	s.Formation.State = "formed"
	seed.Initialized = true
	assertPXCJoinConfig(t, p, rc, s, seed.Name, first.Name)
	first.Stage, first.Initialized = "joined", true
	assertPXCJoinConfig(t, p, rc, s, seed.Name, first.Name, second.Name)
	second.Stage, second.Initialized = "joined", true
	assertPXCJoinConfig(t, p, rc, s, seed.Name, first.Name, second.Name)
	// A restored formation invalidates stopped followers' old launch receipts.
	s.Formation.FormationEpoch++
	s.RestoreGTIDFence = "restore-client-fence"
	first.Stage, second.Stage = "deployed", "deployed"
	assertPXCJoinConfig(t, p, rc, s, seed.Name, first.Name)
	first.Stage = "joined"
	assertPXCJoinConfig(t, p, rc, s, seed.Name, first.Name, second.Name)
	second.Offline = true
	assertPXCJoinConfig(t, p, rc, s, seed.Name, first.Name)
	second.Offline = false
	second.GaleraSSTDonor = seed.Name
	assertPXCJoinConfig(t, p, rc, s, seed.Name, first.Name, second.Name)
}

func TestPXCJoinBarrierSequencesScaleUpWithoutRestartingMembers(t *testing.T) {
	_, rc, s := joinBarrierConfigFixture(t)
	for _, m := range s.Members {
		m.Stage, m.Initialized = "joined", true
	}
	existing := s.data()
	first := &member{Name: rc.Cluster.Name + "m3", Role: "data", Index: 3, Stage: "planned"}
	second := &member{Name: rc.Cluster.Name + "m4", Role: "data", Index: 4, Stage: "deployed"}
	// Deliberately insert the later deployment first: list order is not a lock.
	s.Members = append(s.Members, second, first)
	if pxcJoinAllowed(s, second) || pxcJoinAllowed(s, first) {
		t.Fatal("out-of-order platform deployment released a later joiner")
	}
	first.Stage = "deployed"
	if !pxcJoinAllowed(s, first) || pxcJoinAllowed(s, second) {
		t.Fatal("scale-up did not admit exactly its first new identity")
	}
	for _, m := range existing {
		if !pxcJoinAllowed(s, m) {
			t.Fatal("new join barrier stopped an existing Synced/restarting member")
		}
	}
	first.Stage = "joined"
	if !pxcJoinAllowed(s, second) {
		t.Fatal("second scale-up member remained blocked after first Synced receipt")
	}
}

func TestPXCFormationFenceProxySeedFallbackRequiresOwnedLiveSeed(t *testing.T) {
	for _, scenario := range []string{"owned", "no-owner", "owner-mismatch", "no-fence", "uninitialized", "offline", "removing", "unadmitted"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, s := joinBarrierConfigFixture(t)
			seed := s.find(s.Formation.Seed)
			seed.Stage = "deployed"
			s.RestoreGTIDFence = "formation-owner"
			s.Formation.GTIDCertification = &pxcFormationCertification{Owner: "formation-owner"}
			switch scenario {
			case "no-owner":
				s.Formation.GTIDCertification = nil
			case "owner-mismatch":
				s.Formation.GTIDCertification.Owner = "other-owner"
			case "no-fence":
				s.RestoreGTIDFence = ""
			case "uninitialized":
				seed.Initialized = false
			case "offline":
				seed.Offline = true
			case "removing":
				seed.Removing = true
			case "unadmitted":
				seed.ServiceID = ""
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			config, err := p.MemberConfig(t.Context(), rc, rc.Cluster.Name+"mx")
			if err != nil {
				t.Fatal(err)
			}
			var spec myspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if scenario == "owned" {
				if len(config.DesiredProcesses) != 0 || len(spec.Proxy.Backends) != 1 || spec.Proxy.Backends[0].Name != seed.Name {
					t.Fatal("owned formation fence did not retain a valid stopped proxy image")
				}
				if err := spec.Validate(); err != nil {
					t.Fatalf("real validator rejected owned formation config: %v", err)
				}
			} else if len(spec.Proxy.Backends) != 0 || spec.Validate() == nil {
				t.Fatal("unowned/unsafe seed bypassed the real proxy validator")
			}
		})
	}
}

func TestPXCJoinBarrierRequiresPublishedFenceAndDonorPin(t *testing.T) {
	for _, tc := range []struct {
		name    string
		change  func(*state)
		allowed []string
	}{
		{"owned fence and pins", func(*state) {}, []string{"m0", "m1"}},
		{"no negotiated proof", func(s *state) { s.Formation.GTIDCertification.Checkpoint.GTIDCertification = nil }, []string{"m0"}},
		{"fence intent only", func(s *state) { s.Formation.GTIDCertification.Checkpoint.GTIDCertification.FencesOwned = false }, []string{"m0"}},
		{"no published fence", func(s *state) { s.RestoreGTIDFence = "" }, []string{"m0"}},
		{"foreign fence owner", func(s *state) { s.RestoreGTIDFence = "other-owner" }, []string{"m0"}},
		{"no owner identity", func(s *state) { s.Formation.GTIDCertification.Owner = "" }, []string{"m0"}},
		{"missing donor pin", func(s *state) { s.find("m1").GaleraSSTDonor = "" }, []string{"m0"}},
		{"wrong donor pin", func(s *state) { s.find("m1").GaleraSSTDonor = "m2" }, []string{"m0"}},
		{"legacy does not invent controls", func(s *state) {
			s.Formation.GTIDCertification.Legacy = true
			s.RestoreGTIDFence = ""
			s.find("m1").GaleraSSTDonor = ""
		}, []string{"m0", "m1"}},
		{"joined member remains running on release", func(s *state) {
			s.find("m1").Stage = "joined"
			s.RestoreGTIDFence = ""
			s.find("m1").GaleraSSTDonor = ""
		}, []string{"m0", "m1"}},
		{"nonempty existing member is not an empty joiner", func(s *state) { delete(s.Formation.GTIDCertification.EmptyMembers, "m1"); s.RestoreGTIDFence = "" }, []string{"m0", "m1"}},
		{"bootstrap seed waits for published fence", func(s *state) { s.Formation.State = "bootstrapping"; s.RestoreGTIDFence = "" }, nil},
		{"bootstrap seed waits for acquisition", func(s *state) {
			s.Formation.State = "bootstrapping"
			s.Formation.GTIDCertification.Checkpoint.GTIDCertification.FencesOwned = false
		}, nil},
		{"fenced bootstrap releases only seed", func(s *state) { s.Formation.State = "bootstrapping" }, []string{"m0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := joinBarrierState()
			s.RestoreGTIDFence = "formation-owner"
			s.Formation.GTIDCertification = &pxcFormationCertification{
				Owner: "formation-owner", EmptyMembers: map[string]bool{"m0": true, "m1": true, "m2": true},
				Checkpoint: backupCheckpoint{GTIDCertification: &pxcRestoreCertification{FencesOwned: true}},
			}
			s.find("m1").GaleraSSTDonor, s.find("m2").GaleraSSTDonor = "m0", "m0"
			tc.change(s)
			for _, m := range s.Members {
				if m.Role == "data" && pxcJoinAllowed(s, m) != slices.Contains(tc.allowed, m.Name) {
					t.Fatalf("member %s crossed the empty-member fence/donor barrier", m.Name)
				}
			}
		})
	}
}

func TestPXCJoinBarrierMemberConfigWaitsForPublishedControls(t *testing.T) {
	p, rc, s := joinBarrierConfigFixture(t)
	data := s.data()
	seed, first, second := data[0], data[1], data[2]
	certification := &pxcFormationCertification{
		Owner: "formation-owner", EmptyMembers: map[string]bool{first.Name: true, second.Name: true},
		Checkpoint: backupCheckpoint{GTIDCertification: &pxcRestoreCertification{FencesOwned: true}},
	}
	s.Formation.GTIDCertification = certification
	// Persisted acquisition intent alone must not release a new SST joiner.
	assertPXCJoinConfig(t, p, rc, s, seed.Name)
	s.RestoreGTIDFence = certification.Owner
	// Owner publication precedes donor pinning; both are required.
	assertPXCJoinConfig(t, p, rc, s, seed.Name)
	first.GaleraSSTDonor, second.GaleraSSTDonor = seed.Name, seed.Name
	assertPXCJoinConfig(t, p, rc, s, seed.Name, first.Name)
	first.Stage, first.Initialized = "joined", true
	assertPXCJoinConfig(t, p, rc, s, seed.Name, first.Name, second.Name)
	second.Stage, second.Initialized = "joined", true
	certification.EmptyMembers = nil
	certification.Checkpoint.GTIDCertification.FencesOwned = false
	s.RestoreGTIDFence = ""
	first.GaleraSSTDonor, second.GaleraSSTDonor = "", ""
	assertPXCJoinConfig(t, p, rc, s, seed.Name, first.Name, second.Name)
}

func TestPXCJoinBarrierJoinOnlyNeverUsesGlobalBirthAuthority(t *testing.T) {
	for _, scenario := range []string{"owned", "no-member-fence", "no-acquisition", "no-donor", "bootstrap-is-not-join-only", "legacy"} {
		t.Run(scenario, func(t *testing.T) {
			s := joinBarrierState()
			s.find("m1").Stage, s.find("m2").Stage = "joined", "joined"
			first := &member{Name: "m3", Role: "data", Index: 3, Stage: "deployed", GaleraJoinFence: true, GaleraSSTDonor: "m0"}
			second := &member{Name: "m4", Role: "data", Index: 4, Stage: "deployed", GaleraJoinFence: true, GaleraSSTDonor: "m0"}
			s.Members = append(s.Members, second, first)
			s.Formation.GTIDCertification = &pxcFormationCertification{
				Owner: "join-owner", JoinOnly: true, EmptyMembers: map[string]bool{"m3": true, "m4": true},
				Checkpoint: backupCheckpoint{GTIDCertification: &pxcRestoreCertification{FencesOwned: true}},
			}
			switch scenario {
			case "no-member-fence":
				first.GaleraJoinFence = false
			case "no-acquisition":
				s.Formation.GTIDCertification.Checkpoint.GTIDCertification.FencesOwned = false
			case "no-donor":
				first.GaleraSSTDonor = ""
			case "bootstrap-is-not-join-only":
				s.Formation.State = "bootstrapping"
			case "legacy":
				s.Formation.GTIDCertification.Legacy = true
				first.GaleraJoinFence, first.GaleraSSTDonor = false, ""
			}
			expected := scenario == "owned" || scenario == "legacy"
			if pxcJoinAllowed(s, first) != expected || pxcJoinAllowed(s, second) {
				t.Fatal("join-only gate ignored member fence/acquisition/donor ordering")
			}
			if s.Formation.State == "formed" {
				for _, name := range []string{"m0", "m1", "m2"} {
					if !pxcJoinAllowed(s, s.find(name)) {
						t.Fatal("join-only certification stopped an established member")
					}
				}
			}
		})
	}
}

func TestPXCJoinOnlyConfigFencesNewMemberWithoutStoppingProxy(t *testing.T) {
	p, rc, s := joinBarrierConfigFixture(t)
	storage := &domain.BackupStorage{Namespace: rc.Cluster.Namespace, Name: "archive", Type: "s3", EndpointURL: "https://objects.example.test", Bucket: "backups", Region: "region", Prefix: "prefix", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage, rc.Cluster.Spec.Backup.PITR.Enabled = storage.Name, true
	for _, m := range s.Members {
		m.Stage, m.Initialized = "joined", true
	}
	joining := s.data()[2]
	joining.GaleraJoinFence, joining.GaleraSSTDonor = true, s.Formation.Seed
	s.Formation.GTIDCertification = &pxcFormationCertification{
		Owner: "join-owner", JoinOnly: true, EmptyMembers: map[string]bool{joining.Name: true},
		FencedMembers: map[string]bool{joining.Name: true},
		Checkpoint:    backupCheckpoint{GTIDCertification: &pxcRestoreCertification{FencesOwned: true}},
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		config, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec myspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if err := spec.Validate(); err != nil {
			t.Fatalf("real validator rejected join-only policy for %s: %v", m.Name, err)
		}
		if m.Role == "proxy" {
			if !slices.Contains(config.DesiredProcesses, "proxysql") || len(spec.Proxy.Backends) != 2 {
				t.Fatal("join-only certification stopped proxy or exposed a fenced joiner")
			}
			for _, backend := range spec.Proxy.Backends {
				if backend.Name == joining.Name {
					t.Fatal("per-member-fenced joiner entered published proxy backends")
				}
			}
			continue
		}
		if spec.Galera.RestoreFence != (m.Name == joining.Name) || spec.Backup.Binlog.Enabled == (m.Name == joining.Name) ||
			!slices.Contains(config.DesiredProcesses, "mysqld") {
			t.Fatal("join-only fence affected existing processes/archive or failed to isolate joining member")
		}
	}
	// Removing desired offline_mode is not native release acknowledgement.
	// Empty-members erase authority may already be retired at this point.
	joining.GaleraJoinFence, joining.GaleraSSTDonor = false, ""
	s.Formation.GTIDCertification.EmptyMembers = nil
	for _, scenario := range []string{"blocked-cleanup", "uncertified-cleanup", "certified-release-pending", "released"} {
		certification := s.Formation.GTIDCertification
		certification.Blocked = ""
		if scenario == "blocked-cleanup" {
			certification.Blocked = "joining member could not prove its SID mapping"
		}
		certification.Checkpoint.GTIDCertification.Certified = scenario == "certified-release-pending" || scenario == "released"
		certification.Checkpoint.GTIDCertification.FencesOwned = scenario == "certified-release-pending"
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		config, err := p.MemberConfig(t.Context(), rc, rc.Cluster.Name+"mx")
		if err != nil {
			t.Fatal(err)
		}
		var spec myspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if err := spec.Validate(); err != nil {
			t.Fatal(err)
		}
		published := false
		for _, backend := range spec.Proxy.Backends {
			published = published || backend.Name == joining.Name
		}
		if published != (scenario == "released") || !slices.Contains(config.DesiredProcesses, "proxysql") {
			t.Fatal("proxy publication outran native release proof or stopped existing clients")
		}
	}
}

func TestPXCHistoryRoutingHoldSurvivesAdmissionOwnerChange(t *testing.T) {
	s := joinBarrierState()
	s.find("m1").GaleraHistoryBlocked = true
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored state
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	// Starting a different empty join must not route a previously unproven
	// member merely because its original admission owner was superseded.
	restored.Formation.GTIDCertification = &pxcFormationCertification{JoinOnly: true, FencedMembers: map[string]bool{"m2": true}}
	if !pxcJoinRoutingFenced(&restored, restored.find("m1")) {
		t.Fatal("a new admission owner erased the persisted unique-history route hold")
	}
	clearPXCBlockedHistoryRouting(&restored)
	if pxcJoinRoutingFenced(&restored, restored.find("m1")) {
		t.Fatal("successful full-membership certification could not retire the route hold")
	}
}
