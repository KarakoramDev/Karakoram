// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type partitionInventoryAgents map[string]testDialer

func (d partitionInventoryAgents) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	return d[member].Dial(ctx, ref, member)
}

func partitionInventoryFixture(t *testing.T, rc *engine.Context, allHung bool) {
	t.Helper()
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://storage.example.test", Bucket: "test-bucket", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = "archive"
	rc.Clock = engine.RealClock{}
	agents := partitionInventoryAgents{}
	for _, member := range []string{"demom0", "demom1", "demom2"} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			enc := json.NewEncoder(w)
			switch r.URL.Path {
			case "/v1/status":
				_ = enc.Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
			case "/v1/pbm":
				var request agentapi.PBMRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				if member == "demom0" || allHung {
					<-r.Context().Done()
					return
				}
				output := `{}`
				if request.Command == "list" {
					output = `{"snapshots":[],"pitr":{"ranges":[]}}`
				} else if request.Command == "status" {
					output = `{"backups":{"snapshot":[]}}`
				}
				_ = enc.Encode(agentapi.PBMResponse{Output: json.RawMessage(output)})
			default:
				_ = enc.Encode(agentapi.ActionResponse{Success: true})
			}
		}))
		t.Cleanup(server.Close)
		agents[member] = testDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	}
	rc.Agents = agents
}

func TestPartitionPBMInventoryDoesNotBlockHealthyMembersOrPITRSync(t *testing.T) {
	p, rc, _ := writableReadinessFixture(t)
	partitionInventoryFixture(t, rc, false)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	env, err := p.pbmEnv(t.Context(), rc, s)
	if err != nil {
		t.Fatal(err)
	}
	env.PITR.Enabled = true
	disabled := env
	disabled.PITR.Enabled = false
	config, err := pbm.RenderConfig(disabled)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(config)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	// Sync's EnsureConfig must consult base inventory without waiting for m0's
	// local PBM command to reach its unreachable primary.
	if _, err := pbm.EnsureConfig(ctx, env, hex.EncodeToString(sum[:])); err != nil {
		t.Fatal("PITR convergence blocked behind partitioned secondary", err)
	}
	status, err := engine.ObserveStatus(ctx, p, rc)
	if err != nil || status.Phase != domain.PhaseReady || status.ObservedAt.IsZero() {
		t.Fatal("healthy member observations were discarded by PBM", status, err)
	}
	for _, member := range status.Members {
		if !member.Health || !member.Agent.Healthy {
			t.Fatal("healthy agent reported disconnected", member)
		}
	}
}

func TestPartitionUnavailableInventoryKeepsFreshMemberTruth(t *testing.T) {
	p, rc, _ := writableReadinessFixture(t)
	partitionInventoryFixture(t, rc, true)
	lastBackup := time.Now().Add(-time.Hour)
	rc.Cluster.Status.Backup.LastBackup = &lastBackup
	start := time.Now()
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady || status.ObservedAt.IsZero() {
		t.Fatal("backup timeout invalidated independent member observations", status, err)
	}
	if elapsed := time.Since(start); elapsed >= engine.ObservationSweepTimeout {
		t.Fatal("inventory consumed the entire member sweep", elapsed)
	}
	if status.Backup.LastBackup == nil || !status.Backup.LastBackup.Equal(lastBackup) {
		t.Fatal("inventory timeout discarded historical backup summary", status.Backup)
	}
	for _, member := range status.Members {
		if !member.Health || !member.Agent.Healthy || member.Agent.LastSeen.Before(start) {
			t.Fatal("fresh reachable member was reported down", member)
		}
	}
	for _, condition := range status.Conditions {
		if condition.Type == "Backup" && condition.Status == "False" && condition.Reason == "InventoryUnavailable" {
			return
		}
	}
	t.Fatal("inventory failure was hidden", status.Conditions)
}

func TestPartitionHungAgentPreservesHealthyPeers(t *testing.T) {
	p, rc, _ := writableReadinessFixture(t)
	rc.Clock = engine.RealClock{}
	rc.Agents = &readinessAgentDialer{base: rc.Agents, failed: map[string]bool{"demom0": true}, hung: true}
	start := time.Now()
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseDegraded {
		t.Fatal("hung member did not produce isolated degradation", status, err)
	}
	if elapsed := time.Since(start); elapsed >= engine.ObservationSweepTimeout {
		t.Fatal("hung member exceeded its independent probe bound", elapsed)
	}
	for _, member := range status.Members {
		if member.Name == "demom0" {
			if member.Health || member.Agent.Healthy {
				t.Fatal("hung agent was reported connected", member)
			}
		} else if !member.Health || !member.Agent.Healthy || member.Agent.LastSeen.Before(start) {
			t.Fatal("hung member invalidated its healthy sibling", member)
		}
	}
}

type partitionMongoDialer struct {
	primary, secondary *replacementMongo
}

func (d partitionMongoDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if !strings.HasPrefix(address, "demom1:") {
		return d.secondary.DialContext(ctx, network, address)
	}
	return d.primary.DialContext(ctx, network, address)
}

func TestPartitionReplicationUsesPrimaryViewAndNamesAffectedMember(t *testing.T) {
	p, rc, primary := writableReadinessFixture(t)
	at := time.Now()
	primary.handshake = bson.M{"ok": 1, "ismaster": true, "isWritablePrimary": true, "setName": "rs0", "hosts": bson.A{"demom0:27017", "demom1:27017", "demom2:27017"}, "primary": "demom1:27017", "minWireVersion": 0, "maxWireVersion": 25, "maxBsonObjectSize": 16777216, "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000, "saslSupportedMechs": bson.A{"SCRAM-SHA-256"}}
	primary.observed = &mongoadmin.Status{Set: "rs0", Members: []mongoadmin.Member{
		{Name: "demom0:27017", State: "(not reachable/healthy)", Health: 0, OptimeDate: at},
		{Name: "demom1:27017", State: "PRIMARY", Health: 1, OptimeDate: at},
		{Name: "demom2:27017", State: "SECONDARY", Health: 1, OptimeDate: at},
	}}
	secondary := &replacementMongo{t: t, auth: primary.auth, config: primary.config, command: func(bson.Raw) bson.M {
		return bson.M{"ok": 1}
	}, handshake: bson.M{"ok": 1, "ismaster": false, "isWritablePrimary": false, "secondary": true, "setName": "rs0", "hosts": bson.A{"demom0:27017", "demom1:27017", "demom2:27017"}, "primary": "demom1:27017", "minWireVersion": 0, "maxWireVersion": 25, "maxBsonObjectSize": 16777216, "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000, "saslSupportedMechs": bson.A{"SCRAM-SHA-256"}}, observed: &mongoadmin.Status{Set: "rs0", Members: []mongoadmin.Member{
		{Name: "demom0:27017", State: "SECONDARY", Health: 1, OptimeDate: at},
		{Name: "demom1:27017", State: "(not reachable/healthy)", Health: 0, OptimeDate: at},
		{Name: "demom2:27017", State: "SECONDARY", Health: 1, OptimeDate: at},
	}}}
	rc.Dialer = partitionMongoDialer{primary, secondary}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseDegraded || status.ObservedAt.IsZero() {
		t.Fatal("partition did not produce a fresh member-scoped degradation", status, err)
	}
	for _, member := range status.Members {
		if !member.Agent.Healthy || member.Health != (member.Name != "demom0") {
			t.Fatal("secondary-view partition leaked into reachable peers", member, status.Conditions)
		}
	}
	for _, condition := range status.Conditions {
		if condition.Type == "Healthy" && strings.Contains(condition.Message, "unhealthy or unobserved: demom0") && !strings.Contains(condition.Message, "demom1") && !strings.Contains(condition.Message, "demom2") {
			return
		}
	}
	t.Fatal("health condition did not identify the affected secondary", status.Conditions)
}

func TestPartitionChainedSecondaryLagRemainsTruthful(t *testing.T) {
	for _, lag := range []time.Duration{14 * time.Second, 10 * time.Second, 0} {
		t.Run(lag.String(), func(t *testing.T) {
			p, rc, primary := writableReadinessFixture(t)
			at := time.Now()
			// m1 remains reachable and SECONDARY, but is still fetching from
			// isolated m2 until MongoDB chooses a new sync source.
			primary.observed = &mongoadmin.Status{Set: "rs0", Members: []mongoadmin.Member{
				{Name: "demom0:27017", State: "PRIMARY", Health: 1, OptimeDate: at},
				{Name: "demom1:27017", State: "SECONDARY", Health: 1, OptimeDate: at.Add(-lag)},
				{Name: "demom2:27017", State: "(not reachable/healthy)", Health: 0, OptimeDate: at.Add(-14 * time.Second)},
			}}
			status, err := engine.ObserveStatus(t.Context(), p, rc)
			if err != nil || status.Phase != domain.PhaseDegraded || status.ObservedAt.IsZero() {
				t.Fatal("partition did not produce fresh degradation", status, err)
			}
			for _, member := range status.Members {
				wantHealthy := member.Name == "demom0" || member.Name == "demom1" && lag <= 10*time.Second
				if !member.Agent.Healthy || member.Health != wantHealthy {
					t.Fatal("partition or lag truth was lost", member)
				}
				if member.Name == "demom1" && (member.State != "SECONDARY" || member.LagSeconds != lag.Seconds()) {
					t.Fatal("lagging secondary lost its observed state", member)
				}
			}
			var replication, health domain.Condition
			for _, condition := range status.Conditions {
				switch condition.Type {
				case "Replication/rs0":
					replication = condition
				case "Healthy":
					health = condition
				}
			}
			if health.Status != "False" || !strings.Contains(health.Message, "demom2") || strings.Contains(health.Message, "demom1") != (lag > 10*time.Second) {
				t.Fatal("health condition lost affected members", health)
			}
			if lag > 10*time.Second {
				if replication.Status != "Unknown" || replication.Reason != "MemberObservationUnavailable" || !strings.Contains(replication.Message, "demom1: replication lag 14.0 seconds") || !strings.Contains(replication.Message, "demom2: observation unavailable") {
					t.Fatal("observed replication lag mislabeled as missing observation", replication)
				}
			} else if replication.Status != "Unknown" || replication.Reason != "MemberObservationUnavailable" || !strings.Contains(replication.Message, "demom2") {
				t.Fatal("cut member must remain unobserved after peer catches up", replication)
			}
		})
	}
}

type partitionRuntimeBackend struct {
	infra.Backend
	containers []infra.Container
	err        error
}

func (b partitionRuntimeBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if name == "demom1" {
		return b.containers, b.err
	}
	return b.Backend.Containers(ctx, name)
}

func TestPartitionLagDoesNotMaskUnavailableRuntime(t *testing.T) {
	for _, tc := range []struct {
		name       string
		containers []infra.Container
		err        error
	}{
		{name: "inventory error", err: errors.New("runtime inventory unavailable")},
		{name: "empty inventory"},
		{name: "stopped container", containers: []infra.Container{{Status: infra.ContainerRunning}, {Status: "stopped"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, primary := writableReadinessFixture(t)
			rc.Backend = partitionRuntimeBackend{Backend: rc.Backend, containers: tc.containers, err: tc.err}
			at := time.Now()
			primary.observed = &mongoadmin.Status{Set: "rs0", Members: []mongoadmin.Member{
				{Name: "demom0:27017", State: "PRIMARY", Health: 1, OptimeDate: at},
				{Name: "demom1:27017", State: "SECONDARY", Health: 1, OptimeDate: at.Add(-14 * time.Second)},
				{Name: "demom2:27017", State: "(not reachable/healthy)", Health: 0, OptimeDate: at.Add(-14 * time.Second)},
			}}
			status, err := engine.ObserveStatus(t.Context(), p, rc)
			if err != nil || status.Phase != domain.PhaseDegraded || status.ObservedAt.IsZero() {
				t.Fatal("runtime failure did not produce fresh degradation", status, err)
			}
			for _, member := range status.Members {
				if member.Name == "demom1" && (member.Health || !member.Agent.Healthy || member.LagSeconds != 14) {
					t.Fatal("runtime failure lost independent agent or lag truth", member)
				}
			}
			for _, condition := range status.Conditions {
				if condition.Type == "Replication/rs0" {
					if condition.Status != "Unknown" || condition.Reason != "MemberObservationUnavailable" || !strings.Contains(condition.Message, "demom1") {
						t.Fatal("lag masked unavailable runtime observation", condition)
					}
					return
				}
			}
			t.Fatal("missing replication condition", status.Conditions)
		})
	}
}

func TestPartitionReplicationEnumeratesMixedMemberFailures(t *testing.T) {
	for _, cut := range []bool{true, false} {
		t.Run(fmt.Sprintf("cut-first-member=%t", cut), func(t *testing.T) {
			p, rc, primary := writableReadinessFixture(t)
			at := time.Now()
			primary.observed = &mongoadmin.Status{Set: "rs0", Members: []mongoadmin.Member{
				{Name: "demom0:27017", State: "PRIMARY", Health: 1, OptimeDate: at},
				{Name: "demom1:27017", State: "SECONDARY", Health: 1, OptimeDate: at},
				{Name: "demom2:27017", State: "SECONDARY", Health: 1, OptimeDate: at.Add(-13 * time.Second)},
			}}
			wantStatus, wantReason := "False", "ReplicationLag"
			wantMessage := "demom2: replication lag 13.0 seconds (10-second health threshold)"
			if cut {
				primary.observed.Members[1].Health = 0
				primary.observed.Members[1].State = "(not reachable/healthy)"
				wantStatus, wantReason = "Unknown", "MemberObservationUnavailable"
				wantMessage = "demom1: observation unavailable; " + wantMessage
			}
			status, err := engine.ObserveStatus(t.Context(), p, rc)
			if err != nil || status.Phase != domain.PhaseDegraded {
				t.Fatal("member failures did not degrade status", status, err)
			}
			for _, condition := range status.Conditions {
				if condition.Type == "Replication/rs0" {
					if condition.Status != wantStatus || condition.Reason != wantReason || condition.Message != wantMessage || len(condition.Message) > 2048 {
						t.Fatal("mixed failures lost member attribution or precedence", condition)
					}
					return
				}
			}
			t.Fatal("missing replication condition", status.Conditions)
		})
	}
}
