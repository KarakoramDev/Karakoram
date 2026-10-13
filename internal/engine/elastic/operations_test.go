// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type opsStore struct {
	engine.ClusterStore
	backup     domain.Backup
	restore    domain.Restore
	storage    domain.BackupStorage
	deleted    bool
	failUpdate bool
}

func (s *opsStore) GetBackup(context.Context, domain.ClusterRef, string) (*domain.Backup, error) {
	if s.deleted {
		return nil, engine.ErrNotFound
	}
	b := s.backup
	return &b, nil
}
func (s *opsStore) UpdateBackup(_ context.Context, b *domain.Backup) error {
	if s.failUpdate {
		s.failUpdate = false
		return errors.New("interrupted inventory write")
	}
	s.backup = *b
	return nil
}
func (s *opsStore) DeleteBackup(context.Context, domain.ClusterRef, string) error {
	s.deleted = true
	return nil
}
func (s *opsStore) GetBackupStorage(context.Context, string, string) (*domain.BackupStorage, error) {
	return &s.storage, nil
}
func (s *opsStore) GetRestore(context.Context, domain.ClusterRef, string) (*domain.Restore, error) {
	r := s.restore
	return &r, nil
}
func (s *opsStore) UpdateRestore(_ context.Context, r *domain.Restore) error {
	s.restore = *r
	return nil
}

type opsSecrets struct {
	engine.SecretStore
	values map[string][]byte
}

func (s *opsSecrets) GetClusterSecret(_ context.Context, _ domain.ClusterRef, key string) ([]byte, error) {
	v, ok := s.values[key]
	if !ok {
		return nil, engine.ErrNotFound
	}
	return v, nil
}
func (s *opsSecrets) SetClusterSecret(_ context.Context, _ domain.ClusterRef, key string, v []byte) error {
	s.values[key] = append([]byte(nil), v...)
	return nil
}
func (s *opsSecrets) DeleteClusterSecret(_ context.Context, _ domain.ClusterRef, key string) error {
	delete(s.values, key)
	return nil
}

type opsDialer struct{ address string }

func (d opsDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

type opsAgents struct{ options agentapi.ClientOptions }

func (a opsAgents) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return agentapi.NewClient(a.options)
}

func opsFixture(t *testing.T, handler http.HandlerFunc) (*Provider, *engine.Context, *state, *opsStore) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	st := &opsStore{storage: domain.BackupStorage{Namespace: domain.DefaultNamespace, Name: "archive", Type: "s3", Bucket: "backups", Prefix: "saved", EndpointURL: "https://storage.example", Region: "us-east-1", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}, backup: domain.Backup{Namespace: domain.DefaultNamespace, Name: "snapshot-one", Cluster: "test", Storage: "archive", Type: "snapshot", Phase: domain.PhasePending}}
	sec := &opsSecrets{values: map[string][]byte{secretCA: ca, secretAdmin: []byte("admin-password"), secretElastic: []byte("elastic-password"), secretLogstash: []byte("logstash-password"), secretAPM: []byte("apm-password"), secretKibanaToken: []byte("kibana-token"), secretKibanaTokenName: []byte("kk-original")}}
	rc := &engine.Context{Cluster: &domain.Cluster{Namespace: domain.DefaultNamespace, Name: "test", Spec: domain.ClusterSpec{Provider: "elasticsearch", Version: "9.5.5", Topology: domain.Topology{Type: "cluster"}}}, Store: st, Secrets: sec, Dialer: opsDialer{u.Host}, Clock: engine.RealClock{}, Agents: opsAgents{agentapi.ClientOptions{BaseURL: server.URL, Token: "agent-token", CAPEM: ca}}}
	s := &state{UID: "unique", ConfigVersion: 1, ClusterUUID: "formed", Storages: []string{storageIdentity(&st.storage)}, Repositories: map[string]*repository{}, NextIndex: map[string]int{}, Members: []*member{{Name: "example.com", Kind: kindNode, Stage: stageJoined, Version: "9.5.5", ConfigVersion: 1}}}
	p := New(ProviderOptions{})
	p.ApplyDefaults(&rc.Cluster.Spec)
	for _, m := range s.esNodes() {
		spec, err := p.desiredSpec(context.Background(), rc, s, m, rc.Cluster.Spec.Version)
		if err != nil {
			t.Fatal(err)
		}
		m.ConfigHash, m.RestartToken, m.HeapMB = spec.RestartToken, spec.RestartToken, spec.HeapMB
		raw, err := json.Marshal(spec.Elasticsearch.SecureSettings)
		if err != nil {
			t.Fatal(err)
		}
		s.SecureSettingsHash = digest(raw)
	}
	return p, rc, s, st
}

func opsJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
func opsCheckpoint(t *testing.T, result engine.OpResult) operationState {
	t.Helper()
	var cp operationState
	if err := json.Unmarshal(result.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func TestOperationsUnsupportedAndSyncPolicy(t *testing.T) {
	p := New(ProviderOptions{})
	result, err := p.runOperation(context.Background(), nil, &state{}, &domain.Operation{Kind: "raise-fcv"})
	if !errors.Is(err, engine.ErrNotSupported) || !result.Done {
		t.Fatalf("unsupported: %+v %v", result, err)
	}
	for _, stage := range []string{"", "join", "remove", "done"} {
		raw, _ := json.Marshal(operationState{Stage: stage})
		op := &domain.Operation{Kind: domain.OperationReplaceMember, Checkpoint: raw}
		if got, want := syncDuringOperation(op), stage == "join" || stage == "remove"; got != want {
			t.Fatalf("sync %s: %v", stage, got)
		}
		op.Kind = domain.OperationRestore
		if syncDuringOperation(op) {
			t.Fatal("restore permitted sync")
		}
	}
}
func TestReplaceMemberRejection(t *testing.T) {
	for _, kind := range []string{kindNode, kindKibana, kindLogstash, kindAPM} {
		t.Run(kind, func(t *testing.T) {
			p := New(ProviderOptions{})
			s := &state{ClusterUUID: "formed", Members: []*member{{Name: "member", Kind: kind, Stage: stageJoined}}}
			result, err := p.runReplaceMember(context.Background(), &engine.Context{Cluster: &domain.Cluster{}}, s, operationState{}, "member")
			if !errors.Is(err, engine.ErrInvalid) || !result.Done {
				t.Fatalf("replacement accepted: %+v %v", result, err)
			}
		})
	}
}
func TestSnapshotLocationAndNames(t *testing.T) {
	storage := &domain.BackupStorage{Name: "archive", Bucket: "backups", Prefix: "saved"}
	b := &domain.Backup{Storage: "archive", Provider: "elasticsearch", Location: snapshotLocation(storage, "saved/default/test/unique", "kk-archive", snapshotName("Backup, One*"))}
	base, repo, name, err := recordedSnapshot(storage, b)
	if err != nil || base != "saved/default/test/unique" || repo != "kk-archive" || name == "" {
		t.Fatalf("location: %s %s %s %v", base, repo, name, err)
	}
	for _, bad := range []string{"s3://backups/saved/../test?repository=x#snap", "s3://other/saved/test?repository=x#snap", "s3://backups/saved/test?repository=x#Upper"} {
		b.Location = bad
		if _, _, _, err = recordedSnapshot(storage, b); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}

func TestDisruptiveOperationsRequireFormation(t *testing.T) {
	for _, kind := range []string{domain.OperationRestart, domain.OperationReplaceMember} {
		for _, problem := range []string{"uuid", "planned", "imported", "deployed", "deleting", "suspended", "phase-suspended"} {
			t.Run(string(kind)+"/"+problem, func(t *testing.T) {
				p := New(ProviderOptions{})
				rc := &engine.Context{Cluster: &domain.Cluster{}}
				s := &state{ClusterUUID: "formed", Members: []*member{{Name: "n0", Kind: kindNode, Stage: stageJoined}, {Name: "n1", Kind: kindNode, Stage: stageJoined}}}
				switch problem {
				case "uuid":
					s.ClusterUUID = ""
				case "deleting":
					rc.Cluster.Deleting = true
				case "suspended":
					s.Suspended = true
				case "phase-suspended":
					rc.Cluster.Status.Phase = domain.PhaseSuspended
				default:
					s.Members[1].Stage = problem
				}
				op := &domain.Operation{Kind: kind, Payload: json.RawMessage(`{"member":"n0"}`)}
				result, err := p.runOperation(context.Background(), rc, s, op)
				if !result.Done || !errors.Is(err, engine.ErrConflict) || s.Rolling != nil || len(s.Members) != 2 {
					t.Fatalf("premature operation: %+v %v %+v", result, err, s)
				}
			})
		}
	}
}
