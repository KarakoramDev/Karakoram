// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type rotationClock struct{ stamp time.Time }

func (c rotationClock) Now() time.Time { return c.stamp }

func TestFCVAcceptanceAndTerminalFailure(t *testing.T) {
	p, rc, _, _ := setup(t)
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	s.Phase = "ready"
	for _, m := range s.Members {
		m.Stage, m.Version = "active", rc.Cluster.Spec.Version
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateFCV(context.Background(), rc, "8.0"); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"8.0.32-14", "7.0", "", "8"} {
		err := p.ValidateFCV(context.Background(), rc, version)
		var field *engine.ValidationError
		if !errors.As(err, &field) || field.Field != "version" || !strings.Contains(err.Error(), "8.0") {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(map[string]string{"version": version})
		res, err := p.RunOperation(context.Background(), rc, &domain.Operation{Kind: domain.OperationRaiseFCV, Payload: payload})
		if err == nil || !res.Done || res.Message == "" {
			t.Fatal("FCV failure retried", res, err)
		}
	}
	s.Members[0].Version = "7.0.43-23"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	var conflict *domain.ResourceError
	if err := p.ValidateFCV(context.Background(), rc, "8.0"); !errors.As(err, &conflict) || conflict.Reason != domain.ReasonInvalidState {
		t.Fatal(err)
	}
	res, err := p.RunOperation(context.Background(), rc, &domain.Operation{Kind: domain.OperationRaiseFCV, Payload: json.RawMessage(`{"version":"8.0"}`)})
	if err == nil || !res.Done {
		t.Fatal("rolling FCV failure retried", res, err)
	}
}

func TestPITRInitializingAndNoCoverageConflicts(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Spec.Backup.Storage = "primary"
	var conflict *domain.ResourceError
	if _, err := p.PITRWindow(context.Background(), rc); !errors.As(err, &conflict) || conflict.Reason != domain.ReasonInvalidState || !strings.Contains(err.Error(), "initializing") {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = ""
	if _, err := p.ValidatePITRRestore(context.Background(), rc, time.Now()); !errors.As(err, &conflict) || conflict.Reason != domain.ReasonInvalidState || !strings.Contains(err.Error(), "no PITR coverage") {
		t.Fatal(err)
	}
}

func TestPITRPreflightUsesLiveBackedCoverage(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Clock = rotationClock{time.Unix(500, 0).UTC()}
	rc.Cluster.Spec.Backup.Storage = "primary"
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	s.Phase, s.PBMPrefix = "ready", "karakoram/demo"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err := rc.Store.(*store.Store).CreateBackupStorage(context.Background(), &domain.BackupStorage{Name: "primary", Type: "s3"}); err != nil {
		t.Fatal(err)
	}
	covered := true
	rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
			return
		}
		raw := `{"snapshots":[],"pitr":{"ranges":[{"start":100,"end":200},{"start":300,"end":400}]}}`
		if !covered {
			raw = `{"snapshots":[],"pitr":{"ranges":[]}}`
		}
		_ = json.NewEncoder(w).Encode(agentapi.PBMResponse{Output: json.RawMessage(raw)})
	})
	for _, second := range []int64{99, 250, 401, 600} {
		windows, err := p.ValidatePITRRestore(context.Background(), rc, time.Unix(second, 0))
		var field *domain.FieldError
		if len(windows) != 2 || !errors.As(err, &field) || field.Reason != domain.ReasonOutOfRange {
			t.Fatal(windows, err)
		}
	}
	if _, err := p.ValidatePITRRestore(context.Background(), rc, time.Unix(150, 0)); err != nil {
		t.Fatal(err)
	}
	covered = false
	var conflict *domain.ResourceError
	if _, err := p.ValidatePITRRestore(context.Background(), rc, time.Unix(150, 0)); !errors.As(err, &conflict) || conflict.Reason != domain.ReasonInvalidState {
		t.Fatal(err)
	}
}

func TestSystemUserRotationPersistsCompletionTime(t *testing.T) {
	p, rc, _, _ := setup(t)
	stamp := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	rc.Clock = rotationClock{stamp}
	// Resume the checkpoint after password writes and member reloads; the last
	// successful step is the only point that should record rotation metadata.
	op := &domain.Operation{ID: "rotation", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"databaseAdmin"}`), Checkpoint: json.RawMessage(`{"stage":"reload"}`)}
	res, err := p.RunOperation(context.Background(), rc, op)
	if err != nil || !res.Done {
		t.Fatal(res, err)
	}
	st := rc.Store.(*store.Store)
	if err := st.UpdateClusterState(context.Background(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Cluster, err = st.GetCluster(context.Background(), rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	p = New(Options{})
	listed, err := p.SystemUsers(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, user := range listed {
		if user.Name == "databaseAdmin" {
			if user.LastRotatedAt == nil || !user.LastRotatedAt.Equal(stamp) {
				t.Fatal("rotation timestamp lost", user)
			}
		} else if user.LastRotatedAt != nil {
			t.Fatal("unrotated user timestamp", user)
		}
	}
}

func TestPlacementUnknownIsNotColocation(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	s, _ := load(rc.Cluster)
	plan(rc.Cluster, s)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(agentapi.Status{})
	})
	v, err := p.Placement(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Groups["unknown"]) != 3 || strings.Contains(v.Warning, "co-located") || !strings.Contains(v.Warning, "placement unknown until members report") {
		t.Fatal(v)
	}
}

var _ engine.Clock = rotationClock{}
