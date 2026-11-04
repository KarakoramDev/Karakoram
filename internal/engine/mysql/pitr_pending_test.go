// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestPITRCoveragePendingPublishesProgressWithoutWindow(t *testing.T) {
	p, rc, backend := backupFixture(t, "single")
	rc.Cluster.Spec.Backup.PITR.Enabled = true
	inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
	var freshFlags []bool
	pending := true
	backend.action = func(w http.ResponseWriter, r *http.Request, _ string) bool {
		if r.URL.Path == "/v1/mysql/archive-status" {
			_ = json.NewEncoder(w).Encode(myspec.BinlogArchiveStatus{})
			return true
		}
		if r.URL.Path != "/v1/mysql/pitr-coverage" {
			return false
		}
		var request myspec.PITRCoverageRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		freshFlags = append(freshFlags, request.RequireFresh)
		if !request.AcceptPending {
			t.Error("updated control plane did not negotiate pending responses")
		}
		_ = json.NewEncoder(w).Encode(myspec.PITRCoverageResponse{Pending: pending, Progress: &myspec.PITRCoverageProgress{Stage: "sidecars", Processed: 320, Total: 10000}, Fresh: request.RequireFresh, Windows: []myspec.PITRWindow{{Start: time.Unix(10, 0), End: time.Unix(20, 0)}}})
		return true
	}
	status := domain.ClusterStatus{}
	status.Backup.PITRWindow = &domain.TimeWindow{Start: time.Unix(10, 0), End: time.Unix(20, 0)}
	if err := p.backupStatus(t.Context(), rc, &status); err != nil {
		t.Fatal(err)
	}
	if status.Backup.PITRWindow != nil {
		t.Fatal("pending coverage retained a previously verified window")
	}
	found := false
	for _, condition := range status.Conditions {
		if condition.Type == "PITR" {
			found = true
			if condition.Status == "True" || condition.Reason != "CoveragePending" || !strings.Contains(condition.Message, "320/10000") {
				t.Fatalf("pending progress not exposed: %+v", condition)
			}
		}
	}
	if !found {
		t.Fatal("PITR condition missing")
	}
	_, err := p.PITRWindow(t.Context(), rc)
	var conflict *domain.ResourceError
	if !errors.As(err, &conflict) || conflict.Reason != domain.ReasonInvalidState || !strings.Contains(conflict.Message, "pending") {
		t.Fatalf("public window omitted clear pending error: %v", err)
	}
	pending = false
	if _, err = p.ValidatePITRRestore(t.Context(), rc, time.Unix(15, 0)); err != nil {
		t.Fatal(err)
	}
	if len(freshFlags) < 3 || freshFlags[0] || freshFlags[1] || !freshFlags[len(freshFlags)-1] {
		t.Fatalf("status/admission freshness modes incorrect: %v", freshFlags)
	}
}

func TestPITRAdmissionSupportsLegacySynchronousAndFreshBackgroundAgents(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
		skew   time.Duration
		fresh  bool
	}{
		{name: "legacy-synchronous", legacy: true},
		{name: "background-fresh", fresh: true},
		{name: "background-behind-milliseconds", skew: -5 * time.Millisecond, fresh: true},
		{name: "background-ahead-milliseconds", skew: 5 * time.Millisecond, fresh: true},
		{name: "background-behind-minutes", skew: -5 * time.Minute, fresh: true},
		{name: "background-ahead-minutes", skew: 5 * time.Minute, fresh: true},
		{name: "background-unfresh"},
		{name: "background-stale-behind", skew: -5 * time.Minute},
		{name: "background-stale-ahead", skew: 5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, backend := backupFixture(t, "single")
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
			// Anchor before calling admission so a lagging agent's timestamp is
			// deterministically earlier than the control plane's request start.
			verificationStartedAt := time.Now().UTC().Add(tc.skew)
			backend.action = func(w http.ResponseWriter, r *http.Request, _ string) bool {
				if r.URL.Path != "/v1/mysql/pitr-coverage" {
					return false
				}
				var request myspec.PITRCoverageRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if !request.RequireFresh {
					t.Error("admission did not request freshness")
				}
				response := myspec.PITRCoverageResponse{Windows: []myspec.PITRWindow{{Start: time.Unix(10, 0), End: time.Unix(20, 0)}}}
				if !tc.legacy {
					response.VerificationStartedAt = verificationStartedAt
					response.Fresh = tc.fresh
				}
				_ = json.NewEncoder(w).Encode(response)
				return true
			}
			_, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(15, 0))
			reject := !tc.legacy && !tc.fresh
			if reject && (err == nil || !strings.Contains(err.Error(), "predating")) {
				t.Fatalf("stale background proof admitted: %v", err)
			}
			if !reject && err != nil {
				t.Fatal(err)
			}
		})
	}
}
