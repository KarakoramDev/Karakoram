// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestOrphanedVolumeCRUDAndClusterIndependence(t *testing.T) {
	s, path := testStore(t)
	ctx := context.Background()
	at := time.Date(2024, 1, 7, 0, 0, 0, 0, time.UTC)
	v := domain.OrphanedVolume{Hostname: "demom3v", ServiceID: "volume-id", Cluster: "demo", Member: "demom3", FirstError: "volume deletion rejected", LastError: "volume deletion rejected again", FirstFailedAt: at, OrphanedAt: at.Add(time.Hour), LastAttemptAt: at, NextAttemptAt: at.Add(time.Hour), Attempts: 130, LastProcessID: "process", ProcessPending: true, JanitorFailures: 3}
	if err := s.CreateCluster(ctx, &domain.Cluster{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutOrphanedVolume(ctx, &v); err != nil {
		t.Fatal(err)
	}
	other := v
	other.Hostname = "am0v"
	if err := s.PutOrphanedVolume(ctx, &other); err != nil {
		t.Fatal(err)
	}
	items, err := s.ListOrphanedVolumes(ctx, "")
	if err != nil || len(items) != 2 || items[0].Hostname != "am0v" {
		t.Fatal(items, err)
	}
	v.LastError, v.Attempts = "retry failed", 131
	if err := s.PutOrphanedVolume(ctx, &v); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteCluster(ctx, domain.ParseClusterKey("demo")); err != nil {
		t.Fatal(err)
	}
	// Reopen the same SQLite file to exercise persisted private bookkeeping.
	reopened, err := Open(path, s.sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.GetOrphanedVolume(ctx, domain.DefaultNamespace, v.Hostname)
	if err != nil || *got != v {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	data, err := json.Marshal(got)
	if err != nil || strings.Contains(string(data), "processPending") || strings.Contains(string(data), "janitorFailures") {
		t.Fatal(string(data), err)
	}
	if err := reopened.DeleteOrphanedVolume(ctx, domain.DefaultNamespace, v.Hostname); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.GetOrphanedVolume(ctx, domain.DefaultNamespace, v.Hostname); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
