// SPDX-License-Identifier: Apache-2.0
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func drillSource(t *testing.T, s *Store, name string) domain.ClusterRef {
	t.Helper()
	c := domain.Cluster{Name: name, Spec: domain.ClusterSpec{Provider: "psmdb"}}
	if err := s.CreateCluster(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	return c.Ref()
}

func drillRecord(ref domain.ClusterRef, id string, created time.Time) domain.BackupDrill {
	return domain.BackupDrill{ID: id, Namespace: ref.Namespace, Cluster: ref.Name, Phase: "Pending", Trigger: "manual", CreatedAt: created}
}

func TestBackupDrillAdmissionAndPhaseCAS(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	ref := drillSource(t, s, "source")
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	drill := drillRecord(ref, "drill", now)
	if err := s.CreateBackupDrill(ctx, drill); err != nil {
		t.Fatal(err)
	}
	drill.Phase = "Provisioning"
	ok, err := s.UpdateBackupDrill(ctx, drill, "Pending")
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err = s.CreateBackupDrill(ctx, drillRecord(ref, drill.ID, now)); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetBackupDrill(ctx, drill.ID)
	if err != nil || got.Phase != "Provisioning" {
		t.Fatal(got, err)
	}
	if ok, err = s.UpdateBackupDrill(ctx, drill, "Pending"); err != nil || ok {
		t.Fatal("stale claim", ok, err)
	}
	other := drillSource(t, s, "other")
	if err = s.CreateBackupDrill(ctx, drillRecord(other, drill.ID, now)); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	changed := drill
	changed.Cluster = other.Name
	if ok, err = s.UpdateBackupDrill(ctx, changed, "Provisioning"); err != nil || ok {
		t.Fatal("source mutation", ok, err)
	}
	changed = drill
	changed.CreatedAt = now.Add(time.Second)
	if ok, err = s.UpdateBackupDrill(ctx, changed, "Provisioning"); err != nil || ok {
		t.Fatal("creation mutation", ok, err)
	}
	drill.Phase = "Succeeded"
	drill.CompletedAt = &now
	if ok, err = s.UpdateBackupDrill(ctx, drill, "Provisioning"); err != nil || !ok {
		t.Fatal(ok, err)
	}
	drill.Phase = "Pending"
	if ok, err = s.UpdateBackupDrill(ctx, drill, "Succeeded"); err != nil || ok {
		t.Fatal("terminal resumed", ok, err)
	}
	if _, err = s.GetBackupDrill(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err = s.CreateBackupDrill(ctx, drillRecord(domain.ClusterRef{Name: "absent"}, "absent", now)); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err = s.CreateBackupDrill(ctx, drillRecord(ref, "", now)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestBackupDrillConcurrentClaim(t *testing.T) {
	s, _ := testStore(t)
	ref := drillSource(t, s, "source")
	drill := drillRecord(ref, "claim", time.Now().UTC())
	if err := s.CreateBackupDrill(t.Context(), drill); err != nil {
		t.Fatal(err)
	}
	drill.Phase = "Provisioning"
	results := make(chan bool, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := s.UpdateBackupDrill(t.Context(), drill, "Pending")
			if err != nil {
				t.Error(err)
			}
			results <- ok
		}()
	}
	wg.Wait()
	close(results)
	wins := 0
	for ok := range results {
		if ok {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("claim winners=%d", wins)
	}
}

func TestBackupDrillOrderingRetentionAndMetrics(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	ref := drillSource(t, s, "source")
	other := drillSource(t, s, "other")
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for i := range 55 {
		drill := drillRecord(ref, fmt.Sprintf("run-%02d", i), base.Add(time.Duration(i)*time.Minute))
		if err := s.CreateBackupDrill(ctx, drill); err != nil {
			t.Fatal(err)
		}
		completed := drill.CreatedAt.Add(time.Second)
		drill.CompletedAt = &completed
		drill.Phase = "Succeeded"
		drill.RestoreDurationSeconds = int64(i)
		if i == 54 {
			drill.Phase = "Failed"
		}
		if ok, err := s.UpdateBackupDrill(ctx, drill, "Pending"); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	for _, drill := range []domain.BackupDrill{
		drillRecord(ref, "pending", base.Add(-time.Hour)),
		drillRecord(ref, "tie-a", base.Add(time.Hour)),
		drillRecord(other, "tie-b", base.Add(time.Hour)),
	} {
		if err := s.CreateBackupDrill(ctx, drill); err != nil {
			t.Fatal(err)
		}
	}
	cleaning, _ := s.GetBackupDrill(ctx, "tie-a")
	cleaning.Phase = "CleaningUp"
	cleaning.CompletedAt = &base
	if ok, err := s.UpdateBackupDrill(ctx, cleaning, "Pending"); err != nil || !ok {
		t.Fatal(ok, err)
	}
	list, err := s.ListBackupDrills(ctx, ref)
	if err != nil || len(list) != 52 || list[0].ID != "tie-a" || list[1].ID != "run-54" || list[len(list)-1].ID != "pending" {
		t.Fatal(len(list), err)
	}
	if _, err = s.GetBackupDrill(ctx, "run-04"); !errors.Is(err, ErrNotFound) {
		t.Fatal("old report retained", err)
	}
	if _, err = s.GetBackupDrill(ctx, "run-05"); err != nil {
		t.Fatal(err)
	}
	active, err := s.ListActiveBackupDrills(ctx)
	if err != nil || len(active) != 3 || active[0].ID != "pending" || active[1].ID != "tie-a" || active[2].ID != "tie-b" {
		t.Fatal(active, err)
	}
	metrics, err := s.LatestBackupDrillTimes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	times := metrics[ref]
	if !times.Success.Equal(base.Add(53*time.Minute+time.Second)) || !times.Failure.Equal(base.Add(54*time.Minute+time.Second)) || times.LastRestoreDuration != 53*time.Second {
		t.Fatal(times)
	}
	if _, ok := metrics[other]; ok {
		t.Fatal("active report counted in metrics")
	}
}

func TestBackupDrillDeletionAndDurableSchedule(t *testing.T) {
	s, path := testStore(t)
	ctx := t.Context()
	source := drillSource(t, s, "source")
	target := drillSource(t, s, "target")
	other := drillSource(t, s, "other")
	drill := drillRecord(source, "report", time.Now().UTC())
	drill.TargetCluster = target.Name
	if err := s.CreateBackupDrill(ctx, drill); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSetting(ctx, "drills/cleanup/"+drill.ID, json.RawMessage(`{"attempts":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSetting(ctx, "drills/event/"+drill.ID, json.RawMessage(`true`)); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []domain.ClusterRef{source, other} {
		key := "drills/schedule/" + ref.Key()
		if err := s.SetSetting(ctx, key, json.RawMessage(`{"lastDueAt":"2026-10-01T00:00:00Z"}`)); err != nil {
			t.Fatal(err)
		}
	}
	// A separately opened store observes persisted rows and settings.
	reopened, err := Open(path, s.sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err = reopened.GetBackupDrill(ctx, drill.ID); err != nil {
		t.Fatal(err)
	}
	sourceKey := "drills/schedule/" + source.Key()
	if _, err = reopened.GetSetting(ctx, sourceKey); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteCluster(ctx, target); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetBackupDrill(ctx, drill.ID); err != nil {
		t.Fatal("target deletion removed source report", err)
	}
	if err = s.DeleteCluster(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, err = reopened.GetBackupDrill(ctx, drill.ID); err != nil {
		t.Fatal("source deletion removed report", err)
	}
	if _, err = s.GetSetting(ctx, sourceKey); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err = reopened.GetSetting(ctx, "drills/cleanup/"+drill.ID); err != nil {
		t.Fatal("source deletion removed cleanup retry state", err)
	}
	if _, err = reopened.GetSetting(ctx, "drills/event/"+drill.ID); err != nil {
		t.Fatal("source deletion removed event deduplication state", err)
	}
	active, err := reopened.ListActiveBackupDrills(ctx)
	if err != nil || len(active) != 1 || active[0].ID != drill.ID {
		t.Fatal("source deletion hid cleanup work", active, err)
	}
	drill.Phase = "Failed"
	drill.Cleanup.Phase = "Done"
	if ok, updateErr := reopened.UpdateBackupDrill(ctx, drill, "Pending"); updateErr != nil || !ok {
		t.Fatal("could not finish report after source deletion", ok, updateErr)
	}
	if got, getErr := s.GetBackupDrill(ctx, drill.ID); getErr != nil || got.Phase != "Failed" {
		t.Fatal("final report was not retained", got, getErr)
	}
	for _, key := range []string{"drills/cleanup/" + drill.ID, "drills/event/" + drill.ID} {
		if _, err = s.GetSetting(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatal("terminal report retained transient bookkeeping", key, err)
		}
	}
	if _, err = s.GetSetting(ctx, "drills/schedule/"+other.Key()); err != nil {
		t.Fatal(err)
	}
	if err = s.CreateBackupDrill(ctx, drill); !errors.Is(err, ErrNotFound) {
		t.Fatal("resurrected deleted source", err)
	}
}
