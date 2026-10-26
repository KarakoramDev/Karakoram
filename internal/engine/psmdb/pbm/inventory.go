// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/redact"
)

type snapshot struct {
	Name          string     `json:"name"`
	Status        string     `json:"status"`
	Type          string     `json:"type"`
	Namespaces    []string   `json:"namespaces"`
	Size          int64      `json:"size"`
	RestoreTo     int64      `json:"restoreTo"`
	LastWriteTS   int64      `json:"last_write_ts"`
	LastWriteTime *time.Time `json:"last_write_time"`
	Start         *time.Time `json:"start"`
	Finish        *time.Time `json:"finish"`
	Error         string     `json:"error"`
}
type epochRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}
type pitrRange struct {
	Start  int64       `json:"start"`
	End    int64       `json:"end"`
	Range  *epochRange `json:"range"`
	NoBase bool        `json:"noBaseSnapshot"`
}
type inventoryData struct {
	Snapshots []snapshot `json:"snapshots"`
	PITR      struct {
		Ranges []pitrRange `json:"ranges"`
	} `json:"pitr"`
}
type statusData struct {
	Backups struct {
		Snapshot []snapshot `json:"snapshot"`
		Chunks   struct {
			Chunks []pitrRange `json:"pitrChunks"`
		} `json:"pitrChunks"`
	} `json:"backups"`
	Running map[string]any `json:"running"`
}

func inventory(ctx context.Context, env Env) (inventoryData, error) {
	out, err := inventoryCommand(ctx, env, "list")
	if err != nil {
		return inventoryData{}, err
	}
	var inv inventoryData
	if err = decodeLast(out, &inv); err != nil {
		return inv, err
	}
	// During resync PBM emits a message object instead of an inventory. Never treat it as empty storage.
	if inv.Snapshots == nil && !hasJSONKey(out, "snapshots") {
		return inv, errors.New("PBM inventory not available yet (storage resync may be running)")
	}
	return inv, nil
}

func hasJSONKey(data []byte, key string) bool {
	var obj map[string]json.RawMessage
	if decodeLast(data, &obj) != nil {
		return false
	}
	_, ok := obj[key]
	return ok
}

func windows(ranges []pitrRange) []domain.TimeWindow {
	out := make([]domain.TimeWindow, 0, len(ranges))
	for _, r := range ranges {
		if r.Range != nil {
			r.Start, r.End = r.Range.Start, r.Range.End
		}
		if r.NoBase || r.Start <= 0 || r.End < r.Start {
			continue
		}
		out = append(out, domain.TimeWindow{Start: time.Unix(r.Start, 0).UTC(), End: time.Unix(r.End, 0).UTC()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].End.Before(out[j].End) })
	return out
}

// PITRWindows reads continuous, backed restorable intervals and converts epoch seconds to UTC.
func PITRWindows(ctx context.Context, env Env) ([]domain.TimeWindow, error) {
	inv, err := inventory(ctx, env)
	if err != nil {
		return nil, err
	}
	return windows(inv.PITR.Ranges), nil
}

// SyncInventory adopts storage snapshots, updates known records, and marks disappeared completed backups Failed.
func SyncInventory(ctx context.Context, env Env) (domain.BackupStatus, []domain.TimeWindow, error) {
	var summary domain.BackupStatus
	inv, err := inventory(ctx, env)
	if err != nil {
		return summary, nil, err
	}
	data, err := inventoryCommand(ctx, env, "status")
	if err != nil {
		return summary, nil, err
	}
	var status statusData
	if err = decodeLast(data, &status); err != nil {
		return summary, nil, err
	}
	detailed := make(map[string]snapshot, len(status.Backups.Snapshot))
	for _, b := range status.Backups.Snapshot {
		detailed[b.Name] = b
	}
	stored, err := env.RC.Store.ListBackups(ctx, env.RC.Cluster.Ref())
	if err != nil {
		return summary, nil, err
	}
	known := make(map[string]domain.Backup, len(stored))
	for _, b := range stored {
		if b.PBMName != "" {
			known[b.PBMName] = b
		}
	}
	seen := make(map[string]bool, len(inv.Snapshots))
	for _, s := range inv.Snapshots {
		seen[s.Name] = true
		if d, ok := detailed[s.Name]; ok {
			s.Size = d.Size
		}
		b, exists := known[s.Name]
		if !exists {
			b = domain.Backup{Namespace: env.RC.Cluster.Ref().Namespace, Name: s.Name, Cluster: env.RC.Cluster.Name, Storage: env.Storage.Name, PBMName: s.Name, Type: s.Type, Trigger: "manual", CreatedAt: now(env)}
		}
		if b.PBMPrefix == "" {
			b.PBMPrefix, b.SourceTopology, b.SourceVersion = env.Prefix, env.RC.Cluster.Spec.Topology, env.RC.Cluster.Spec.Version
		}
		applySnapshot(&b, s, now(env))
		if exists {
			err = env.RC.Store.UpdateBackup(ctx, &b)
		} else {
			err = env.RC.Store.CreateBackup(ctx, &b)
		}
		if err != nil {
			return summary, nil, fmt.Errorf("persist PBM snapshot %s: %w", s.Name, err)
		}
		if b.Phase == domain.PhaseSucceeded && b.LastWriteTime != nil && (summary.LastBackup == nil || b.LastWriteTime.After(*summary.LastBackup)) {
			t := *b.LastWriteTime
			summary.LastBackup = &t
		}
	}
	for _, b := range stored {
		if b.Storage != env.Storage.Name || b.PBMName == "" || seen[b.PBMName] || b.Phase != domain.PhaseSucceeded {
			continue
		}
		b.Phase, b.Error = domain.PhaseFailed, "snapshot is missing from PBM storage inventory"
		if err := env.RC.Store.UpdateBackup(ctx, &b); err != nil {
			return summary, nil, err
		}
	}
	ranges := windows(inv.PITR.Ranges)
	if len(ranges) == 0 {
		ranges = windows(status.Backups.Chunks.Chunks)
	}
	if len(ranges) > 0 {
		latest := ranges[len(ranges)-1]
		summary.PITRWindow = &latest
	}
	return summary, ranges, nil
}

// setSnapshotLocation exposes the data directory, not the PBM namespace shared
// by other snapshots and PITR slices. Keep deletion keyed by PBMPrefix/PBMName.
func setSnapshotLocation(b *domain.Backup) {
	if b.Location == "" && b.PBMName != "" {
		if prefix := domain.BackupStoragePrefix(b.PBMPrefix); prefix != "" {
			b.Location = prefix + "/" + b.PBMName + "/"
		}
	}
}

func applySnapshot(b *domain.Backup, s snapshot, stamp time.Time) {
	b.PBMName, b.Size = s.Name, s.Size
	setSnapshotLocation(b)
	if s.Type != "" {
		b.Type = s.Type
	}
	if s.Start != nil {
		b.StartedAt = s.Start
	} else if b.StartedAt == nil {
		if t, e := time.Parse(time.RFC3339, s.Name); e == nil {
			b.StartedAt = &t
		}
	}
	b.LastWriteTime = s.LastWriteTime
	ts := s.LastWriteTS
	if ts == 0 {
		ts = s.RestoreTo
	}
	if b.LastWriteTime == nil && ts != 0 {
		t := time.Unix(ts, 0).UTC()
		b.LastWriteTime = &t
	}
	switch s.Status {
	case "done":
		b.Phase, b.Error = domain.PhaseSucceeded, ""
		if s.Finish != nil {
			b.CompletedAt = s.Finish
		} else if b.CompletedAt == nil {
			b.CompletedAt = &stamp
		}
	case "error", "cancelled", "canceled":
		b.Phase = domain.PhaseFailed
		b.Error = redact.NativeError(s.Error)
		if b.Error == "" {
			b.Error = "PBM backup ended with status " + s.Status
		}
		if s.Finish != nil {
			b.CompletedAt = s.Finish
		} else if b.CompletedAt == nil {
			b.CompletedAt = &stamp
		}
	default:
		b.Phase = domain.PhaseRunning
	}
}

func resyncDone(ctx context.Context, env Env, member string) (bool, error) {
	data, err := command(ctx, env, member, "", "status", nil, "--out=json")
	if err != nil {
		return false, err
	}
	var status statusData
	if err = decodeLast(data, &status); err != nil {
		return false, err
	}
	if len(status.Running) != 0 {
		return false, nil
	}
	_, err = inventory(ctx, env)
	if err != nil {
		return false, err
	}
	return true, nil
}
