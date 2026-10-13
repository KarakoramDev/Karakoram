// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"sort"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type orphanedVolumeRecord struct {
	domain.OrphanedVolume
	ProcessPending  bool `json:"processPending"`
	JanitorFailures int  `json:"janitorFailures"`
}

func orphanRecord(v *domain.OrphanedVolume) orphanedVolumeRecord {
	return orphanedVolumeRecord{OrphanedVolume: *v, ProcessPending: v.ProcessPending, JanitorFailures: v.JanitorFailures}
}

func (v orphanedVolumeRecord) volume() domain.OrphanedVolume {
	out := v.OrphanedVolume
	out.ProcessPending, out.JanitorFailures = v.ProcessPending, v.JanitorFailures
	return out
}

// PutOrphanedVolume inserts or updates a globally hostname-keyed orphan record.
// Global scope deliberately keeps records independent of cluster deletion.
func (s *Store) PutOrphanedVolume(ctx context.Context, v *domain.OrphanedVolume) error {
	return put(ctx, s.writer, "orphaned-volume", "", v.Hostname, orphanRecord(v), "", "", "", false)
}

// GetOrphanedVolume loads the record for a platform volume hostname.
func (s *Store) GetOrphanedVolume(ctx context.Context, hostname string) (*domain.OrphanedVolume, error) {
	var v orphanedVolumeRecord
	if _, _, err := s.get(ctx, "orphaned-volume", "", hostname, &v); err != nil {
		return nil, err
	}
	out := v.volume()
	return &out, nil
}

// ListOrphanedVolumes returns global records ordered by hostname.
func (s *Store) ListOrphanedVolumes(ctx context.Context) ([]domain.OrphanedVolume, error) {
	records, err := list[orphanedVolumeRecord](ctx, s, "orphaned-volume", "")
	items := make([]domain.OrphanedVolume, 0, len(records))
	for _, record := range records {
		items = append(items, record.volume())
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Hostname < items[j].Hostname })
	return items, err
}

// DeleteOrphanedVolume removes only the orphan inventory record.
func (s *Store) DeleteOrphanedVolume(ctx context.Context, hostname string) error {
	return s.remove(ctx, "orphaned-volume", "", hostname)
}
