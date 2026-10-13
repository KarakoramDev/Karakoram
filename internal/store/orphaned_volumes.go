// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type orphanedVolumeRecord struct {
	domain.OrphanedVolume
	ProcessPending  bool `json:"processPending"`
	JanitorFailures int  `json:"janitorFailures"`
}

func (v *orphanedVolumeRecord) UnmarshalJSON(data []byte) error {
	type volumeJSON domain.OrphanedVolume
	var wire struct {
		volumeJSON
		ProcessPending  bool `json:"processPending"`
		JanitorFailures int  `json:"janitorFailures"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	v.OrphanedVolume = domain.OrphanedVolume(wire.volumeJSON)
	v.Namespace = domain.NormalizedNamespace(v.Namespace)
	v.ProcessPending, v.JanitorFailures = wire.ProcessPending, wire.JanitorFailures
	return nil
}

func orphanRecord(v *domain.OrphanedVolume) orphanedVolumeRecord {
	return orphanedVolumeRecord{OrphanedVolume: *v, ProcessPending: v.ProcessPending, JanitorFailures: v.JanitorFailures}
}

func (v orphanedVolumeRecord) volume() domain.OrphanedVolume {
	out := v.OrphanedVolume
	out.ProcessPending, out.JanitorFailures = v.ProcessPending, v.JanitorFailures
	return out
}

// PutOrphanedVolume keeps orphan inventory independent of cluster deletion.
func (s *Store) PutOrphanedVolume(ctx context.Context, v *domain.OrphanedVolume) error {
	v.Namespace = domain.NormalizedNamespace(v.Namespace)
	return put(ctx, s.writer, "orphaned-volume", namespaceScope(v.Namespace), v.Hostname, orphanRecord(v), "", "", "", false)
}

// GetOrphanedVolume loads the record for a platform volume hostname.
func (s *Store) GetOrphanedVolume(ctx context.Context, namespace, hostname string) (*domain.OrphanedVolume, error) {
	var v orphanedVolumeRecord
	if _, _, err := s.get(ctx, "orphaned-volume", namespaceScope(namespace), hostname, &v); err != nil {
		return nil, err
	}
	out := v.volume()
	return &out, nil
}

// ListOrphanedVolumes returns records ordered by namespace and hostname.
func (s *Store) ListOrphanedVolumes(ctx context.Context, namespace string) ([]domain.OrphanedVolume, error) {
	scope := "*"
	if namespace != "" {
		scope = namespaceScope(namespace)
	}
	records, err := list[orphanedVolumeRecord](ctx, s, "orphaned-volume", scope)
	items := make([]domain.OrphanedVolume, 0, len(records))
	for _, record := range records {
		items = append(items, record.volume())
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Namespace != items[j].Namespace {
			return items[i].Namespace < items[j].Namespace
		}
		return items[i].Hostname < items[j].Hostname
	})
	return items, err
}

// DeleteOrphanedVolume removes only the orphan inventory record.
func (s *Store) DeleteOrphanedVolume(ctx context.Context, namespace, hostname string) error {
	return s.remove(ctx, "orphaned-volume", namespaceScope(namespace), hostname)
}
