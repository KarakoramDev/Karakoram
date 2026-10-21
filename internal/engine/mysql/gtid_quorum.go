// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Serialize each cluster's observation and journal commit together: a newer
// equal sample must not move the boundary past an older in-flight conflict.
// Unrelated clusters only share the short sealed-journal read/write lock.
var gtidJournalMu sync.Mutex
var gtidObservationLocks sync.Map

func gtidObservationMutex(rc *engine.Context) *sync.Mutex {
	key := [2]string{rc.Cluster.Namespace, rc.Cluster.Name}
	if cached, ok := gtidObservationLocks.Load(key); ok {
		return cached.(*sync.Mutex)
	}
	actual, _ := gtidObservationLocks.LoadOrStore(key, new(sync.Mutex))
	return actual.(*sync.Mutex)
}

const gtidWindowProofVersion = 2

type gtidJournal struct {
	Incidents          []gtidIncident      `json:"incidents"`
	SafeAt             time.Time           `json:"safeAt"`
	Windows            []domain.TimeWindow `json:"windows,omitempty"`
	Generation         int                 `json:"generation,omitempty"`
	WindowProofVersion int                 `json:"windowProofVersion,omitempty"`
}

func loadGTIDJournal(ctx context.Context, rc *engine.Context, s *state) error {
	if rc.Secrets == nil {
		return engine.ErrUnavailable
	}
	raw, err := secret(ctx, rc, "mysql/gtid/history")
	if missing(err) {
		s.GTIDWindows, s.GTIDWindowGeneration = nil, 0
		s.LastGTIDSafeAt = time.Time{}
		return nil
	}
	if err != nil {
		return err
	}
	var journal gtidJournal
	if err = json.Unmarshal(raw, &journal); err != nil {
		return err
	}
	s.GTIDIncidents, s.LastGTIDSafeAt = journal.Incidents, journal.SafeAt
	s.GTIDWindows, s.GTIDWindowGeneration = nil, 0
	if journal.WindowProofVersion == gtidWindowProofVersion {
		s.GTIDWindows, s.GTIDWindowGeneration = journal.Windows, journal.Generation
	}
	return nil
}

func persistGTIDHistory(ctx context.Context, rc *engine.Context, s *state) error {
	raw, err := json.Marshal(gtidJournal{Incidents: s.GTIDIncidents, SafeAt: s.LastGTIDSafeAt, Windows: s.GTIDWindows, Generation: s.GTIDWindowGeneration, WindowProofVersion: gtidWindowProofVersion})
	if err != nil {
		return err
	}
	if err = putSecret(ctx, rc, "mysql/gtid/history", raw); err != nil {
		return err
	}
	return save(rc.Cluster, s)
}

func checkPXCQuorumGTIDs(ctx context.Context, rc *engine.Context, s *state) error {
	if rc.Cluster.Spec.Provider != "pxc" {
		return nil
	}
	observationMu := gtidObservationMutex(rc)
	observationMu.Lock()
	defer observationMu.Unlock()
	members := s.data()
	observations := make(map[string]myspec.GaleraStatusResponse, len(members))
	sampleAt := now(rc)
	for _, m := range members {
		if rc.Agents == nil {
			observations[m.Name] = myspec.GaleraStatusResponse{}
			continue
		}
		status, err := galeraStatus(ctx, rc, m.Name)
		if err != nil {
			status = myspec.GaleraStatusResponse{}
		}
		observations[m.Name] = status
	}
	err := myspec.CheckGTIDQuorum(observations)
	complete := len(observations) > 0
	stateUUID := ""
	for _, status := range observations {
		if !status.Eligible() || !myspec.ValidUUID(status.StateUUID) || status.ClusterSize != len(observations) || stateUUID != "" && status.StateUUID != stateUUID {
			complete = false
			break
		}
		stateUUID = status.StateUUID
	}
	gtidJournalMu.Lock()
	defer gtidJournalMu.Unlock()
	if e := loadGTIDJournal(ctx, rc, s); e != nil {
		return e
	}
	if conflict, ok := err.(*myspec.GTIDQuorumError); ok && conflict.Diverged && activeGTIDIncident(s) == nil {
		incident := gtidIncident{DetectedAt: now(rc), SafeThrough: s.LastGTIDSafeAt, Members: append([]string{}, conflict.Members...)}
		s.GTIDIncidents = append(s.GTIDIncidents, incident)
		if e := persistGTIDHistory(ctx, rc, s); e != nil {
			return e
		}
	}
	// The safety latch is durable before recording its audit event. A recorder
	// outage must not erase evidence; the unrecorded transition retries later.
	if incident := activeGTIDIncident(s); incident != nil && !incident.Audited && rc.Events != nil {
		if e := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "GTIDHistoryDiverged", Message: incident.message(), CreatedAt: now(rc)}); e != nil {
			return e
		}
		incident.Audited = true
		if e := persistGTIDHistory(ctx, rc, s); e != nil {
			return e
		}
	}
	if err == nil && complete && activeGTIDIncident(s) == nil {
		if sampleAt.After(s.LastGTIDSafeAt) {
			s.LastGTIDSafeAt = sampleAt
		}
		if e := persistGTIDHistory(ctx, rc, s); e != nil {
			return e
		}
	}
	if e := save(rc.Cluster, s); e != nil {
		return e
	}
	return err
}

// ValidateBackup performs fresh admission independently of cached cluster phase.
func (p *Core) ValidateBackup(ctx context.Context, rc *engine.Context, backupType string) error {
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	err = checkPXCQuorumGTIDs(ctx, rc, s)
	if err != nil {
		if _, observation := err.(*myspec.GTIDQuorumError); !observation {
			return err
		}
	}
	if backupType == "full" {
		return nil
	}
	if incident := activeGTIDIncident(s); incident != nil {
		err = fmt.Errorf("%s", incident.message())
	}
	if err != nil {
		return domain.Conflict("cluster", rc.Cluster.Name, domain.ReasonInvalidState, engine.ErrConflict, "%s", err.Error())
	}
	return nil
}

func activeGTIDIncident(s *state) *gtidIncident {
	if n := len(s.GTIDIncidents); n > 0 && s.GTIDIncidents[n-1].BaselineAt.IsZero() {
		return &s.GTIDIncidents[n-1]
	}
	return nil
}

func (i gtidIncident) message() string {
	return fmt.Sprintf("PXC GTID discontinuity detected at %s (last verified safe through %s) affecting members %s; GTID-continuity operations beyond the safe watermark are blocked. Preserve evidence and take a new full backup to establish a fresh PITR baseline; never reset or inject GTIDs.", i.DetectedAt.UTC().Format(time.RFC3339Nano), i.SafeThrough.UTC().Format(time.RFC3339Nano), strings.Join(i.Members, ", "))
}

func gtidContinuitySafe(s *state, base, target time.Time) bool {
	for _, incident := range s.GTIDIncidents {
		if (incident.SafeThrough.IsZero() || target.After(incident.SafeThrough)) && (incident.BaselineAt.IsZero() || base.Before(incident.BaselineAt)) {
			return false
		}
	}
	return true
}

func establishGTIDBaseline(ctx context.Context, rc *engine.Context, s *state, b *domain.Backup) error {
	if rc.Cluster.Spec.Provider != "pxc" {
		return nil
	}
	observationMu := gtidObservationMutex(rc)
	observationMu.Lock()
	defer observationMu.Unlock()
	gtidJournalMu.Lock()
	defer gtidJournalMu.Unlock()
	if err := loadGTIDJournal(ctx, rc, s); err != nil {
		return err
	}
	incident := activeGTIDIncident(s)
	if incident == nil || b.Type != "full" || b.Phase != domain.PhaseSucceeded || b.MySQL == nil || b.StartedAt == nil || !b.StartedAt.After(incident.DetectedAt) {
		return nil
	}
	if rc.Events != nil {
		if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Operation: b.OperationID, Type: "Normal", Reason: "GTIDHistoryBaselineEstablished", Message: fmt.Sprintf("Successful full backup %s establishes a new GTID/PITR baseline after discontinuity at %s; older chains remain bounded by that point", b.Name, incident.DetectedAt.UTC().Format(time.RFC3339Nano)), CreatedAt: now(rc)}); err != nil {
			return err
		}
	}
	incident.BaselineAt, incident.Backup = b.MySQL.EndTime, b.Name
	return persistGTIDHistory(ctx, rc, s)
}

func gtidWindowEnd(s *state, base, end time.Time) time.Time {
	for _, incident := range s.GTIDIncidents {
		if incident.BaselineAt.IsZero() || base.Before(incident.BaselineAt) {
			if incident.SafeThrough.IsZero() {
				return time.Time{}
			}
			if end.After(incident.SafeThrough) {
				end = incident.SafeThrough
			}
		}
	}
	return end
}

func backupGTIDContinuitySafe(s *state, items []domain.Backup, b domain.Backup, target time.Time) bool {
	if b.MySQL == nil {
		return false
	}
	for _, root := range items {
		if root.MySQL != nil && root.MySQL.Label == b.MySQL.ChainRootLabel && root.MySQL.Repository == b.MySQL.Repository {
			return gtidContinuitySafe(s, root.MySQL.EndTime, target)
		}
	}
	return len(s.GTIDIncidents) == 0
}

func mergeGTIDWindows(windows []domain.TimeWindow) []domain.TimeWindow {
	sort.Slice(windows, func(i, j int) bool { return windows[i].Start.Before(windows[j].Start) })
	merged := windows[:0]
	for _, w := range windows {
		if len(merged) > 0 && !w.Start.After(merged[len(merged)-1].End) {
			if w.End.After(merged[len(merged)-1].End) {
				merged[len(merged)-1].End = w.End
			}
		} else {
			merged = append(merged, w)
		}
	}
	return merged
}

func rememberedGTIDWindows(s *state) []domain.TimeWindow {
	windows := make([]domain.TimeWindow, 0, len(s.GTIDWindows))
	for _, w := range s.GTIDWindows {
		w.End = gtidWindowEnd(s, w.Start, w.End)
		if !w.End.IsZero() && !w.End.Before(w.Start) {
			windows = append(windows, w)
		}
	}
	return mergeGTIDWindows(windows)
}

func rememberGTIDWindows(ctx context.Context, rc *engine.Context, s *state, windows []domain.TimeWindow) error {
	gtidJournalMu.Lock()
	defer gtidJournalMu.Unlock()
	if err := loadGTIDJournal(ctx, rc, s); err != nil {
		return err
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return err
	}
	if s.GTIDWindowGeneration != bs.Generation {
		s.GTIDWindows = nil
	}
	s.GTIDWindowGeneration = bs.Generation
	for _, w := range windows {
		if w.End.After(s.LastGTIDSafeAt) {
			w.End = s.LastGTIDSafeAt
		}
		if !w.End.IsZero() && !w.End.Before(w.Start) {
			s.GTIDWindows = append(s.GTIDWindows, w)
		}
	}
	s.GTIDWindows = mergeGTIDWindows(s.GTIDWindows)
	s.GTIDWindows = rememberedGTIDWindows(s)
	return persistGTIDHistory(ctx, rc, s)
}
