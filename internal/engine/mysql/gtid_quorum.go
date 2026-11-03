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
var gtidJournalMu = gtidObservationLock{held: make(chan struct{}, 1)}
var gtidObservationLocks sync.Map

type gtidObservationLock struct{ held chan struct{} }

func (m *gtidObservationLock) Lock()   { m.held <- struct{}{} }
func (m *gtidObservationLock) Unlock() { <-m.held }
func (m *gtidObservationLock) TryLock() bool {
	select {
	case m.held <- struct{}{}:
		return true
	default:
		return false
	}
}
func (m *gtidObservationLock) LockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.held <- struct{}{}:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func gtidObservationMutex(rc *engine.Context) *gtidObservationLock {
	key := [2]string{rc.Cluster.Namespace, rc.Cluster.Name}
	if cached, ok := gtidObservationLocks.Load(key); ok {
		return cached.(*gtidObservationLock)
	}
	actual, _ := gtidObservationLocks.LoadOrStore(key, &gtidObservationLock{held: make(chan struct{}, 1)})
	return actual.(*gtidObservationLock)
}

const gtidWindowProofVersion = 2

type gtidJournal struct {
	Incidents          []gtidIncident      `json:"incidents"`
	SafeAt             time.Time           `json:"safeAt"`
	Windows            []domain.TimeWindow `json:"windows,omitempty"`
	Generation         int                 `json:"generation,omitempty"`
	WindowProofVersion int                 `json:"windowProofVersion,omitempty"`
	PrefixLag          *gtidPrefixLag      `json:"prefixLag,omitempty"`
}

func loadGTIDJournal(ctx context.Context, rc *engine.Context, s *state) error {
	if rc.Secrets == nil {
		return engine.ErrUnavailable
	}
	raw, err := secret(ctx, rc, "mysql/gtid/history")
	if missing(err) {
		s.GTIDWindows, s.GTIDWindowGeneration = nil, 0
		s.LastGTIDSafeAt = time.Time{}
		s.GTIDPrefixLag = nil
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
	s.GTIDPrefixLag = journal.PrefixLag
	s.GTIDWindows, s.GTIDWindowGeneration = nil, 0
	if journal.WindowProofVersion == gtidWindowProofVersion {
		s.GTIDWindows, s.GTIDWindowGeneration = journal.Windows, journal.Generation
	}
	return nil
}

func persistGTIDHistory(ctx context.Context, rc *engine.Context, s *state) error {
	raw, err := json.Marshal(gtidJournal{Incidents: s.GTIDIncidents, SafeAt: s.LastGTIDSafeAt, Windows: s.GTIDWindows, Generation: s.GTIDWindowGeneration, WindowProofVersion: gtidWindowProofVersion, PrefixLag: s.GTIDPrefixLag})
	if err != nil {
		return err
	}
	if err = putSecret(ctx, rc, "mysql/gtid/history", raw); err != nil {
		return err
	}
	return save(rc.Cluster, s)
}

func checkPXCQuorumGTIDs(ctx context.Context, rc *engine.Context, s *state) (result error) {
	if rc.Cluster.Spec.Provider != "pxc" {
		return nil
	}
	if !mysqlReadsFresh(ctx) {
		ctx = context.WithValue(ctx, mysqlQuorumReadKey{}, true)
	}
	probe, cancel := mysqlReadContext(ctx, rc)
	defer cancel()
	if !mysqlReadsFresh(ctx) {
		optional, stop := context.WithTimeout(probe, time.Second)
		defer stop()
		probe = optional
	}
	observationMu := gtidObservationMutex(rc)
	if err := observationMu.LockContext(probe); err != nil {
		names := make([]string, 0, len(s.data()))
		outcomes := make(map[string]string, len(s.data()))
		for _, m := range s.data() {
			names = append(names, m.Name)
			outcomes[m.Name] = "no-observation"
		}
		sort.Strings(names)
		return &myspec.GTIDQuorumError{Members: names, Detail: quorumOutcomeDetail(outcomes, "observation-lock")}
	}
	defer observationMu.Unlock()
	outcomes := make(map[string]string, len(s.data()))
	observations, sampleAt := samplePXCQuorumGTIDs(probe, rc, s, mysqlReadsFresh(ctx), outcomes)
	defer func() {
		if result != nil {
			if _, comparison := result.(*myspec.GTIDQuorumError); !comparison {
				result = &gtidHistoryPersistenceError{cause: result, detail: quorumOutcomeDetail(outcomes, "persistence-error")}
			}
		}
	}()
	err := myspec.CheckGTIDQuorum(observations)
	complete := completeGTIDObservation(observations)
	// Sampling may consume its entire reservation. Journal serialization gets a
	// separate bounded wait so timeout evidence and divergence still get recorded.
	journalWait, stopJournalWait := context.WithTimeout(ctx, time.Second)
	defer stopJournalWait()
	if e := gtidJournalMu.LockContext(journalWait); e != nil {
		return fmt.Errorf("GTID history journal lock unavailable: %w", e)
	}
	defer gtidJournalMu.Unlock()
	if e := loadGTIDJournal(ctx, rc, s); e != nil {
		return e
	}
	if n := len(s.GTIDIncidents); n > 0 && !s.GTIDIncidents[n-1].BaselineAt.IsZero() {
		incident := &s.GTIDIncidents[n-1]
		if incident.Allowance != nil && !incident.Allowance.matches(s, observations) {
			incident.Allowance = nil
			if e := persistGTIDHistory(ctx, rc, s); e != nil {
				return e
			}
		}
		if incident.Allowance != nil {
			err = myspec.CheckGTIDQuorumBaseline(observations, incident.Allowance.Histories)
		}
	}
	prefixConflict, prefixSuspected, prefixChanged := observePXCEqualPositionLag(s, observations, sampleAt, err == nil && complete)
	if prefixConflict != nil {
		err = prefixConflict
	}
	if prefixChanged {
		if e := persistGTIDHistory(ctx, rc, s); e != nil {
			return e
		}
	}
	if conflict, ok := err.(*myspec.GTIDQuorumError); ok && conflict.Diverged && activeGTIDIncident(s) == nil {
		incident := gtidIncident{DetectedAt: now(rc), SafeThrough: s.LastGTIDSafeAt, Members: append([]string{}, conflict.Members...)}
		s.GTIDIncidents = append(s.GTIDIncidents, incident)
		if e := persistGTIDHistory(ctx, rc, s); e != nil {
			return e
		}
		invalidatePXCQuorumObservation(ctx, rc)
	}
	if err == nil && complete && activeGTIDIncident(s) == nil {
		if n := len(s.GTIDIncidents); n > 0 && s.GTIDIncidents[n-1].Allowance != nil {
			// The comparison bounded every directed difference by the previous
			// allowance. Replacing it with this complete sample retires healed
			// differences permanently, including after a controller restart.
			s.GTIDIncidents[n-1].Allowance.Histories = observations
		}
		if !prefixSuspected && sampleAt.After(s.LastGTIDSafeAt) {
			s.LastGTIDSafeAt = sampleAt
		}
		if e := persistGTIDHistory(ctx, rc, s); e != nil {
			return e
		}
	}
	// Commit this observation's safety transitions before any recorder call.
	// Older baseline events remain retryable after a new incident is appended.
	if e := auditGTIDBaselines(ctx, rc, s); e != nil {
		return e
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
	if e := save(rc.Cluster, s); e != nil {
		return e
	}
	if unavailable, ok := err.(*myspec.GTIDQuorumError); ok && !unavailable.Diverged {
		class := "comparison"
		if probe.Err() != nil {
			class = "observation-budget"
		}
		unavailable.Detail = quorumOutcomeDetail(outcomes, class)
	}
	return err
}

func observePXCQuorumGTIDs(ctx context.Context, rc *engine.Context, s *state) map[string]myspec.GaleraStatusResponse {
	ctx = freshMySQLReads(ctx)
	probe, cancel := mysqlReadContext(ctx, rc)
	defer cancel()
	observationMu := gtidObservationMutex(rc)
	if err := observationMu.LockContext(probe); err != nil {
		return nil
	}
	defer observationMu.Unlock()
	observations, _ := samplePXCQuorumGTIDs(probe, rc, s, true, nil)
	return observations
}

func invalidatePXCQuorumObservation(ctx context.Context, rc *engine.Context) {
	invalidateMySQLReadObservation(ctx, rc)
}

func completeGTIDObservation(observations map[string]myspec.GaleraStatusResponse) bool {
	if len(observations) == 0 {
		return false
	}
	stateUUID := ""
	for _, status := range observations {
		if !status.Eligible() || !myspec.ValidUUID(status.StateUUID) || status.ClusterSize != len(observations) || stateUUID != "" && status.StateUUID != stateUUID {
			return false
		}
		if _, err := myspec.GTIDContains(status.GTIDExecuted, status.GTIDExecuted); err != nil {
			return false
		}
		stateUUID = status.StateUUID
	}
	return true
}

func gtidMemberIdentities(s *state, observations map[string]myspec.GaleraStatusResponse) map[string]gtidMemberIdentity {
	members := s.data()
	identities := make(map[string]gtidMemberIdentity, len(members))
	for _, m := range members {
		identities[m.Name] = gtidMemberIdentity{ServerUUID: m.ServerUUID, ServiceID: m.ServiceID, VolumeID: m.VolumeID, NativeUUID: observations[m.Name].ServerUUID}
	}
	return identities
}

func (a *gtidAllowance) matches(s *state, observations map[string]myspec.GaleraStatusResponse) bool {
	if a.Epoch != s.Formation.FormationEpoch || a.Group != s.Formation.GroupName || len(a.Members) != len(s.data()) {
		return false
	}
	for _, m := range s.data() {
		identity, ok := a.Members[m.Name]
		if !ok || identity.ServiceID == "" || identity.ServerUUID != m.ServerUUID || identity.ServiceID != m.ServiceID || identity.VolumeID != m.VolumeID {
			return false
		}
		if status := observations[m.Name]; status.Eligible() && (!myspec.ValidUUID(status.ServerUUID) || identity.NativeUUID != status.ServerUUID) {
			return false
		}
	}
	return true
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
	probe, cancel := mysqlReadContext(freshMySQLReads(ctx), rc)
	defer cancel()
	observationMu := gtidObservationMutex(rc)
	if err := observationMu.LockContext(probe); err != nil {
		return err
	}
	defer observationMu.Unlock()
	// Ordinary successful fulls with no incident need no agent sweep. Inspect
	// the journal briefly, then release the shared lock before any network I/O.
	gtidJournalMu.Lock()
	if err := loadGTIDJournal(ctx, rc, s); err != nil {
		gtidJournalMu.Unlock()
		return err
	}
	pending := activeGTIDIncident(s)
	if pending == nil {
		err := auditGTIDBaselines(ctx, rc, s)
		gtidJournalMu.Unlock()
		return err
	}
	eligible := b.Type == "full" && b.Phase == domain.PhaseSucceeded && b.MySQL != nil && b.StartedAt != nil && b.StartedAt.After(pending.DetectedAt)
	gtidJournalMu.Unlock()
	if !eligible {
		return nil
	}
	// A history acknowledgement is not a repair of native SID assignment.
	// Retired post-admission owners retain probe evidence even after releasing
	// unauthorized client holds; only a later lifecycle certificate supersedes it.
	if pxcUnrepairedMixedMapping(s) {
		return engine.Invalid("PXC members have an observed mixed GTID SID mapping; replace from trusted history or restore before acknowledging with a fresh full backup")
	}
	// Legacy restore does not certify a replacement history. Its automatic
	// post-restore full cannot silently acknowledge an existing incident.
	if s.RestoreGTIDUnavailable != "" && b.Trigger == "restore" {
		return engine.ErrUnavailable
	}
	// Acknowledge complete member histories, never just the backup source.
	observations, _ := samplePXCQuorumGTIDs(ctx, rc, s, true, nil)
	gtidJournalMu.Lock()
	defer gtidJournalMu.Unlock()
	if err := loadGTIDJournal(ctx, rc, s); err != nil {
		return err
	}
	incident := activeGTIDIncident(s)
	if incident == nil {
		return auditGTIDBaselines(ctx, rc, s)
	}
	if b.Type != "full" || b.Phase != domain.PhaseSucceeded || b.MySQL == nil || b.StartedAt == nil || !b.StartedAt.After(incident.DetectedAt) {
		return nil
	}
	if !completeGTIDObservation(observations) {
		return engine.ErrUnavailable
	}
	for _, status := range observations {
		if !myspec.ValidUUID(status.ServerUUID) {
			return engine.ErrUnavailable
		}
	}
	if !myspec.PXCCurrentSIDMappingCompatible(observations) {
		return engine.Invalid("PXC current direct/inverse GTID accounting is incompatible or unproven; preserve history and replace from a trusted donor or restore before full acknowledgement")
	}
	incident.BaselineAt, incident.Backup = b.MySQL.EndTime, b.Name
	incident.Acknowledged = observations
	s.GTIDPrefixLag = nil
	incident.Allowance = &gtidAllowance{Epoch: s.Formation.FormationEpoch, Group: s.Formation.GroupName, Members: gtidMemberIdentities(s, observations), Histories: observations}
	incident.BaselineAuditPending, incident.BaselineOperation = true, b.OperationID
	// The transition must be durable before its event claims a new baseline.
	if err := persistGTIDHistory(ctx, rc, s); err != nil {
		return err
	}
	invalidatePXCQuorumObservation(ctx, rc)
	return auditGTIDBaselines(ctx, rc, s)
}

func auditGTIDBaseline(ctx context.Context, rc *engine.Context, s *state, incident *gtidIncident) error {
	if !incident.BaselineAuditPending || incident.BaselineAt.IsZero() || rc.Events == nil {
		return nil
	}
	if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Operation: incident.BaselineOperation, Type: "Normal", Reason: "GTIDHistoryBaselineEstablished", Message: fmt.Sprintf("Successful full backup %s establishes a new GTID/PITR baseline after discontinuity at %s; older chains remain bounded by that point", incident.Backup, incident.DetectedAt.UTC().Format(time.RFC3339Nano)), CreatedAt: now(rc)}); err != nil {
		return err
	}
	incident.BaselineAuditPending = false
	return persistGTIDHistory(ctx, rc, s)
}

func auditGTIDBaselines(ctx context.Context, rc *engine.Context, s *state) error {
	for i := range s.GTIDIncidents {
		if err := auditGTIDBaseline(ctx, rc, s, &s.GTIDIncidents[i]); err != nil {
			return err
		}
	}
	return nil
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

func quorumOutcomeDetail(outcomes map[string]string, class string) string {
	names := make([]string, 0, len(outcomes))
	for name := range outcomes {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+outcomes[name])
	}
	return fmt.Sprintf(" (%s; %s)", class, strings.Join(parts, "; "))
}

type gtidHistoryPersistenceError struct {
	cause  error
	detail string
}

func (e *gtidHistoryPersistenceError) Error() string {
	return "PXC GTID history journal, state or audit persistence unavailable" + e.detail + "; continuity cannot be certified until persistence recovers"
}

func (e *gtidHistoryPersistenceError) Unwrap() error { return e.cause }
