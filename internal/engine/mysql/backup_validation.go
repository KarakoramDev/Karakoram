// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/robfig/cron/v3"
)

// IncidentBackupReady admits an explicit full while a history latch prevents
// a desired-state tracking operation from becoming healthy. It does not certify
// history or complete that operation: the ordinary backup claim policy excludes
// active native mutations, and only the post-success sample can acknowledge.
func (p *Core) IncidentBackupReady(ctx context.Context, c *domain.Cluster, secrets engine.SecretStore, kind string) bool {
	if kind != "full" || p.Name() != "pxc" || c.Spec.Provider != p.Name() || c.Deleting || c.Status.Phase != domain.PhaseDegraded || secrets == nil {
		return false
	}
	allowed, err := p.CanClaimOperation(c)
	if err != nil || !allowed {
		return false
	}
	s, err := load(c)
	if err != nil || s.Formation.reforming() || s.Rolling != nil || s.NativeConvergedGeneration != c.Generation {
		return false
	}
	rc := &engine.Context{Cluster: c, Secrets: secrets}
	if err := loadGTIDJournal(ctx, rc, s); err != nil {
		return false
	}
	return activeGTIDIncident(s) != nil
}

func normalizedDataSource(d *domain.DataSource, ns string) *domain.DataSource {
	if d == nil {
		return nil
	}
	copy := *d
	if copy.Namespace == "" {
		copy.Namespace = domain.NormalizedNamespace(ns)
	}
	return &copy
}
func (p *Core) validateBackups(ctx context.Context, old, n *domain.Cluster) error {
	policy := n.Spec.Backup
	if policy.PITR.Enabled && policy.Storage == "" {
		return engine.Invalid("PITR requires backup storage")
	}
	if policy.PITR.OplogSpanMinutes < 0 || policy.PITR.OplogSpanMinutes > 1440 {
		return engine.Invalid("PITR oplogSpanMinutes must be between 1 and 1440, or omitted")
	}
	storages := map[string]bool{}
	if policy.Storage != "" {
		storages[policy.Storage] = true
	}
	names := map[string]bool{}
	for i, s := range policy.Schedules {
		if s.Name == "" || names[s.Name] {
			return engine.Invalid("backup schedule names must be nonempty and unique")
		}
		names[s.Name] = true
		if _, err := cron.ParseStandard(s.Cron); err != nil {
			return engine.Invalid("invalid backup schedule %q", s.Name)
		}
		if s.Type != "full" && s.Type != "incremental" {
			return engine.Invalid("MySQL backup schedule type must be full or incremental")
		}
		if s.RetainCount < 1 {
			return engine.InvalidField(fmt.Sprintf("spec.backup.schedules.%d.retainCount", i), domain.ReasonOutOfRange, "backup retainCount must be at least 1")
		}
		storage := s.Storage
		if storage == "" {
			storage = policy.Storage
		}
		if storage == "" {
			return engine.Invalid("backup schedule requires storage")
		}
		storages[storage] = true
	}
	for name := range storages {
		if p.options.Store == nil {
			return engine.Invalid("backup storage validation requires a configured store")
		}
		if _, err := p.options.Store.GetBackupStorage(ctx, n.Ref().Namespace, name); err != nil {
			return engine.Invalid("backup storage %q is unavailable", name)
		}
	}
	if old != nil {
		before, _ := json.Marshal(normalizedDataSource(old.Spec.DataSource, old.Namespace))
		after, _ := json.Marshal(normalizedDataSource(n.Spec.DataSource, n.Namespace))
		if string(before) != string(after) {
			return engine.Invalid("dataSource is immutable")
		}
		return nil
	}
	d := normalizedDataSource(n.Spec.DataSource, n.Namespace)
	if d == nil {
		return nil
	}
	if policy.Storage == "" {
		return engine.Invalid("dataSource requires independent target-local backup storage")
	}
	ref := domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}
	if ref.Validate() != nil || ref == n.Ref() || (d.Backup == "") == (d.PITRTime == nil) {
		return engine.Invalid("dataSource requires another cluster and exactly one of backup or pitrTime")
	}
	if p.options.Store == nil {
		return engine.ErrUnavailable
	}
	if d.Backup != "" {
		b, err := p.options.Store.GetBackup(ctx, ref, d.Backup)
		if err != nil || b.Phase != domain.PhaseSucceeded || b.MySQL == nil {
			return engine.Invalid("dataSource backup requires succeeded XtraBackup provenance")
		}
		if !b.ProviderMatches(n.Spec.Provider) || major(b.SourceVersion) != major(n.Spec.Version) {
			return engine.Invalid("dataSource provider and MySQL major must match")
		}
	} else {
		source, err := p.options.Store.GetCluster(ctx, ref)
		if err != nil {
			return engine.Invalid("dataSource cluster is unavailable")
		}
		if source.Spec.Provider != n.Spec.Provider || major(source.Spec.Version) != major(n.Spec.Version) {
			return engine.Invalid("dataSource provider and MySQL major must match")
		}
		if !source.Spec.Backup.PITR.Enabled {
			return engine.Invalid("dataSource PITR requires PITR enabled on the source")
		}
		// Baseline-specific archived provenance is checked by the server's
		// source validator and again at execution. A cached status window
		// cannot represent disjoint safe ranges or prove current coverage.
	}
	return nil
}
func archiveWriter(ctx context.Context, rc *engine.Context, s *state) (string, string, error) {
	for _, m := range s.data() {
		// Archive ownership requires an admitted data member, not a planned
		// hostname. PITR observation must not spend the sweep on absent DNS.
		if m.ServiceID == "" || m.Stage != "joined" || m.Removing {
			continue
		}
		st, err := live(ctx, rc, m.Name)
		if err == nil && st.MemberState == "ONLINE" && (s.Topology == "single" || onlinePrimary(st)) {
			return m.Name, st.ServerUUID, nil
		}
	}
	return "", "", engine.ErrUnavailable
}

func (p *Core) PITRWindow(ctx context.Context, rc *engine.Context) ([]domain.TimeWindow, error) {
	windows, _, err := p.pitrWindowsByFull(ctx, rc)
	return windows, err
}

// PITRWindowIntervals retains per-full provenance without changing the merged
// coverage used for legacy responses, admission, and cluster status.
func (p *Core) PITRWindowIntervals(ctx context.Context, rc *engine.Context) ([]domain.TimeWindow, []domain.TimeWindow, error) {
	windows, byFull, err := p.pitrWindowsByFull(ctx, rc)
	if err != nil || byFull == nil {
		return windows, windows, err
	}
	return windows, fullPITRIntervals(windows, byFull), nil
}

func fullPITRIntervals(windows []domain.TimeWindow, byFull map[string][]domain.TimeWindow) []domain.TimeWindow {
	var intervals, live []domain.TimeWindow
	for backup, coverage := range byFull {
		var bounded []domain.TimeWindow
		for _, window := range coverage {
			bounded = append(bounded, intersectCoverageWindow(window, windows)...)
		}
		for _, window := range mergeGTIDWindows(bounded) {
			live = append(live, window)
			window.Backup = backup
			intervals = append(intervals, window)
		}
	}
	// Remembered PXC coverage can outlive an individual live observation.
	// Preserve its uncovered portions without inventing full-backup provenance.
	live = mergeGTIDWindows(live)
	for _, window := range windows {
		cursor := window.Start
		covered := false
		for _, current := range live {
			if current.End.Before(cursor) || current.Start.After(window.End) {
				continue
			}
			if current.Start.After(cursor) {
				intervals = append(intervals, domain.TimeWindow{Start: cursor, End: current.Start})
			}
			if !current.End.Before(window.End) {
				covered = true
				break
			}
			cursor = current.End
		}
		if !covered {
			intervals = append(intervals, domain.TimeWindow{Start: cursor, End: window.End})
		}
	}
	return intervals
}

// pitrWindowsByFull returns nil lineage coverage when only remembered windows
// are available. A non-nil map contains the live, GTID-bounded coverage of each
// full backup, keyed by backup name.
func (p *Core) pitrWindowsByFull(ctx context.Context, rc *engine.Context) ([]domain.TimeWindow, map[string][]domain.TimeWindow, error) {
	return p.observePITRWindowsByFull(ctx, rc, nil, false)
}

// pitrWindowObservation describes only the newest live window. A historical
// baseline's foreign uploads must not override coverage proven by a newer full.
type pitrWindowObservation struct {
	end           time.Time
	writerChanged bool
	blockedReason string
	pending       error
}

type pitrCoveragePending struct{ detail *domain.ResourceError }

func (e *pitrCoveragePending) Error() string { return e.detail.Error() }
func (e *pitrCoveragePending) Unwrap() error { return e.detail }

func (p *Core) observePITRWindowsByFull(ctx context.Context, rc *engine.Context, observation *pitrWindowObservation, requireFresh bool) ([]domain.TimeWindow, map[string][]domain.TimeWindow, error) {
	if !rc.Cluster.Spec.Backup.PITR.Enabled || rc.Cluster.Spec.Backup.Storage == "" {
		return nil, nil, nil
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return nil, nil, err
	}
	observationErr := checkPXCQuorumGTIDs(ctx, rc, s)
	if observationErr != nil {
		if _, observation := observationErr.(*myspec.GTIDQuorumError); !observation {
			return nil, nil, observationErr
		}
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return nil, nil, err
	}
	if s.GTIDWindowGeneration != bs.Generation {
		s.GTIDWindows = nil
	}
	var frozenCoverage []domain.TimeWindow
	if incident := activeGTIDIncident(s); incident != nil || observationErr != nil {
		known := rememberedGTIDWindows(s)
		if len(known) > 0 {
			if !requireFresh {
				return known, nil, nil
			}
			// Archived proof may freshly validate historical data, but cannot
			// extend the already established safe GTID/time boundaries.
			frozenCoverage = known
		} else {
			if incident != nil {
				return nil, nil, engine.Invalid("%s", incident.message())
			}
			return nil, nil, observationErr
		}
	}
	storage, err := p.backupStorage(ctx, rc, "", rc.Cluster.Ref().Namespace)
	if err != nil {
		return nil, nil, err
	}
	writer, writerUUID, err := archiveWriter(ctx, rc, s)
	var observers []string
	if err == nil {
		observers = []string{writer}
	} else {
		if known := rememberedGTIDWindows(s); rc.Cluster.Spec.Provider == "pxc" && len(known) > 0 {
			if !requireFresh {
				return known, nil, nil
			}
			frozenCoverage = known
		}
		if (rc.Cluster.Spec.Provider != "mysql" && !(requireFresh && len(frozenCoverage) > 0)) || rc.Agents == nil {
			return nil, nil, err
		}
		// Coverage reads the sealed baseline and archive index from object
		// storage; it needs an agent, not a running source mysqld. Retain
		// baseline-specific proof even when no ONLINE writer can be found.
		for _, m := range s.data() {
			if m.ServiceID != "" && m.Stage == "joined" && !m.Removing {
				observers = append(observers, m.Name)
			}
		}
		if len(observers) == 0 {
			return nil, nil, err
		}
	}
	items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return nil, nil, err
	}
	var windows []domain.TimeWindow
	byFull := make(map[string][]domain.TimeWindow)
	var coverageErr error
	var pendingErr error
	for _, b := range items {
		m := b.MySQL
		if b.Phase != domain.PhaseSucceeded || m == nil || m.Type != "full" || b.Storage != storage.Name || m.Repository != repoPrefix(storage, rc.Cluster, s, bs.Generation) || m.ClusterUID != s.UID {
			continue
		}
		repo, e := repository(storage, m.Repository)
		if e != nil {
			return nil, nil, e
		}
		refs, e := backupManifestRefs(ctx, rc, items, m.ObjectPaths)
		if e != nil {
			coverageErr = e
			continue
		}
		var coverage myspec.PITRCoverageResponse
		for _, observer := range observers {
			coverage = myspec.PITRCoverageResponse{}
			e = mysqlAction(ctx, rc, observer, myspec.ActionPITRCoverage, myspec.PITRCoverageRequest{Source: repo, ObjectPaths: m.ObjectPaths, ObjectManifests: refs, RequireFresh: requireFresh, AcceptPending: true}, &coverage)
			if e == nil {
				break
			}
		}
		if e != nil {
			coverageErr = e
			if writer == "" {
				coverageErr = engine.ErrUnavailable
			}
			continue
		}
		if coverage.Pending {
			progress := myspec.PITRCoverageProgress{Stage: "queued"}
			if coverage.Progress != nil {
				progress = *coverage.Progress
			}
			message := fmt.Sprintf("PITR coverage verification pending: %s (%d/%d sidecars)", progress.Stage, progress.Processed, progress.Total)
			if pendingErr == nil {
				pendingErr = &pitrCoveragePending{detail: domain.Conflict("cluster", rc.Cluster.Name, domain.ReasonInvalidState, engine.ErrUnavailable, "%s", message)}
			}
			continue
		}
		// Legacy agents compute synchronously inside this request. Only the
		// background protocol can return a cached proof and must attest freshness.
		// The agent compares verification start with this request's arrival on
		// its own clock; comparing its timestamp with the control plane's clock
		// would reject fresh proofs whenever the agent clock lags behind.
		if requireFresh && !coverage.VerificationStartedAt.IsZero() && !coverage.Fresh {
			return nil, nil, engine.Invalid("agent returned PITR verification predating restore admission")
		}
		for _, w := range coverage.Windows {
			if w.Start.Before(m.EndTime) || w.End.Before(w.Start) {
				return nil, nil, engine.Invalid("agent returned invalid baseline-specific PITR coverage")
			}
			boundedEnd := gtidWindowEnd(s, m.EndTime, w.End)
			if !boundedEnd.IsZero() && !boundedEnd.Before(w.Start) {
				bounds := []domain.TimeWindow{{Start: w.Start, End: boundedEnd}}
				if len(frozenCoverage) > 0 {
					bounds = intersectCoverageWindow(bounds[0], frozenCoverage)
				}
				for _, bounded := range bounds {
					windows = append(windows, bounded)
					byFull[b.Name] = append(byFull[b.Name], bounded)
					if observation != nil {
						// A separate GTID incident cap is not proof of a writer-change cap.
						// If the original writer returns, its pending uploads can still
						// extend this baseline; historical foreign uploads do not forbid it.
						incompatibleWriter := myspec.ValidUUID(writerUUID) && myspec.ValidUUID(coverage.SourceUUID) && writerUUID != coverage.SourceUUID
						frozen := incompatibleWriter && bounded.End.Equal(w.End) && coverage.LatestForeignEventTime.After(w.End)
						// Equal safe endpoints prefer a lineage that can still extend.
						if bounded.End.After(observation.end) || bounded.End.Equal(observation.end) && !frozen && (observation.writerChanged || observation.blockedReason != "" || coverage.BlockedReason == "") {
							observation.end = bounded.End
							observation.writerChanged = frozen
							observation.blockedReason = coverage.BlockedReason
						}
					}
				}
			}
		}
	}
	if observation != nil {
		observation.pending = pendingErr
	}
	merged := mergeGTIDWindows(windows)
	if len(merged) == 0 && pendingErr != nil {
		return nil, byFull, pendingErr
	}
	if len(merged) == 0 && coverageErr != nil {
		if known := rememberedGTIDWindows(s); !requireFresh && rc.Cluster.Spec.Provider == "pxc" && len(known) > 0 {
			return known, nil, nil
		}
		return nil, nil, coverageErr
	}
	if rc.Cluster.Spec.Provider == "pxc" {
		if err := rememberGTIDWindows(ctx, rc, s, merged); err != nil {
			return nil, nil, err
		}
		if pendingErr != nil {
			return merged, byFull, nil
		}
		return rememberedGTIDWindows(s), byFull, nil
	}
	return merged, byFull, nil
}
func (p *Core) ValidatePITRRestore(ctx context.Context, rc *engine.Context, target time.Time) ([]domain.TimeWindow, error) {
	// Preserve the totally dead-source DR path. Its target-agent preflight
	// freshly validates staged inputs before activation; reachable source agents
	// must instead provide post-admission verification, never a status lease.
	windows, _, err := p.validatePITRRestoreMode(ctx, rc, target, rc.Agents != nil)
	return windows, err
}

func pitrTargetOutOfRange() error {
	return engine.InvalidField("source.pitrTime", domain.ReasonOutOfRange, "restore target is outside conservative contiguous binlog coverage; an unprovable writer lineage requires a fresh full backup from the current archive writer")
}

func (p *Core) validatePITRRestore(ctx context.Context, rc *engine.Context, target time.Time) ([]domain.TimeWindow, map[string][]domain.TimeWindow, error) {
	// Selection is not admission. An admitted restore still requires the fresh
	// public validator and fresh member-side input preflight before activation.
	return p.validatePITRRestoreMode(ctx, rc, target, false)
}

func (p *Core) validatePITRRestoreMode(ctx context.Context, rc *engine.Context, target time.Time, requireFresh bool) ([]domain.TimeWindow, map[string][]domain.TimeWindow, error) {
	if !rc.Cluster.Spec.Backup.PITR.Enabled {
		return nil, nil, engine.Invalid("PITR is not enabled for the source cluster")
	}
	var observation pitrWindowObservation
	windows, byFull, err := p.observePITRWindowsByFull(ctx, rc, &observation, requireFresh)
	if err != nil {
		return windows, byFull, err
	}
	for _, w := range windows {
		if !target.Before(w.Start) && !target.After(w.End) {
			return windows, byFull, nil
		}
	}
	if observation.pending != nil {
		return windows, byFull, observation.pending
	}
	return windows, byFull, pitrTargetOutOfRange()
}

func intersectCoverageWindow(window domain.TimeWindow, limits []domain.TimeWindow) []domain.TimeWindow {
	var out []domain.TimeWindow
	for _, limit := range limits {
		start, end := window.Start, window.End
		if limit.Start.After(start) {
			start = limit.Start
		}
		if limit.End.Before(end) {
			end = limit.End
		}
		if !end.Before(start) {
			out = append(out, domain.TimeWindow{Start: start, End: end})
		}
	}
	return out
}
func (p *Core) backupStatus(ctx context.Context, rc *engine.Context, status *domain.ClusterStatus) error {
	if rc.Store == nil {
		return nil
	}
	items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return err
	}
	for _, b := range items {
		if b.Phase == domain.PhaseSucceeded && b.MySQL != nil && b.CompletedAt != nil && (status.Backup.LastBackup == nil || b.CompletedAt.After(*status.Backup.LastBackup)) {
			t := *b.CompletedAt
			status.Backup.LastBackup = &t
		}
	}
	if !rc.Cluster.Spec.Backup.PITR.Enabled {
		return nil
	}
	var observation pitrWindowObservation
	windows, _, err := p.observePITRWindowsByFull(ctx, rc, &observation, false)
	condition := domain.Condition{Type: "PITR", Status: "False", Reason: "WindowUnavailable", LastTransitionTime: now(rc)}
	if err != nil {
		condition.Status = "Unknown"
		condition.Reason = "ObservationUnavailable"
		if pending, ok := err.(*pitrCoveragePending); ok {
			condition.Reason = "CoveragePending"
			condition.Message = pending.Error()
			status.Backup.PITRWindow = nil
		}
	} else if len(windows) > 0 {
		// Match the PITR endpoint: expose the newest contiguous window,
		// rather than the oldest (possibly snapshot-only) recovery point.
		latest := windows[0]
		for _, w := range windows[1:] {
			if w.End.After(latest.End) {
				latest = w
			}
		}
		status.Backup.PITRWindow = &latest
		condition.Status = "True"
		condition.Reason = "WindowAvailable"
		if observation.writerChanged && observation.end.Equal(latest.End) {
			condition.Reason = "WindowFrozenWriterChanged"
			condition.Message = fmt.Sprintf("Recovery window ends at %s because the archive writer changed; take a full backup to extend point-in-time recovery", latest.End.UTC().Format(time.RFC3339Nano))
		}
		if observation.blockedReason != "" && observation.end.Equal(latest.End) {
			condition.Status = "False"
			condition.Reason = "CoverageBlocked"
			condition.Message = fmt.Sprintf("Recovery coverage ends at %s: %s; inspect the binlog chain and take a full backup after resolving the missing history", latest.End.UTC().Format(time.RFC3339Nano), observation.blockedReason)
			known := false
			for _, old := range rc.Cluster.Status.Conditions {
				if old.Type == condition.Type && old.Reason == condition.Reason && old.Message == condition.Message {
					known = true
					break
				}
			}
			if !known && rc.Events != nil {
				if err := recordArchiveWarning(ctx, rc, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: condition.Reason, Message: condition.Message, CreatedAt: now(rc)}); err != nil {
					return err
				}
			}
		}
	}
	status.Conditions = append(status.Conditions, condition)
	s, e := load(rc.Cluster)
	if e == nil {
		writer, _, e := archiveWriter(ctx, rc, s)
		if e == nil {
			var a myspec.BinlogArchiveStatus
			condition := domain.Condition{Type: "BinlogArchive", Status: "Unknown", Reason: "ObservationUnavailable", Message: "Binlog archive observation unavailable", LastTransitionTime: now(rc)}
			if err := mysqlAction(ctx, rc, writer, myspec.ActionArchiveStatus, struct{}{}, &a); err == nil {
				condition.Reason = "ArchiveObservation"
				condition.Status = boolCondition(!a.Gap)
				condition.Message = fmt.Sprintf("Archive lag %.0f seconds; %s", a.LagSeconds, a.GapReason)
				if observation.writerChanged && status.Backup.PITRWindow != nil && observation.end.Equal(status.Backup.PITRWindow.End) {
					condition.Message = fmt.Sprintf("Writer archive lag %.0f seconds; recoverable window ends at %s after an archive writer change; take a full backup to extend point-in-time recovery; %s", a.LagSeconds, observation.end.UTC().Format(time.RFC3339Nano), a.GapReason)
				}
				if a.LastArchivedFile == "" || a.LastEventTime.IsZero() {
					condition.Status = "False"
					condition.Reason = "ArchivePending"
					condition.Message = "No binlog transaction has been archived"
				}
				if a.Error != "" {
					condition.Status = "False"
					condition.Reason = "ArchiveFailed"
					condition.Message = "Binlog archiving failed: " + a.Error + "; recovery coverage ends at the last successfully archived transaction"
					known := false
					for _, old := range rc.Cluster.Status.Conditions {
						if old.Type == condition.Type && old.Reason == condition.Reason && old.Message == condition.Message {
							known = true
							break
						}
					}
					if !known && rc.Events != nil {
						if err := recordArchiveWarning(ctx, rc, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Resource: writer, Type: "Warning", Reason: condition.Reason, Message: condition.Message, CreatedAt: now(rc)}); err != nil {
							return err
						}
					}
				}
			}
			status.Conditions = append(status.Conditions, condition)
		}
	}
	return nil
}

type archiveWarningKey struct {
	namespace, cluster, resource, reason, message string
}

type archiveWarningResult struct {
	once sync.Once
	err  error
}

// ObserveStatus retries before publishing conditions. Deduplicate successful
// appends within that pass, without losing warnings first seen on the retry or
// suppressing a retry after the recorder failed.
func recordArchiveWarning(ctx context.Context, rc *engine.Context, event domain.Event) error {
	cache := engine.ObservationPassCache(ctx)
	if cache == nil {
		return rc.Events.Record(ctx, event)
	}
	key := archiveWarningKey{domain.NormalizedNamespace(event.Namespace), event.Cluster, event.Resource, event.Reason, event.Message}
	value, found := cache.Load(key)
	if !found {
		value, _ = cache.LoadOrStore(key, &archiveWarningResult{})
	}
	result := value.(*archiveWarningResult)
	result.once.Do(func() { result.err = rc.Events.Record(ctx, event) })
	if result.err != nil {
		cache.CompareAndDelete(key, result)
	}
	return result.err
}

var _ engine.PITRRestoreValidator = (*Core)(nil)
