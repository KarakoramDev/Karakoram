// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/robfig/cron/v3"
)

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
		w := source.Status.Backup.PITRWindow
		if !source.Spec.Backup.PITR.Enabled || n.Spec.Provider != "pxc" && (w == nil || d.PITRTime.Before(w.Start) || d.PITRTime.After(w.End)) {
			return engine.Invalid("dataSource PITR time must be in the source conservative binlog window")
		}
		// PXC live/archived provenance is checked by the server's source
		// validator and again at execution. Its single cached status window
		// cannot represent safe ranges separated by a durable GTID incident.
	}
	return nil
}
func archiveWriter(ctx context.Context, rc *engine.Context, s *state) (string, error) {
	for _, m := range s.data() {
		st, err := live(ctx, rc, m.Name)
		if err == nil && st.MemberState == "ONLINE" && (s.Topology == "single" || onlinePrimary(st)) {
			return m.Name, nil
		}
	}
	return "", engine.ErrUnavailable
}

func (p *Core) PITRWindow(ctx context.Context, rc *engine.Context) ([]domain.TimeWindow, error) {
	windows, _, err := p.pitrWindowsByFull(ctx, rc)
	return windows, err
}

// pitrWindowsByFull returns nil lineage coverage when only remembered windows
// are available. A non-nil map contains the live, GTID-bounded coverage of each
// full backup, keyed by backup name.
func (p *Core) pitrWindowsByFull(ctx context.Context, rc *engine.Context) ([]domain.TimeWindow, map[string][]domain.TimeWindow, error) {
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
	if incident := activeGTIDIncident(s); incident != nil || observationErr != nil {
		if known := rememberedGTIDWindows(s); len(known) > 0 {
			return known, nil, nil
		}
		if incident != nil {
			return nil, nil, engine.Invalid("%s", incident.message())
		}
		return nil, nil, observationErr
	}
	storage, err := p.backupStorage(ctx, rc, "", rc.Cluster.Ref().Namespace)
	if err != nil {
		return nil, nil, err
	}
	writer, err := archiveWriter(ctx, rc, s)
	if err != nil {
		if known := rememberedGTIDWindows(s); rc.Cluster.Spec.Provider == "pxc" && len(known) > 0 {
			return known, nil, nil
		}
		return nil, nil, err
	}
	items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return nil, nil, err
	}
	var windows []domain.TimeWindow
	byFull := make(map[string][]domain.TimeWindow)
	var coverageErr error
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
		if e = mysqlAction(ctx, rc, writer, myspec.ActionPITRCoverage, myspec.PITRCoverageRequest{Source: repo, ObjectPaths: m.ObjectPaths, ObjectManifests: refs}, &coverage); e != nil {
			coverageErr = e
			continue
		}
		for _, w := range coverage.Windows {
			if w.Start.Before(m.EndTime) || w.End.Before(w.Start) {
				return nil, nil, engine.Invalid("agent returned invalid baseline-specific PITR coverage")
			}
			boundedEnd := gtidWindowEnd(s, m.EndTime, w.End)
			if !boundedEnd.IsZero() && !boundedEnd.Before(w.Start) {
				bounded := domain.TimeWindow{Start: w.Start, End: boundedEnd}
				windows = append(windows, bounded)
				byFull[b.Name] = append(byFull[b.Name], bounded)
			}
		}
	}
	merged := mergeGTIDWindows(windows)
	if len(merged) == 0 && coverageErr != nil {
		if known := rememberedGTIDWindows(s); rc.Cluster.Spec.Provider == "pxc" && len(known) > 0 {
			return known, nil, nil
		}
		return nil, nil, coverageErr
	}
	if rc.Cluster.Spec.Provider == "pxc" {
		if err := rememberGTIDWindows(ctx, rc, s, merged); err != nil {
			return nil, nil, err
		}
		return rememberedGTIDWindows(s), byFull, nil
	}
	return merged, byFull, nil
}
func (p *Core) ValidatePITRRestore(ctx context.Context, rc *engine.Context, target time.Time) ([]domain.TimeWindow, error) {
	windows, _, err := p.validatePITRRestore(ctx, rc, target)
	return windows, err
}

func pitrTargetOutOfRange() error {
	return engine.InvalidField("source.pitrTime", domain.ReasonOutOfRange, "restore target is outside conservative contiguous binlog coverage; an unprovable writer lineage requires a fresh full backup from the current archive writer")
}

func (p *Core) validatePITRRestore(ctx context.Context, rc *engine.Context, target time.Time) ([]domain.TimeWindow, map[string][]domain.TimeWindow, error) {
	if !rc.Cluster.Spec.Backup.PITR.Enabled {
		return nil, nil, engine.Invalid("PITR is not enabled for the source cluster")
	}
	windows, byFull, err := p.pitrWindowsByFull(ctx, rc)
	if err != nil {
		return windows, byFull, err
	}
	for _, w := range windows {
		if !target.Before(w.Start) && !target.After(w.End) {
			return windows, byFull, nil
		}
	}
	return windows, byFull, pitrTargetOutOfRange()
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
	windows, err := p.PITRWindow(ctx, rc)
	condition := domain.Condition{Type: "PITR", Status: "False", Reason: "WindowUnavailable", LastTransitionTime: now(rc)}
	if err != nil {
		condition.Status = "Unknown"
		condition.Reason = "ObservationUnavailable"
	} else if len(windows) > 0 {
		status.Backup.PITRWindow = &windows[0]
		condition.Status = "True"
		condition.Reason = "WindowAvailable"
	}
	status.Conditions = append(status.Conditions, condition)
	s, e := load(rc.Cluster)
	if e == nil {
		writer, e := archiveWriter(ctx, rc, s)
		if e == nil {
			var a myspec.BinlogArchiveStatus
			if mysqlAction(ctx, rc, writer, myspec.ActionArchiveStatus, struct{}{}, &a) == nil {
				status.Conditions = append(status.Conditions, domain.Condition{Type: "BinlogArchive", Status: boolCondition(!a.Gap), Reason: "ArchiveObservation", Message: fmt.Sprintf("Archive lag %.0f seconds; %s", a.LagSeconds, a.GapReason), LastTransitionTime: now(rc)})
			}
		}
	}
	return nil
}

var _ engine.PITRRestoreValidator = (*Core)(nil)
