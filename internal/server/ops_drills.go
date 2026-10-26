// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/cronutil"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/drills"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"

	"github.com/KarakoramDev/Karakoram/internal/api"
)

func (s *Server) CreateBackupDrill(ctx context.Context, request api.CreateBackupDrillRequestObject) (api.CreateBackupDrillResponseObject, error) {
	return s.handle(ctx, "CreateBackupDrill", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateBackupDrillResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) ListBackupDrills(ctx context.Context, request api.ListBackupDrillsRequestObject) (api.ListBackupDrillsResponseObject, error) {
	return s.handle(ctx, "ListBackupDrills", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListBackupDrillsResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) GetBackupDrill(ctx context.Context, request api.GetBackupDrillRequestObject) (api.GetBackupDrillResponseObject, error) {
	return s.handle(ctx, "GetBackupDrill", input{Cluster: request.Cluster, Name: request.Drill}), nil
}
func (r response) VisitGetBackupDrillResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func validateDrillPolicy(c *domain.Cluster) *response {
	p := c.Spec.Backup.Drill
	if p == nil {
		return nil
	}
	field := func(name, message string) *response {
		r := invalidField("spec.backup.drill."+name, domain.ReasonInvalid, message)
		return &r
	}
	if c.ManagedBy != nil {
		return field("enabled", "Managed clusters cannot have a backup drill policy")
	}
	if strings.Contains(p.Schedule, "TZ=") {
		return field("schedule", "backup drill schedules use UTC; time zone overrides are not supported")
	}
	if _, err := cronutil.Parser().Parse(p.Schedule); err != nil {
		return field("schedule", "schedule must be a valid five-field cron expression")
	}
	if p.Timeout == "" {
		p.Timeout = "6h"
	}
	timeout, err := time.ParseDuration(p.Timeout)
	if err != nil || timeout < 30*time.Minute || timeout > 48*time.Hour {
		return field("timeout", "timeout must be a Go duration between 30m and 48h")
	}
	if p.Enabled {
		if err := drills.ValidateProvider(c.Spec.Provider); err != nil {
			return field("enabled", err.Error())
		}
	}
	if p.BackupSchedule != "" {
		for _, schedule := range c.Spec.Backup.Schedules {
			if schedule.Name == p.BackupSchedule {
				return nil
			}
		}
		return field("backupSchedule", "backupSchedule must name an existing backup schedule")
	}
	return nil
}

// Active targets are controller-owned; once terminal, operators may repair them.
// A missing report cannot prove completion and must not release ownership.
func (s *Server) drillMutationGuard(ctx context.Context, c *domain.Cluster) *response {
	if c.ManagedBy == nil || c.ManagedBy.Kind != "backup-drill" {
		return nil
	}
	d, err := s.Store.GetBackupDrill(ctx, c.ManagedBy.ID)
	if errors.Is(err, store.ErrNotFound) {
		r := fail(409, "conflict", "Backup drill target ownership cannot be released without a terminal report")
		return &r
	}
	if err != nil {
		r := s.failure(err)
		return &r
	}
	if d.Phase != "Succeeded" && d.Phase != "Failed" {
		r := fail(409, "conflict", "Backup drill target is managed until the drill is terminal")
		return &r
	}
	return nil
}

func (s *Server) backupDrills(ctx context.Context, name string, in input) response {
	ref := clusterRef(ctx, in.Cluster)
	c, err := s.Store.GetCluster(ctx, ref)
	if err != nil {
		return s.failure(err)
	}
	switch name {
	case "ListBackupDrills":
		items, err := s.Store.ListBackupDrills(ctx, ref)
		return listResponse(s, ctx, items, err)
	case "GetBackupDrill":
		d, err := s.Store.GetBackupDrill(ctx, in.Name)
		if err != nil {
			return s.failure(err)
		}
		if d.Namespace != ref.Namespace || d.Cluster != ref.Name {
			return s.failure(store.ErrNotFound)
		}
		return ok(d)
	case "CreateBackupDrill":
		if c.Deleting || (c.ManagedBy != nil && c.ManagedBy.Kind == "backup-drill") {
			return fail(409, "conflict", "Cannot drill a deleting cluster or a backup drill target")
		}
		if err := drills.ValidateProvider(c.Spec.Provider); err != nil {
			return fail(409, "unsupported", err.Error())
		}
		var requested string
		if b, ok := in.Body.(*api.CreateBackupDrillRequest); ok && b != nil {
			requested = value(b.Backup)
		}
		backup, err := drills.SelectBackup(ctx, s.Store, *c, requested)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrConflict) {
				return fail(409, "conflict", "No succeeded backup is eligible for this drill")
			}
			return s.failure(err)
		}
		id, err := secrets.RandomString(16)
		if err != nil {
			return s.failure(err)
		}
		now := time.Now().UTC()
		if s.Context.Clock != nil {
			now = s.Context.Clock.Now().UTC()
		}
		d := domain.BackupDrill{ID: id, Namespace: ref.Namespace, Cluster: ref.Name, Backup: backup.Name, BackupType: backup.Type, Trigger: "manual", Phase: "Pending", CreatedAt: now, Cleanup: domain.BackupDrillCleanup{Phase: "Pending", OrphanedVolumes: []string{}}}
		setIdempotencyRecovery(ctx, func(*domain.Operation) response { return response{status: 202, body: d} })
		if err := s.Store.CreateBackupDrill(ctx, d); err != nil {
			return s.failure(err)
		}
		return response{status: 202, body: d}
	}
	return invalid()
}

func drillContext(ctx context.Context, namespace string) context.Context {
	ctx = store.WithActor(ctx, "system:backup-drill")
	ctx = context.WithValue(ctx, identityKey{}, &auth.Principal{Account: "system:backup-drill"})
	return context.WithValue(ctx, requestKey{}, &http.Request{URL: &url.URL{RawQuery: url.Values{"namespace": {namespace}}.Encode()}})
}

func (s *Server) createDrillTarget(ctx context.Context, source domain.Cluster, d domain.BackupDrill) error {
	if err := drills.ValidateProvider(source.Spec.Provider); err != nil {
		return err
	}
	spec := source.Spec
	spec.Backup.Schedules = nil
	spec.Backup.Drill = nil
	spec.Exposure = domain.Exposure{}
	spec.Monitoring.Enabled = false
	spec.DataSource = &domain.DataSource{Namespace: source.Namespace, Cluster: source.Name, Backup: d.Backup}
	// Convert through the existing API model so defaults/validation stay identical.
	raw, err := json.Marshal(specAPI(spec))
	if err != nil {
		return err
	}
	var inputSpec api.ClusterInputSpec
	if err := json.Unmarshal(raw, &inputSpec); err != nil {
		return err
	}
	name := drills.TargetName(d.ID)
	if existing, err := s.Store.GetCluster(ctx, domain.ClusterRef{Namespace: source.Namespace, Name: name}); err == nil {
		if existing.ManagedBy != nil && existing.ManagedBy.Kind == "backup-drill" && existing.ManagedBy.ID == d.ID {
			return nil
		}
		return store.ErrConflict
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	ctx = drillContext(ctx, source.Namespace)
	r := s.createCluster(ctx, &api.CreateClusterRequest{Name: name, Spec: inputSpec}, &domain.ManagedBy{Kind: "backup-drill", Cluster: source.Name, ID: d.ID}, false)
	if r.status != 201 {
		return fmt.Errorf("drill target admission failed (HTTP %d)", r.status)
	}
	return nil
}

func (s *Server) deleteDrillTarget(ctx context.Context, target domain.Cluster) error {
	ctx = drillContext(ctx, target.Namespace)
	op := domain.Operation{Kind: domain.OperationDelete, Actor: "system:backup-drill"}
	ctx = store.WithOperation(ctx, &op)
	ref := domain.ClusterRef{Namespace: target.Namespace, Name: target.Name}
	current, err := s.Store.GetCluster(ctx, ref)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if target.ManagedBy == nil || current.ManagedBy == nil || current.ManagedBy.Kind != "backup-drill" || current.ManagedBy.ID != target.ManagedBy.ID {
		return store.ErrConflict
	}
	if current.Deleting {
		return nil
	}
	ctx = store.WithClusterSnapshot(ctx, current)
	if err := s.Store.MarkClusterDeleting(ctx, ref, domain.DeleteOptions{}); err != nil {
		return err
	}
	s.trigger(ctx, ref)
	return nil
}
