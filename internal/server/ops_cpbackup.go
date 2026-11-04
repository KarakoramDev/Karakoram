// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/cpbackup"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"

	"github.com/KarakoramDev/Karakoram/internal/api"
)

func (s *Server) GetControlPlaneBackupPolicy(ctx context.Context, request api.GetControlPlaneBackupPolicyRequestObject) (api.GetControlPlaneBackupPolicyResponseObject, error) {
	return s.handle(ctx, "GetControlPlaneBackupPolicy", input{}), nil
}
func (r response) VisitGetControlPlaneBackupPolicyResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) UpdateControlPlaneBackupPolicy(ctx context.Context, request api.UpdateControlPlaneBackupPolicyRequestObject) (api.UpdateControlPlaneBackupPolicyResponseObject, error) {
	return s.handle(ctx, "UpdateControlPlaneBackupPolicy", input{Body: request.Body}), nil
}
func (r response) VisitUpdateControlPlaneBackupPolicyResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) ListControlPlaneBackups(ctx context.Context, request api.ListControlPlaneBackupsRequestObject) (api.ListControlPlaneBackupsResponseObject, error) {
	return s.handle(ctx, "ListControlPlaneBackups", input{}), nil
}
func (r response) VisitListControlPlaneBackupsResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) CreateControlPlaneBackup(ctx context.Context, request api.CreateControlPlaneBackupRequestObject) (api.CreateControlPlaneBackupResponseObject, error) {
	return s.handle(ctx, "CreateControlPlaneBackup", input{}), nil
}
func (r response) VisitCreateControlPlaneBackupResponse(w http.ResponseWriter) error {
	return r.write(w)
}

func (s *Server) controlPlaneBackup(ctx context.Context, name string, in input) response {
	switch name {
	case "GetControlPlaneBackupPolicy":
		p, err := cpbackup.Policy(ctx, s.Store)
		if err != nil {
			return s.failure(err)
		}
		return ok(p)
	case "UpdateControlPlaneBackupPolicy":
		body := in.Body.(*api.UpdateControlPlaneBackupPolicyJSONRequestBody)
		p := cpbackup.Defaults(domain.ControlPlaneBackupPolicy{Enabled: body.Enabled, Schedule: body.Schedule, RetainCount: body.RetainCount, Storage: domain.ControlPlaneBackupStorage{Namespace: body.Storage.Namespace, Name: body.Storage.Name}})
		if err := cpbackup.Validate(ctx, s.Store, p); err != nil {
			return fail(422, "validation_failed", err.Error())
		}
		raw, _ := json.Marshal(p)
		if err := s.Store.SetSetting(ctx, cpbackup.PolicyKey, raw); err != nil {
			return s.failure(err)
		}
		return ok(p)
	case "ListControlPlaneBackups":
		items, err := s.Store.ListControlPlaneBackups(ctx)
		return listResponse(s, ctx, items, err)
	case "CreateControlPlaneBackup":
		p, err := cpbackup.Policy(ctx, s.Store)
		if err != nil {
			return s.failure(err)
		}
		if p.Storage.Name == "" {
			return fail(422, "validation_failed", "storage must reference an existing S3 backup storage")
		}
		if err := cpbackup.Validate(ctx, s.Store, p); err != nil {
			return fail(422, "validation_failed", err.Error())
		}
		id, err := secrets.RandomString(16)
		if err != nil {
			return s.failure(err)
		}
		b := domain.ControlPlaneBackup{ID: id, Trigger: "manual", Phase: domain.PhasePending, Storage: p.Storage, CreatedAt: time.Now().UTC()}
		setIdempotencyRecovery(ctx, func(*domain.Operation) response { return response{status: 202, body: b} })
		if err := s.Store.EnqueueControlPlaneBackup(ctx, &b, nil); err != nil {
			if errors.Is(err, store.ErrConflict) {
				return fail(409, "conflict", "A control-plane backup is already pending or running")
			}
			return s.failure(err)
		}
		return response{status: 202, body: b}
	}
	return fail(500, "internal_error", "Unknown control-plane backup operation")
}
