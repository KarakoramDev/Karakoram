// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func openbaoSnapshotAdmission(spec domain.ClusterSpec) *response {
	if spec.Provider != "openbao" {
		return nil
	}
	var options openbao.Options
	if len(spec.EngineOptions) > 0 && json.Unmarshal(spec.EngineOptions, &options) != nil {
		r := invalidField("spec.engineOptions", domain.ReasonInvalid, "invalid OpenBao engine options")
		return &r
	}
	if options.Storage.Type == "postgresql" {
		r := invalidField("spec.engineOptions.storage", domain.ReasonUnsupported, "OpenBao PostgreSQL storage does not support raft snapshots or snapshot restores; use backups of the bound PostgreSQL cluster")
		return &r
	}
	return nil
}

// Restore shares are request-only: only the encrypted secret store receives them.
func (s *Server) openbaoRestoreValues(ctx context.Context, provider string, source domain.ClusterRef, backupName, restoreName string, input *api.OpenBaoRestoreUnsealShares) (map[string][]byte, *response) {
	reject := func(message string) (map[string][]byte, *response) {
		r := invalidField("openbaoUnsealShares", domain.ReasonInvalid, message)
		return nil, &r
	}
	if provider != "openbao" {
		if input != nil {
			return reject("openbaoUnsealShares is only supported for OpenBao raft-snapshot restores")
		}
		return nil, nil
	}
	if backupName == "" {
		if input != nil {
			return reject("openbaoUnsealShares requires a succeeded OpenBao raft-snapshot backup")
		}
		return nil, nil
	}
	backup, err := s.Store.GetBackup(ctx, source, backupName)
	if err != nil {
		r := s.backupDependencyFailure(err, source, backupName)
		return nil, &r
	}
	if backup.Provider != "openbao" || backup.Type != "raft-snapshot" || backup.Phase != domain.PhaseSucceeded {
		return reject("OpenBao restore requires a succeeded OpenBao raft-snapshot backup")
	}
	sourceCluster, err := s.Store.GetCluster(ctx, source)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		r := s.failure(err)
		return nil, &r
	}
	if input == nil {
		if errors.Is(err, store.ErrNotFound) {
			return reject("openbaoUnsealShares is required because the OpenBao backup source cluster no longer exists")
		}
		if !openbao.SnapshotBelongsToCluster(sourceCluster, backup) {
			return reject("openbaoUnsealShares is required because the backup belongs to an earlier incarnation of the OpenBao source cluster")
		}
		return nil, nil
	}
	material := openbao.RestoreUnsealShares{Shares: input.Shares, Threshold: input.Threshold, SealType: string(value(input.SealType)), Recovery: value(input.Recovery)}
	if err := material.Validate(); err != nil {
		r := s.failure(err)
		return nil, &r
	}
	data, err := json.Marshal(material)
	if err != nil {
		r := s.failure(err)
		return nil, &r
	}
	return map[string][]byte{openbao.RestoreUnsealSecret(restoreName): data}, nil
}
