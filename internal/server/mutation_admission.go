// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// initialRestoreMutation selects actions which cannot amend initial provisioning.
// Spec PUT/PATCH keeps its existing narrow placeability-amendment admission;
// backups, target deletion and reads retain their independent lifecycle policy.
func initialRestoreMutation(name string) bool {
	switch name {
	case "RestartCluster", "RotateClusterTLS", "RaiseClusterFCV", "ReplaceClusterMember", "RotateSystemUser", "ResumeCluster", "RetryClusterDeployments", "ReplaceClusterBalancer":
		return true
	}
	native := strings.Contains(name, "Postgres") || strings.Contains(name, "Elastic") || strings.Contains(name, "RabbitMQ") || strings.Contains(name, "Supabase") || strings.Contains(name, "DatabaseUser") || strings.Contains(name, "DatabaseRole")
	if !native {
		return false
	}
	return strings.HasPrefix(name, "Create") || strings.HasPrefix(name, "Replace") || strings.HasPrefix(name, "Update") || strings.HasPrefix(name, "Patch") || strings.HasPrefix(name, "Delete") || strings.HasPrefix(name, "Rotate") || strings.HasPrefix(name, "Set") || strings.HasPrefix(name, "Put") || strings.HasPrefix(name, "Grant") || strings.HasPrefix(name, "Revoke") || strings.HasPrefix(name, "Invalidate") || strings.HasPrefix(name, "Import")
}

func (s *Server) unsupportedClusterAction(name string, p engine.Provider) response {
	action := operationKind(name)
	if action == "" {
		action = operationID(name)
	}
	err := domain.Conflict("operation", action, domain.ReasonUnsupported, engine.ErrNotSupported, "Operation %s is not supported by engine %s", action, p.Name())
	if action == domain.OperationRotateSystemUser && (p.Name() == "postgres" || p.Name() == "supabase") {
		err.Message += "; for non-reserved PostgreSQL login roles use POST /api/v1/clusters/{cluster}/postgres/roles/{role}/password. Managed roles such as postgres remain engine-owned"
	}
	return s.failure(err)
}
