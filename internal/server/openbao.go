// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao"
)

func (s *Server) openbaoTarget(ctx context.Context, rc *engine.Context, target, action string) (openbao.Target, *response) {
	reject := func(r response) (openbao.Target, *response) { return openbao.Target{}, &r }
	if target == "" || strings.ContainsAny(target, "/\\") || target == rc.Cluster.Name {
		return reject(invalidField("targetCluster", domain.ReasonInvalid, "target must be another cluster in the same namespace"))
	}
	if !s.allowed(ctx, "database-users", action, clusterObject(rc.Namespace, target)) {
		return reject(fail(403, "forbidden", "Database connection management requires database-users/"+action+" permission on the target cluster"))
	}
	targetRC, provider, err := s.context(ctx, domain.ClusterRef{Namespace: rc.Namespace, Name: target})
	if err != nil {
		return reject(s.failure(err))
	}
	switch provider.Name() {
	case "postgres", "rabbitmq":
	case "psmdb":
		if targetRC.Cluster.Spec.Topology.Type != "replicaSet" {
			return reject(invalidField("targetCluster", domain.ReasonInvalid, "MongoDB target must use replica-set topology"))
		}
	default:
		return reject(invalidField("targetCluster", domain.ReasonInvalid, "target provider must be postgres, psmdb or rabbitmq"))
	}
	if failure := s.providerContext(targetRC); failure != nil {
		return openbao.Target{}, failure
	}
	return openbao.Target{Provider: provider, Context: targetRC}, nil
}

func (s *Server) openbao(ctx context.Context, name string, in input, rc *engine.Context, provider engine.Provider) response {
	access, supported := provider.(openbao.Access)
	if provider.Name() != "openbao" || !supported {
		return s.failure(engine.ErrNotSupported)
	}
	switch name {
	case "RevealOpenBaoUnsealKeys":
		if in.Body.(*api.OpenBaoUnsealKeysReveal).Confirm != rc.Cluster.Name {
			return invalidField("confirm", domain.ReasonInvalid, "confirmation must equal the cluster name")
		}
		keys, err := access.RevealUnsealKeys(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		return response{status: http.StatusOK, body: keys, noStore: true}
	case "ListOpenBaoDatabaseConnections":
		connections, err := access.ListDatabaseConnections(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		return ok(page[openbao.DatabaseConnection]{Items: nonNil(connections)})
	case "GetOpenBaoDatabaseConnection", "DeleteOpenBaoDatabaseConnection":
		connections, err := access.ListDatabaseConnections(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		for _, connection := range connections {
			if connection.Name != in.Name {
				continue
			}
			if name == "GetOpenBaoDatabaseConnection" {
				return ok(connection)
			}
			target, failure := s.openbaoTarget(ctx, rc, connection.TargetCluster, "delete")
			if failure != nil {
				return *failure
			}
			if err := access.DeleteDatabaseConnection(ctx, rc, target, in.Name); err != nil {
				return s.failure(err)
			}
			s.trigger(ctx, rc.Cluster.Ref())
			return response{status: http.StatusNoContent}
		}
		return s.failure(engine.ErrNotFound)
	case "CreateOpenBaoDatabaseConnection":
		body := in.Body.(*api.OpenBaoDatabaseConnectionWrite)
		target, failure := s.openbaoTarget(ctx, rc, body.TargetCluster, "create")
		if failure != nil {
			return *failure
		}
		req := openbao.DatabaseConnectionWrite{Name: body.Name, TargetCluster: body.TargetCluster, Roles: make([]openbao.DatabaseRoleWrite, len(body.Roles))}
		for i, role := range body.Roles {
			req.Roles[i] = openbao.DatabaseRoleWrite{Name: role.Name, Access: string(role.Access), Database: value(role.Database), DefaultTTLSeconds: value(role.DefaultTtlSeconds), MaxTTLSeconds: value(role.MaxTtlSeconds)}
		}
		connection, err := access.CreateDatabaseConnection(ctx, rc, target, req)
		if err != nil {
			return s.failure(err)
		}
		s.trigger(ctx, rc.Cluster.Ref())
		return response{status: http.StatusCreated, body: connection}
	}
	return s.failure(engine.ErrNotSupported)
}

// A referenced target must remain available until its managed identity is removed.
func (s *Server) openbaoDeletionGuard(ctx context.Context, cluster *domain.Cluster) *response {
	if cluster.Spec.Provider == "openbao" {
		return nil
	}
	clusters, err := s.Store.ListClusters(ctx, domain.NormalizedNamespace(cluster.Namespace))
	if err != nil {
		r := s.failure(err)
		return &r
	}
	for _, source := range clusters {
		if source.Spec.Provider != "openbao" {
			continue
		}
		rc, provider, err := s.context(ctx, source.Ref())
		if err != nil {
			r := s.failure(err)
			return &r
		}
		access, supported := provider.(openbao.Access)
		if !supported {
			r := s.failure(engine.ErrNotSupported)
			return &r
		}
		if failure := s.providerContext(rc); failure != nil {
			return failure
		}
		targets, err := access.ConnectionTargets(ctx, rc)
		if err != nil {
			r := s.failure(err)
			return &r
		}
		for _, target := range targets {
			if domain.NormalizedNamespace(target.Namespace) != cluster.Ref().Namespace || target.Name != cluster.Name {
				continue
			}
			connections, err := access.ListDatabaseConnections(ctx, rc)
			if err != nil {
				r := s.failure(err)
				return &r
			}
			for _, connection := range connections {
				if connection.TargetCluster == cluster.Name {
					r := fail(409, "conflict", fmt.Sprintf("Cluster is referenced by OpenBao cluster %s connection %s; delete that connection first", source.Name, connection.Name))
					return &r
				}
			}
			r := fail(409, "conflict", fmt.Sprintf("Cluster is referenced by OpenBao cluster %s; delete its database connection first", source.Name))
			return &r
		}
	}
	return nil
}

func (s *Server) RevealOpenBaoUnsealKeys(ctx context.Context, request api.RevealOpenBaoUnsealKeysRequestObject) (api.RevealOpenBaoUnsealKeysResponseObject, error) {
	return s.handle(ctx, "RevealOpenBaoUnsealKeys", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitRevealOpenBaoUnsealKeysResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) ListOpenBaoDatabaseConnections(ctx context.Context, request api.ListOpenBaoDatabaseConnectionsRequestObject) (api.ListOpenBaoDatabaseConnectionsResponseObject, error) {
	return s.handle(ctx, "ListOpenBaoDatabaseConnections", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListOpenBaoDatabaseConnectionsResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) CreateOpenBaoDatabaseConnection(ctx context.Context, request api.CreateOpenBaoDatabaseConnectionRequestObject) (api.CreateOpenBaoDatabaseConnectionResponseObject, error) {
	return s.handle(ctx, "CreateOpenBaoDatabaseConnection", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitCreateOpenBaoDatabaseConnectionResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) GetOpenBaoDatabaseConnection(ctx context.Context, request api.GetOpenBaoDatabaseConnectionRequestObject) (api.GetOpenBaoDatabaseConnectionResponseObject, error) {
	return s.handle(ctx, "GetOpenBaoDatabaseConnection", input{Cluster: request.Cluster, Name: request.Connection}), nil
}
func (r response) VisitGetOpenBaoDatabaseConnectionResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) DeleteOpenBaoDatabaseConnection(ctx context.Context, request api.DeleteOpenBaoDatabaseConnectionRequestObject) (api.DeleteOpenBaoDatabaseConnectionResponseObject, error) {
	return s.handle(ctx, "DeleteOpenBaoDatabaseConnection", input{Cluster: request.Cluster, Name: request.Connection}), nil
}
func (r response) VisitDeleteOpenBaoDatabaseConnectionResponse(w http.ResponseWriter) error {
	return r.write(w)
}
