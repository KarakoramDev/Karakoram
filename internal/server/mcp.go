// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	agentmcp "github.com/KarakoramDev/Karakoram/internal/mcp"
)

func (s *Server) newMCPHandler() (http.Handler, error) {
	var once sync.Once
	var rest http.Handler
	metadata := s.MCPResourceMetadata
	if s.OAuth != nil {
		metadata = s.OAuth.ResourceMetadata()
	}
	options := agentmcp.Options{Mode: s.MCPMode, PublicURL: s.PublicURL, ResourceMetadata: metadata, Authenticate: s.authenticateMCP, Allowed: s.mcpAllowed, REST: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { rest = s.Handler() })
		rest.ServeHTTP(w, r)
	})}
	if s.MCPSealer != nil {
		options.SignIdempotency = s.MCPSealer.MCPIdempotencyHash
		options.Confirmer = &agentmcp.Confirmer{Sign: s.MCPSealer.MCPConfirmationHash, Audit: func(b agentmcp.Binding, accepted bool) error {
			reason := "MCPConfirmationDeclined"
			if accepted {
				reason = "MCPConfirmationAccepted"
			}
			actor := b.Principal
			if !strings.Contains(actor, " (mcp: ") {
				actor += " (mcp)"
			}
			return s.recordAudit(context.Background(), domain.Event{Type: "Normal", Reason: reason, Actor: actor, Message: b.Operation + " confirmation", Resource: "mcp"})
		}}
	}
	return agentmcp.New(options)
}

func (s *Server) authenticate(r *http.Request) (*auth.Principal, error) {
	if auth.IsMCPTraffic(r.Context()) {
		return s.authenticateMCP(r)
	}
	return s.Auth.Authenticate(r)
}

func (s *Server) authenticateMCP(r *http.Request) (*auth.Principal, error) {
	fields := strings.Fields(r.Header.Get("Authorization"))
	if s.OAuth != nil && len(fields) == 2 && strings.HasPrefix(fields[1], "kko_") {
		return s.OAuth.Authenticate(r)
	}
	return s.Auth.Authenticate(r)
}

func (s *Server) mcpAllowed(ctx context.Context, p *auth.Principal, id string) bool {
	rule, ok := api.OperationRBAC[id]
	if !ok {
		return false
	}
	if rule.Public {
		return true
	}
	objects := []string{"*"}
	// Only scoped operations may use scoped witnesses. A namespace grant must
	// never advertise an otherwise global settings/account/security operation.
	scoped := rule.ObjectParam != "*" || collection(id) || id == "createCluster" || id == "createBackupStorage" || id == "createNamespace"
	if scoped {
		clusters, err := s.Store.ListClusters(ctx, "")
		if err != nil {
			return false
		}
		for _, c := range clusters {
			objects = append(objects, c.Namespace+"/"+c.Name, c.Namespace)
		}
		namespaces, err := s.Store.ListNamespaces(ctx)
		if err != nil {
			return false
		}
		for _, n := range namespaces {
			objects = append(objects, n.Name)
		}
		if rule.ObjectParam == "storage" {
			storages, err := s.Store.ListBackupStorages(ctx, "")
			if err != nil {
				return false
			}
			for _, storage := range storages {
				objects = append(objects, storage.Namespace+"/"+storage.Name)
			}
		}
	}
	for _, object := range objects {
		if s.RBAC.Can(p.Subjects, rule.Resource, rule.Action, object) && mcpRestrictionAllows(p, id, rule, object) {
			return true
		}
	}
	return false
}

// Discovery uses the same shared deny overlay as REST, including secret-bearing
// operations. Object checks are repeated by REST for the exact call target.
func mcpRestrictionAllows(p *auth.Principal, id string, rule api.RBACRule, object string) bool {
	return auth.RestrictionAllowsOperation(p.Restrictions, rule.Resource, rule.Action, object, collection(id), api.OperationAgent[id].Secret)
}
