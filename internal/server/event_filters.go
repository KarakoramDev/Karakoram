// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"sort"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

var eventResources = func() []string {
	resources := map[string]bool{
		"auth": true, "cluster-data": true, "orphaned-volumes": true, "operations": true,
		"rabbitmq-vhosts": true, "rabbitmq-users": true, "rabbitmq-permissions": true,
		"rabbitmq-topic-permissions": true, "rabbitmq-policies": true, "rabbitmq-operator-policies": true,
	}
	for _, rule := range api.OperationRBAC {
		if rule.Resource != "" && (rule.Action != "read" || rule.Resource == "cluster-credentials") {
			resources[rule.Resource] = true
		}
	}
	out := make([]string, 0, len(resources))
	for resource := range resources {
		out = append(out, resource)
	}
	sort.Strings(out)
	return out
}()

func (s *Server) validateObservationFilters(ctx context.Context, name string) *response {
	q := request(ctx).URL.Query()
	if cluster := q.Get("cluster"); cluster != "" {
		if err := domain.ValidateClusterName(cluster); err != nil {
			r := invalidField("cluster", domain.ReasonInvalid, "cluster must contain 2–12 lowercase letters or digits and start with a letter")
			return &r
		}
	}
	if name != "ListEvents" && name != "StreamEvents" {
		return nil
	}
	if kind := q.Get("type"); kind != "" && kind != "Normal" && kind != "Warning" && kind != "Audit" {
		r := invalidField("type", domain.ReasonUnsupported, "type must be one of: Normal, Warning, Audit")
		return &r
	}
	if resource := q.Get("resource"); resource != "" {
		if i := sort.SearchStrings(eventResources, resource); i == len(eventResources) || eventResources[i] != resource {
			r := invalidField("resource", domain.ReasonUnsupported, "resource must be one of: "+strings.Join(eventResources, ", "))
			return &r
		}
		allowed, err := s.filteredEventAccess(ctx, resource)
		if err != nil {
			r := s.failure(err)
			return &r
		}
		if !allowed {
			r := failDetails(403, "forbidden", "resource filter "+resource+" requires audit/read permission for this scope", map[string]any{"resource": resource, "permission": "audit/read"})
			return &r
		}
	}
	return nil
}

func (s *Server) filteredEventAccess(ctx context.Context, resource string) (bool, error) {
	q := request(ctx).URL.Query()
	namespace, cluster := namespaceFilter(ctx), q.Get("cluster")
	switch resource {
	case "auth", "accounts", "api-tokens", "rbac", "settings":
		return s.allowed(ctx, "audit", "read", "*"), nil
	case "namespaces", "backup-storages":
		if namespace != "" {
			return s.allowed(ctx, "audit", "read", namespace), nil
		}
		if s.allowed(ctx, "audit", "read", "*") {
			return true, nil
		}
		all, err := s.Store.ListNamespaces(ctx)
		for _, n := range all {
			if s.allowed(ctx, "audit", "read", n.Name) {
				return true, nil
			}
		}
		return false, err
	default:
		// Cluster-scoped resources, including cluster-data, share event visibility.
		canRead := func(object string) bool {
			return s.allowed(ctx, "events", "read", object) || s.allowed(ctx, "audit", "read", object)
		}
		if cluster != "" && namespace != "" {
			return canRead(clusterObject(namespace, cluster)), nil
		}
		if namespace == "" && canRead("*") {
			return true, nil
		}
		if cluster == "" && namespace != "" && canRead(clusterObject(namespace, "*")) {
			return true, nil
		}
		all, err := s.Store.ListClusters(ctx, namespace)
		for _, c := range all {
			if (cluster == "" || c.Name == cluster) && canRead(clusterObject(c.Namespace, c.Name)) {
				return true, nil
			}
		}
		return false, err
	}
}
