// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/rbac"
	"github.com/casbin/casbin/v2/util"
)

var tokenPermissionVocabulary = func() map[string]map[string]bool {
	vocabulary := map[string]map[string]bool{}
	add := func(rule api.RBACRule) {
		if rule.Resource == "" {
			return
		}
		if vocabulary[rule.Resource] == nil {
			vocabulary[rule.Resource] = map[string]bool{}
		}
		vocabulary[rule.Resource][rule.Action] = true
	}
	for _, rule := range api.OperationRBAC {
		add(rule)
	}
	for _, rule := range api.AdditionalRBAC {
		add(rule)
	}
	// Export is a nested check, also included in the RBAC policy vocabulary.
	add(api.RBACRule{Resource: "backups", Action: "export"})
	return vocabulary
}()

// effectivePermissionRules intersects policy patterns with the immutable overlay.
// Denies remain in the result: an allow must never hide an owner policy deny.
func (s *Server) effectivePermissionRules(ctx context.Context) []domain.PolicyRule {
	p := principal(ctx)
	if p == nil {
		return nil
	}
	rules := s.RBAC.Permissions(p.Subjects)
	if p.Restrictions == nil {
		return rules
	}
	vocabulary := tokenPermissionVocabulary
	var out []domain.PolicyRule
	seen := map[string]bool{}
	for _, rule := range rules {
		v := rule.Values
		if rule.PType != "p" || len(v) < 5 {
			continue
		}
		if v[4] == "deny" {
			out = append(out, rule)
			continue
		}
		for resource, actions := range vocabulary {
			if match, err := util.GlobMatch(resource, v[1]); err != nil || !match {
				continue
			}
			for action := range actions {
				if match, err := util.GlobMatch(action, v[2]); err != nil || !match {
					continue
				}
				for _, object := range restrictedPermissionObjects(p.Restrictions, resource, v[3]) {
					if !auth.RestrictionAllows(p.Restrictions, resource, action, object) {
						continue
					}
					key := resource + "\x00" + action + "\x00" + object
					if seen[key] {
						continue
					}
					seen[key] = true
					out = append(out, domain.PolicyRule{PType: "p", Values: []string{v[0], resource, action, object, "allow"}})
				}
			}
		}
	}
	slices.SortFunc(out, func(a, b domain.PolicyRule) int {
		return slices.Compare(a.Values, b.Values)
	})
	return out
}

func restrictedPermissionObjects(r *domain.TokenRestrictions, resource, pattern string) []string {
	if len(r.Namespaces) == 0 && len(r.Clusters) == 0 {
		return []string{pattern}
	}
	objectParam := rbac.ResourceObjectParam(resource)
	scoped := objectParam != "*"
	if !scoped {
		return nil
	}
	var out []string
	if len(r.Clusters) > 0 && (objectParam == "cluster" || objectParam == "namespace") {
		for _, cluster := range r.Clusters {
			object := cluster
			if resource == "namespaces" {
				object, _, _ = strings.Cut(cluster, "/")
			}
			if match, err := util.GlobMatch(object, pattern); err == nil && match {
				out = append(out, object)
			}
			if resource == "audit" {
				namespace, _, _ := strings.Cut(cluster, "/")
				if match, err := util.GlobMatch(namespace, pattern); err == nil && match {
					out = append(out, namespace)
				}
			}
		}
		return out
	}
	namespaces := r.Namespaces
	if len(r.Clusters) > 0 {
		// Non-cluster namespaced objects inherit the listed clusters' namespaces,
		// not their cluster names. The final predicate intersects namespaces too.
		namespaces = nil
		for _, cluster := range r.Clusters {
			namespace, _, _ := strings.Cut(cluster, "/")
			if !slices.Contains(namespaces, namespace) {
				namespaces = append(namespaces, namespace)
			}
		}
	}
	for _, namespace := range namespaces {
		if resource == "namespaces" {
			if match, err := util.GlobMatch(namespace, pattern); err == nil && match {
				out = append(out, namespace)
			}
			continue
		}
		if pattern == "**" || pattern == "*" {
			out = append(out, namespace+"/**")
			if resource == "audit" {
				out = append(out, namespace)
			}
			continue
		}
		prefix, name, ok := strings.Cut(pattern, "/")
		if match, err := util.GlobMatch(namespace, prefix); ok && err == nil && match {
			out = append(out, namespace+"/"+name)
		}
		if match, err := util.GlobMatch(namespace, pattern); resource == "audit" && err == nil && match {
			out = append(out, namespace)
		}
	}
	return out
}
