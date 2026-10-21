// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/rbac"
)

// CloneRestrictions takes an independent authentication snapshot.
func CloneRestrictions(r *domain.TokenRestrictions) *domain.TokenRestrictions {
	if r == nil {
		return nil
	}
	c := *r
	c.Namespaces = slices.Clone(r.Namespaces)
	c.Clusters = slices.Clone(r.Clusters)
	c.Permissions = slices.Clone(r.Permissions)
	return &c
}

// NormalizeRestrictions validates names and the generated permission vocabulary.
func NormalizeRestrictions(r *domain.TokenRestrictions) (*domain.TokenRestrictions, error) {
	r = CloneRestrictions(r)
	if r == nil {
		return nil, nil
	}
	normalize := func(values []string, field string, valid func(string) bool) ([]string, error) {
		for i, value := range values {
			value = strings.TrimSpace(value)
			if value == "" || !valid(value) {
				return nil, domain.InvalidField("restrictions."+field, domain.ReasonOutOfRange, "invalid restriction value")
			}
			values[i] = value
		}
		slices.Sort(values)
		return slices.Compact(values), nil
	}
	var err error
	r.Namespaces, err = normalize(r.Namespaces, "namespaces", func(v string) bool { return domain.ValidateNamespaceName(v) == nil })
	if err != nil {
		return nil, err
	}
	r.Clusters, err = normalize(r.Clusters, "clusters", func(v string) bool {
		ns, name, ok := strings.Cut(v, "/")
		return ok && domain.ValidateNamespaceName(ns) == nil && domain.ValidateClusterName(name) == nil
	})
	if err != nil {
		return nil, err
	}
	r.Permissions, err = normalize(r.Permissions, "permissions", func(v string) bool {
		res, action, ok := strings.Cut(v, ":")
		return ok && action != "" && rbac.KnownPermission(res, action)
	})
	return r, err
}

// RestrictionAllows evaluates the deny overlay; nil preserves legacy authority.
func RestrictionAllows(r *domain.TokenRestrictions, resource, action, object string) bool {
	if r == nil {
		return true
	}
	if r.ReadOnly && action != "read" || resource == "cluster-credentials" && !r.AllowCredentialReveal {
		return false
	}
	if len(r.Permissions) > 0 && !slices.Contains(r.Permissions, resource+":"+action) && !slices.Contains(r.Permissions, resource+":*") {
		return false
	}
	return restrictionScopeAllows(r, object, rbac.ResourceObjectParam(resource) == "cluster")
}

// RestrictionAllowsOperation checks collections without granting global-object access.
func RestrictionAllowsOperation(r *domain.TokenRestrictions, resource, action, object string, collection, secret bool) bool {
	if r == nil {
		return true
	}
	if secret && !r.AllowCredentialReveal {
		return false
	}
	if collection {
		c := *r
		c.Namespaces, c.Clusters = nil, nil
		return RestrictionAllows(&c, resource, action, object)
	}
	return RestrictionAllows(r, resource, action, object)
}

func restrictionScopeAllows(r *domain.TokenRestrictions, object string, clusterObject bool) bool {
	if len(r.Namespaces) == 0 && len(r.Clusters) == 0 {
		return true
	}
	if object == "*" || object == "**" || object == "" {
		return false
	}
	namespace, _, namespaced := strings.Cut(object, "/")
	if len(r.Namespaces) > 0 && !slices.Contains(r.Namespaces, namespace) {
		return false
	}
	if len(r.Clusters) == 0 {
		return true
	}
	if namespaced && clusterObject {
		return slices.Contains(r.Clusters, object)
	}
	for _, cluster := range r.Clusters {
		if strings.HasPrefix(cluster, namespace+"/") {
			return true
		}
	}
	return false
}

// RestrictionsSubset rejects any child overlay that could widen the parent's authority.
func RestrictionsSubset(child, parent *domain.TokenRestrictions) bool {
	if parent == nil {
		return true
	}
	if child == nil || parent.ReadOnly && !child.ReadOnly || child.AllowCredentialReveal && !parent.AllowCredentialReveal {
		return false
	}
	if len(parent.Permissions) > 0 {
		if len(child.Permissions) == 0 {
			return false
		}
		for _, permission := range child.Permissions {
			resource, _, _ := strings.Cut(permission, ":")
			if !slices.Contains(parent.Permissions, permission) && !slices.Contains(parent.Permissions, resource+":*") {
				return false
			}
		}
	}
	// A finite cluster scope can narrow either a namespace or a cluster scope.
	if len(child.Clusters) > 0 {
		for _, cluster := range child.Clusters {
			if restrictionScopeAllows(child, cluster, true) && !restrictionScopeAllows(parent, cluster, true) {
				return false
			}
		}
		return true
	}
	if len(parent.Clusters) > 0 {
		return false
	}
	if len(parent.Namespaces) > 0 {
		if len(child.Namespaces) == 0 {
			return false
		}
		for _, namespace := range child.Namespaces {
			if !slices.Contains(parent.Namespaces, namespace) {
				return false
			}
		}
	}
	return true
}
