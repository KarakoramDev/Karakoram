package datamcp

import (
	"errors"
	"fmt"
	"slices"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// AccessDecision is the effective permission for a cluster at one instant.
type AccessDecision struct{ Read, Write bool }

// DecideAccess evaluates token scope, owner RBAC, cluster opt-in and installation policy.
// Callers must check RBAC before loading the cluster to avoid existence disclosure.
func DecideAccess(caller Caller, ref domain.ClusterRef, cluster *domain.Cluster, authorize Authorizer, writes bool) AccessDecision {
	if !readPermission(caller, ref, authorize) || cluster == nil || cluster.Spec.DataMcp == nil || !cluster.Spec.DataMcp.Enabled {
		return AccessDecision{}
	}
	s := caller.Scope
	return AccessDecision{Read: true, Write: cluster.Spec.DataMcp.AllowsWrite() && slices.Contains(s.Permissions, "cluster-data:write") && !s.ReadOnly && authorize(caller.Subjects, RBACResource, ActionWrite, ref.Namespace+"/"+ref.Name) && writes}
}
func readPermission(c Caller, ref domain.ClusterRef, a Authorizer) bool {
	s := c.Scope
	return c.TokenID != "" && s != nil && a != nil && (len(s.Clusters) > 0 || len(s.Namespaces) > 0) && (len(s.Clusters) == 0 || slices.Contains(s.Clusters, ref.Namespace+"/"+ref.Name)) && (len(s.Namespaces) == 0 || slices.Contains(s.Namespaces, ref.Namespace)) && (slices.Contains(s.Permissions, "cluster-data:read") || slices.Contains(s.Permissions, "cluster-data:write")) && a(c.Subjects, RBACResource, ActionRead, ref.Namespace+"/"+ref.Name)
}
func validateManifest(m Manifest) error {
	names := map[string]bool{}
	for n := range m.Tools {
		names[n] = true
	}
	for n, r := range m.Tools {
		switch r.Class {
		case ClassRead, ClassWrite, ClassDestructive, ClassDeny:
		default:
			return fmt.Errorf("manifest %s tool %s: unknown class", m.Upstream, n)
		}
		if r.WriteAlias != "" {
			if r.Class != ClassRead || names[r.WriteAlias] {
				return fmt.Errorf("manifest %s: invalid or duplicate alias %s", m.Upstream, r.WriteAlias)
			}
			names[r.WriteAlias] = true
		}
	}
	return nil
}
func applyRules(args map[string]any, rules []ArgRule) error {
	for _, r := range rules {
		v, ok := args[r.Name]
		if ok && r.Forbidden {
			return errors.New("forbidden argument")
		}
		if !ok && r.Default != nil {
			v = *r.Default
			args[r.Name] = v
			ok = true
		}
		if !ok && r.DefaultString != nil {
			v = *r.DefaultString
			args[r.Name] = v
			ok = true
		}
		if !ok {
			continue
		}
		if r.Max != nil {
			n, yes := v.(float64)
			if !yes {
				return errors.New("numeric argument required")
			}
			if n > *r.Max {
				args[r.Name] = *r.Max
			}
		}
		if len(r.Allow) > 0 || len(r.Deny) > 0 {
			s, yes := v.(string)
			if !yes || (len(r.Allow) > 0 && !slices.Contains(r.Allow, s)) || slices.Contains(r.Deny, s) {
				return errors.New("argument value denied")
			}
		}
	}
	return nil
}
