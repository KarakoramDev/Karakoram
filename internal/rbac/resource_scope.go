// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rbac

import "github.com/KarakoramDev/Karakoram/internal/api"

// resourceObjectParams includes nested authorization resources that have no
// object-addressed OpenAPI route. Audit also accepts bare namespace/global
// objects; its namespace/cluster objects use the same cluster scope rules.
var resourceObjectParams = func() map[string]string {
	out := map[string]string{"events": "cluster", "audit": "cluster", "monitoring": "cluster", "cluster-data": "cluster"}
	add := func(rule api.RBACRule) {
		if rule.ObjectParam != "" && rule.ObjectParam != "*" {
			out[rule.Resource] = rule.ObjectParam
		}
	}
	for _, rule := range api.OperationRBAC {
		add(rule)
	}
	for _, rule := range api.AdditionalRBAC {
		add(rule)
	}
	return out
}()

// ResourceObjectParam returns the namespaced object kind declared by the RBAC
// inventory (cluster, storage or namespace), or * for a global-only resource.
func ResourceObjectParam(resource string) string {
	if param := resourceObjectParams[resource]; param != "" {
		return param
	}
	return "*"
}
