// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"testing"
)

func TestIdleReconcileZeroProviderMutations(t *testing.T) {
	for _, topology := range []string{"single-node", "cluster", "dedicatedMasters", "stack"} {
		t.Run(topology, func(t *testing.T) {
			replicas := 3
			if topology == "single-node" {
				replicas = 1
			}
			p, rc, _, _ := coreSetup(t, replicas)
			if topology == "dedicatedMasters" {
				rc.Cluster.Spec.Topology.Type = topology
				p.ApplyDefaults(&rc.Cluster.Spec)
			}
			if topology == "stack" {
				for _, name := range []string{kindKibana, kindLogstash, kindAPM} {
					c := rc.Cluster.Spec.Components.Extra[name]
					c.Replicas = 1
					rc.Cluster.Spec.Components.Extra[name] = c
				}
			}
			enginetest.AssertIdle(t, p, rc, nil)
		})
	}
}
