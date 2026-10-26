// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import "context"

func (b *Backend) checkPinnedNode(ctx context.Context, hostname string) error {
	if hostname == "" {
		return nil
	}
	nodes, err := b.nodes(ctx)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node.Description.Hostname != hostname {
			continue
		}
		if node.Status.State != "ready" {
			return silentFailure("original pinned storage node is unavailable; restore that node and its retained volume, or explicitly replace the member from healthy replicas/a verified backup; refusing empty-volume relocation")
		}
		if node.Spec.Availability != "active" {
			return silentFailure("original pinned storage node is paused or drained; reactivate that node, or explicitly replace the member from healthy replicas/a verified backup; refusing empty-volume relocation")
		}
		return nil
	}
	return silentFailure("original pinned storage node is missing from Swarm; restore its node identity and retained volume, or explicitly replace the member from healthy replicas/a verified backup; refusing empty-volume relocation")
}
