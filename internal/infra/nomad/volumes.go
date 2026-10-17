// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type node struct {
	ID, Name, Datacenter, NodePool, Status, SchedulingEligibility string
	Drain                                                         bool
	Attributes                                                    map[string]string
}

func (b *Backend) nodes(ctx context.Context) ([]node, error) {
	var ns []node
	if _, err := b.c.get(ctx, "/v1/nodes", nil, &ns); err != nil {
		return nil, err
	}
	out := ns[:0]
	for _, n := range ns {
		if n.NodePool != b.target.Config["nodePool"] {
			continue
		}
		match := false
		for _, dc := range datacenters(b.target.Config) {
			if ok, _ := path.Match(dc, n.Datacenter); ok {
				match = true
				break
			}
		}
		if match {
			out = append(out, n)
		}
	}
	return out, nil
}
func chooseNode(ns []node, rs []*record, group, replaces string) (string, error) {
	eligible := make([]node, 0, len(ns))
	for _, n := range ns {
		if n.Status == "ready" && n.SchedulingEligibility == "eligible" && !n.Drain {
			eligible = append(eligible, n)
		}
	}
	if len(eligible) == 0 {
		return "", fmt.Errorf("%w: no ready eligible nodes", infra.ErrUnavailable)
	}
	counts, total := map[string]int{}, map[string]int{}
	for _, r := range rs {
		if r.Kind == infra.KindVolume {
			total[r.Node]++
			if r.SpreadGroup == group && r.Hostname != replaces {
				counts[r.Node]++
			}
		}
	}
	sort.Slice(eligible, func(i, j int) bool {
		a, c := eligible[i], eligible[j]
		if counts[a.ID] != counts[c.ID] {
			return counts[a.ID] < counts[c.ID]
		}
		if total[a.ID] != total[c.ID] {
			return total[a.ID] < total[c.ID]
		}
		if a.Name != c.Name {
			return a.Name < c.Name
		}
		return a.ID < c.ID
	})
	return eligible[0].ID, nil
}
func (b *Backend) createVolume(ctx context.Context, r *record) error {
	namespace := sha256.Sum256([]byte(b.target.ID))
	r.VolumeName = "kk-" + hex.EncodeToString(namespace[:4]) + "-" + r.Hostname
	var response struct{ Volume struct{ ID string } }
	err := b.c.put(ctx, "/v1/volume/host/create", nil, map[string]any{"Volume": map[string]any{"Name": r.VolumeName, "Namespace": b.target.ID, "PluginID": "mkdir", "NodeID": r.Node, "NodePool": b.target.Config["nodePool"], "RequestedCapacityMinBytes": int64(r.Resources.MinDisk * (1 << 30)), "RequestedCapacityMaxBytes": int64(r.Resources.MaxDisk * (1 << 30)), "RequestedCapabilities": []any{map[string]any{"AccessMode": "single-node-single-writer", "AttachmentMode": "file-system"}}}}, &response)
	var api *APIError
	if errors.As(err, &api) && api.Status == 500 && strings.Contains(strings.ToLower(api.Message), "already exists") {
		return &APIError{Status: 409, Op: "volume create", Message: "host volume already exists"}
	}
	if err == nil {
		if response.Volume.ID == "" {
			return silentFailure("volume creation omitted identity")
		}
		r.VolumeID = response.Volume.ID
	}
	return err
}
func (b *Backend) cpuMHz(ctx context.Context, nodeID string) (int, error) {
	ns, err := b.nodes(ctx)
	if err != nil {
		return 0, err
	}
	frequency := 0
	for _, n := range ns {
		if nodeID != "" && n.ID != nodeID {
			continue
		}
		var full node
		if _, err = b.c.get(ctx, "/v1/node/"+n.ID, nil, &full); err != nil {
			return 0, err
		}
		total, e := strconv.ParseFloat(full.Attributes["cpu.totalcompute"], 64)
		cores, e2 := strconv.ParseFloat(full.Attributes["cpu.numcores"], 64)
		if e == nil && e2 == nil && total > 0 && cores > 0 {
			mhz := int(total / cores)
			if mhz > 0 && (frequency == 0 || mhz < frequency) {
				frequency = mhz
			}
		}
	}
	if frequency == 0 {
		frequency = 1000
	}
	return frequency, nil
}
