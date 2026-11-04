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
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

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
func chooseNode(ns []node, rs []*record, group, replaces string, avoid ...string) (string, error) {
	eligible := make([]node, 0, len(ns))
	for _, n := range ns {
		// Allocations report NodeName, falling back to NodeID when omitted.
		// Accept either authoritative identity even if the node list has a name.
		excluded := slices.Contains(avoid, n.ID) || n.Name != "" && slices.Contains(avoid, n.Name)
		if n.Status == "ready" && n.SchedulingEligibility == "eligible" && !n.Drain && !excluded {
			eligible = append(eligible, n)
		}
	}
	if len(eligible) == 0 {
		if len(avoid) != 0 {
			return "", fmt.Errorf("choose volume node: %w", infra.ErrPlacementUnsatisfiable)
		}
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
func volumeName(namespace, hostname string) string {
	digest := sha256.Sum256([]byte(namespace))
	return "kk-" + hex.EncodeToString(digest[:4]) + "-" + hostname
}

func (b *Backend) createVolume(ctx context.Context, r *record) error {
	reserved := r.VolumeName != "" && r.Importing
	if r.VolumeName == "" {
		r.VolumeName = volumeName(b.target.ID, r.Hostname)
	}
	if reserved {
		found, err := b.findReservedVolume(ctx, r)
		if err != nil || found {
			return err
		}
	}
	var response struct{ Volume struct{ ID string } }
	err := b.c.put(ctx, "/v1/volume/host/create", nil, map[string]any{"Volume": map[string]any{"Name": r.VolumeName, "Namespace": b.target.ID, "PluginID": "mkdir", "NodeID": r.Node, "NodePool": b.target.Config["nodePool"], "RequestedCapacityMinBytes": int64(r.Resources.MinDisk * (1 << 30)), "RequestedCapacityMaxBytes": int64(r.Resources.MaxDisk * (1 << 30)), "RequestedCapabilities": []any{map[string]any{"AccessMode": "single-node-single-writer", "AttachmentMode": "file-system"}}}}, &response)
	if err != nil && reserved {
		observe, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if found, readErr := b.findReservedVolume(observe, r); readErr == nil && found {
			return nil
		}
	}
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

func (b *Backend) findReservedVolume(ctx context.Context, r *record) (bool, error) {
	var volumes []struct{ ID, Name, Namespace, NodeID, PluginID string }
	if _, err := b.c.get(ctx, "/v1/volumes", query("type", "host"), &volumes); err != nil {
		return false, err
	}
	for _, volume := range volumes {
		if volume.Name != r.VolumeName || volume.Namespace != b.target.ID {
			continue
		}
		if volume.NodeID != r.Node || volume.PluginID != "mkdir" || r.VolumeID != "" {
			return false, &APIError{Status: 409, Op: "volume create", Message: "reserved volume identity is ambiguous; inspect original storage before retrying"}
		}
		r.VolumeID = volume.ID
	}
	return r.VolumeID != "", nil
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

func (b *Backend) volumeStatus(ctx context.Context, r *record) (string, error) {
	if r.VolumeID == "" {
		return infra.StatusFailed, &APIError{Status: 409, Op: "volume observe", Message: "original volume identity is missing from metadata; inspect storage before resuming"}
	}
	var actual struct{ ID, Name, Namespace, NodeID, State string }
	_, err := b.c.get(ctx, "/v1/volume/host/"+r.VolumeID, nil, &actual)
	if errors.Is(err, infra.ErrNotFound) {
		return infra.StatusFailed, &infra.VolumeLostError{Volume: r.Hostname, ServiceID: r.ID, Message: "original host volume is missing; recover original storage or replace the member through its native safety workflow; refusing empty-volume recreation"}
	}
	if err != nil {
		return infra.StatusFailed, err
	}
	if actual.ID != r.VolumeID || actual.Name != r.VolumeName || actual.Namespace != b.target.ID || actual.NodeID != r.Node {
		return infra.StatusFailed, &APIError{Status: 409, Op: "volume observe", Message: "host volume identity or placement changed; inspect original storage before resuming"}
	}
	var node node
	_, err = b.c.get(ctx, "/v1/node/"+r.Node, nil, &node)
	if errors.Is(err, infra.ErrNotFound) {
		return infra.StatusFailed, silentFailure("original volume node is missing; recover that node or explicitly replace the member")
	}
	if err != nil {
		return infra.StatusFailed, err
	}
	switch {
	case node.Status != "ready":
		return infra.StatusFailed, silentFailure("original volume node is unavailable; recover that node; storage will not migrate automatically")
	case node.Drain:
		return infra.StatusFailed, silentFailure("original volume node is draining; resolve the drain or explicitly replace the member")
	case node.SchedulingEligibility != "eligible":
		return infra.StatusFailed, silentFailure("original volume node is scheduling-ineligible; restore eligibility before restarting the member")
	case actual.State != "ready":
		return infra.StatusCreating, silentFailure("original host volume is not ready; inspect the host-volume registration and original node")
	default:
		return infra.StatusActive, nil
	}
}
