// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/netip"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func (s *Server) DryRunCreateCluster(ctx context.Context, r api.DryRunCreateClusterRequestObject) (api.DryRunCreateClusterResponseObject, error) {
	return s.handle(ctx, "DryRunCreateCluster", input{Body: r.Body}), nil
}
func (r response) VisitDryRunCreateClusterResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DryRunReplaceCluster(ctx context.Context, r api.DryRunReplaceClusterRequestObject) (api.DryRunReplaceClusterResponseObject, error) {
	return s.handle(ctx, "DryRunReplaceCluster", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitDryRunReplaceClusterResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DryRunPatchCluster(ctx context.Context, r api.DryRunPatchClusterRequestObject) (api.DryRunPatchClusterResponseObject, error) {
	return s.handle(ctx, "DryRunPatchCluster", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitDryRunPatchClusterResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DryRunCreateRestore(ctx context.Context, r api.DryRunCreateRestoreRequestObject) (api.DryRunCreateRestoreResponseObject, error) {
	return s.handle(ctx, "DryRunCreateRestore", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitDryRunCreateRestoreResponse(w http.ResponseWriter) error { return r.write(w) }

type dryRunEffect struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}
type dryRunPreview struct {
	Operation string         `json:"operation"`
	Cluster   api.Cluster    `json:"cluster"`
	Restore   *api.Restore   `json:"restore,omitempty"`
	Changes   []specChange   `json:"changes"`
	Effects   []dryRunEffect `json:"effects"`
	Warnings  []string       `json:"warnings"`
}

func (s *Server) dryRunResult(operation string, before, after *domain.Cluster, restore *domain.Restore) response {
	result := dryRunPreview{Operation: operation, Cluster: s.clusterAPI(*after), Changes: []specChange{}, Effects: []dryRunEffect{}, Warnings: []string{}}
	if operation == "create" {
		result.Cluster.Status.Phase = api.ClusterStatusPhaseCreating
		result.Cluster.Status.Message = ptr("Preview only; provisioning has not been queued.")
	}
	add := func(kind, message string) {
		for _, e := range result.Effects {
			if e.Kind == kind {
				return
			}
		}
		result.Effects = append(result.Effects, dryRunEffect{kind, message})
	}
	if operation == "create" && after.Spec.DataSource != nil {
		add("clone", "Creates a new cluster from a recovery source.")
	}
	if restore != nil {
		r := restoreAPI(*restore)
		result.Restore = &r
		if restore.Source.SourceCluster == after.Name && domain.NormalizedNamespace(restore.SourceNamespace) == domain.NormalizedNamespace(after.Namespace) {
			add("in-place-restore", "Restores over data in the source cluster.")
		} else {
			add("clone", "Restores data from another cluster.")
		}
		result.Warnings = append(result.Warnings, "Admission checks do not guarantee that a future restore will complete.")
	}
	if before != nil && operation != "restore" {
		var b, a any
		oldJSON, err := json.Marshal(before.Spec)
		if err != nil {
			return s.failure(err)
		}
		newJSON, err := json.Marshal(after.Spec)
		if err != nil {
			return s.failure(err)
		}
		if err = json.Unmarshal(oldJSON, &b); err != nil {
			return s.failure(err)
		}
		if err = json.Unmarshal(newJSON, &a); err != nil {
			return s.failure(err)
		}
		diffSpecValues("", b, a, &result.Changes)
		for i := range result.Changes {
			change := &result.Changes[i]
			path := change.Path
			if path == "version" {
				add("version-upgrade", "Changes the engine version.")
			}
			if strings.HasSuffix(path, ".replicas") || path == "topology.shards" {
				old, _ := change.Before.(float64)
				next, _ := change.After.(float64)
				if next < old {
					add("member-removal", "Reduces a replica or shard count.")
				}
				if next > old {
					add("member-addition", "Increases a replica or shard count.")
				}
			}
			if strings.Contains(path, ".resources.") {
				add("resource-change", "Changes infrastructure resource sizing.")
			}
			change.Path = "spec." + path
		}
		if exposureWidens(before.Spec.Exposure, after.Spec.Exposure) {
			add("public-exposure", "Enables public access or widens its allowed address ranges.")
		}
		if (before.Spec.TLS.Mode == "requireTLS" && after.Spec.TLS.Mode != "requireTLS") ||
			(before.Spec.TLS.Mode == "preferTLS" && after.Spec.TLS.Mode == "disabled") {
			add("tls-downgrade", "Weakens the transport encryption requirement.")
		}
		disabled := before.Spec.Backup.PITR.Enabled && !after.Spec.Backup.PITR.Enabled
		for _, old := range before.Spec.Backup.Schedules {
			if !old.Enabled {
				continue
			}
			found := false
			for _, next := range after.Spec.Backup.Schedules {
				if old.Name == next.Name && next.Enabled {
					found = true
				}
			}
			disabled = disabled || !found
		}
		if disabled {
			add("backups-disabled", "Disables PITR or removes an enabled backup schedule.")
		}
	}
	if after.Spec.Provider == "rabbitmq" && after.Spec.Version == "4.3.5" {
		result.Warnings = append(result.Warnings, rabbitMQ435Warning)
	}
	return ok(result)
}

// A new range widens access unless an old range of the same family contains it.
func exposureWidens(before, after domain.Exposure) bool {
	if !after.Public {
		return false
	}
	if !before.Public {
		return true
	}
	if before.IPType != after.IPType {
		return true
	}
	for _, raw := range after.Allowlist {
		next, err := netip.ParsePrefix(raw)
		if err != nil {
			addr, e := netip.ParseAddr(raw)
			if e != nil {
				continue
			}
			next = netip.PrefixFrom(addr, addr.BitLen())
		}
		covered := false
		for _, oldRaw := range before.Allowlist {
			old, err := netip.ParsePrefix(oldRaw)
			if err != nil {
				addr, e := netip.ParseAddr(oldRaw)
				if e != nil {
					continue
				}
				old = netip.PrefixFrom(addr, addr.BitLen())
			}
			if old.Addr().BitLen() == next.Addr().BitLen() && old.Bits() <= next.Bits() && old.Contains(next.Addr()) {
				covered = true
				break
			}
		}
		if !covered {
			return true
		}
	}
	return false
}
