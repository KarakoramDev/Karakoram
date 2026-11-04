// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/certificates"
	"github.com/KarakoramDev/Karakoram/internal/cpbackup"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/monitoring"
)

func (s *Server) metricsClusterAllowed(ctx context.Context, ref domain.ClusterRef) bool {
	return (namespaceFilter(ctx) == "" || namespaceFilter(ctx) == domain.NormalizedNamespace(ref.Namespace)) && s.allowed(ctx, "monitoring", "read", clusterObject(ref.Namespace, ref.Name))
}

// Load all durable state before starting a text response. Storage failures must
// fail the scrape, not manufacture healthy zeroes or return a partial HTTP 200.
func (s *Server) stateMetrics(ctx context.Context) (monitoring.StateSnapshot, error) {
	var snapshot monitoring.StateSnapshot
	clusters, err := s.Store.ListClusters(ctx, namespaceFilter(ctx))
	if err != nil {
		return snapshot, err
	}
	authorized := make(map[domain.ClusterRef]bool)
	for _, cluster := range clusters {
		ref := domain.ClusterRef{Namespace: domain.NormalizedNamespace(cluster.Namespace), Name: cluster.Name}
		if !s.metricsClusterAllowed(ctx, ref) {
			continue
		}
		authorized[ref] = true
		state := monitoring.ClusterState{Cluster: cluster}
		state.Backups, err = s.Store.ListBackups(ctx, ref)
		if err != nil {
			return snapshot, err
		}
		state.Drills, err = s.Store.ListBackupDrills(ctx, ref)
		if err != nil {
			return snapshot, err
		}
		snapshot.Clusters = append(snapshot.Clusters, state)
	}
	// An explicit namespace filter excludes installation-wide series even for a
	// settings reader. They have no namespace and cannot belong to that selection.
	snapshot.Installation = namespaceFilter(ctx) == "" && s.allowed(ctx, "settings", "read", "*")
	if snapshot.Installation {
		policy, err := cpbackup.Policy(ctx, s.Store)
		if err != nil {
			return snapshot, err
		}
		snapshot.ControlPlaneBackupEnabled = policy.Enabled
		snapshot.ControlPlaneBackups, err = s.Store.ListControlPlaneBackups(ctx)
		if err != nil {
			return snapshot, err
		}
		snapshot.NotificationFailures, err = s.Store.NotificationFailedCounts(ctx)
		if err != nil {
			return snapshot, err
		}
		// Include configured channels before their first failure, and retain
		// historical failed counts for deleted channels without opening secrets.
		channels, err := s.Store.ListNotificationChannels(ctx)
		if err != nil {
			return snapshot, err
		}
		for _, channel := range channels {
			if _, exists := snapshot.NotificationFailures[channel.Name]; !exists {
				snapshot.NotificationFailures[channel.Name] = 0
			}
		}
	}
	if len(authorized) == 0 && !snapshot.Installation {
		return snapshot, nil
	}
	inventory, err := certificates.Inventory(ctx, s.Store)
	if err != nil {
		return snapshot, err
	}
	for _, cert := range inventory {
		if cert.Cluster == "" {
			if !snapshot.Installation {
				continue
			}
		} else if !authorized[domain.ClusterRef{Namespace: domain.NormalizedNamespace(cert.Namespace), Name: cert.Cluster}] {
			continue
		}
		snapshot.Certificates = append(snapshot.Certificates, monitoring.CertificateExpiry{Namespace: cert.Namespace, Cluster: cert.Cluster, Certificate: cert.Name, Expiry: cert.NotAfter})
	}
	return snapshot, nil
}
