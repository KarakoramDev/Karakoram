// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"testing"
)

func TestStatusAdvertisesHealthCapabilitiesBeforeSampling(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	status := New().status(ctx, testSpec(), dataPaths(t.TempDir()))
	if status.Running || status.SlotStatusKnown || status.ArchiveStatusKnown || status.WALStatusKnown {
		t.Fatal("cancelled native probes unexpectedly returned healthy observations")
	}
	capabilities := status.HealthCapabilities
	if capabilities == nil || !capabilities.LocalEtcd || !capabilities.ReplicationSlots || !capabilities.WALArchive || !capabilities.ReplicationDistance || !capabilities.PatroniPause {
		t.Fatalf("failed sample erased implemented health capabilities: %+v", capabilities)
	}
	if !status.CredentialReceipts {
		t.Fatal("native credential replay capability is not advertised")
	}
}
