// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestProvisioningRejectsForeignVolumeBeforeRecordingID(t *testing.T) {
	p, rc, b := setup(t, "single")
	foreign := &infra.Service{ID: "foreign-volume-id", Hostname: "demom0v", Kind: infra.KindVolume, Ownership: map[string]string{"foreign": "owner"}}
	b.services[foreign.Hostname] = foreign
	for range 30 {
		_, err := p.Sync(t.Context(), rc)
		if err != nil {
			if !errors.Is(err, engine.ErrConflict) {
				t.Fatalf("ownership failure: %v", err)
			}
			s, loadErr := load(rc.Cluster)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if m := s.find("demom0"); m != nil && m.VolumeID != "" {
				t.Fatalf("foreign volume identity recorded before verification: %q", m.VolumeID)
			}
			if b.services[foreign.Hostname] != foreign || len(foreign.Ownership) != 1 || foreign.Ownership["foreign"] != "owner" || len(b.deployed) != 0 {
				t.Fatal("foreign volume was modified or mounted")
			}
			return
		}
	}
	t.Fatal("foreign volume was not rejected")
}
