// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestCompletedCloneDoesNotResolveSourceForPITRStatus(t *testing.T) {
	p, rc, _, _ := setup(t)
	for _, source := range []*domain.DataSource{{Cluster: "missing", Backup: "baseline"}, {Cluster: "missing", PITRTime: new(time.Time)}} {
		rc.Cluster.Spec.DataSource = source
		windows, err := p.PITRWindow(t.Context(), rc)
		if err != nil || len(windows) != 0 {
			t.Fatal(windows, err)
		}
	}
}

func TestCreateSourceValidationNamesDependencies(t *testing.T) {
	p, rc, _, _ := setup(t)
	for _, source := range []*domain.DataSource{{Cluster: "missing", Backup: "baseline"}, {Cluster: "missing", PITRTime: new(time.Time)}} {
		rc.Cluster.Spec.DataSource = source
		err := p.Validate(t.Context(), nil, rc.Cluster)
		if err == nil {
			t.Fatal("missing source accepted")
		}
		for _, name := range []string{"missing", "default", source.Backup} {
			if !strings.Contains(err.Error(), name) {
				t.Fatal(err)
			}
		}
	}
}
