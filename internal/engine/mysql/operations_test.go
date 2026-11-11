// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestOperationClaimWaitReasonPersistedMaintenance(t *testing.T) {
	tests := []struct {
		name  string
		state *state
		want  string
	}{
		{"replacement", &state{Formation: formation{State: "formed"}, Replacement: &replacementState{Operation: "replacement-owner", Stage: "retire"}}, "Waiting for member replacement replacement-owner (stage retire) to finish"},
		{"rolling", &state{Formation: formation{State: "formed"}, Rolling: &rollingState{Stage: "rejoin"}}, "Waiting for rolling maintenance (stage rejoin) to finish"},
		{"scale", &state{Formation: formation{State: "formed"}, Pending: &pending{Action: "scale"}}, "Waiting for member deployment or scale maintenance to finish"},
		{"tls", &state{Formation: formation{State: "formed"}, TLSRotation: &tlsRotationState{Operation: "tls-owner", Stage: "activate"}}, "Waiting for TLS rotation tls-owner (stage activate) to finish"},
		{"galera-tls", &state{Formation: formation{State: "formed"}, GaleraTLS: &galeraTLSRotation{Stage: "restart"}}, "Waiting for Galera TLS rotation (stage restart) to finish"},
		{"ready", &state{Formation: formation{State: "formed"}}, ""},
		{"rolling-backup", &state{Formation: formation{State: "formed"}, Rolling: &rollingState{Stage: "backup"}}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &domain.Cluster{}
			if err := save(c, tt.state); err != nil {
				t.Fatal(err)
			}
			p := &Core{}
			allowed, err := p.CanClaimOperation(c)
			if err != nil || allowed != (tt.want == "") {
				t.Fatalf("claim allowed=%v err=%v", allowed, err)
			}
			reason, err := p.OperationClaimWaitReason(c)
			if err != nil || reason != tt.want {
				t.Fatalf("reason=%q want=%q err=%v", reason, tt.want, err)
			}
		})
	}
}
