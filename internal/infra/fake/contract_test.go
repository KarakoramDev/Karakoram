// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package fake_test

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/contracttest"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestContract(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options fake.Options
	}{
		{"immediate", fake.Options{}},
		{"polled", fake.Options{Polls: 2}},
		{"masked", fake.Options{MaskSecrets: true}},
		{"restricted", fake.Options{Capabilities: infra.Capabilities{VolumeQuota: true}}},
		{"placement", fake.Options{Capabilities: infra.Capabilities{Placement: true, VolumeQuota: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contracttest.Run(t, func(t *testing.T) contracttest.Harness {
				return contracttest.Harness{Backend: fake.New(tc.options), Hostname: func(suffix string) string { return "contract" + suffix }, Artifact: func() infra.Artifact {
					return infra.Artifact{Reader: strings.NewReader("archive"), Name: "karakoram.tar.gz", Binary: "karakoram"}
				}}
			})
		})
	}
}
