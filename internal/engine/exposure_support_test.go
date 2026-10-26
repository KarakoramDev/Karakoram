// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type exposureSupportBackend struct {
	infra.Backend
	supported bool
	err       error
}

func (b exposureSupportBackend) SupportsPublicIP(context.Context, string) (bool, error) {
	return b.supported, b.err
}

func TestExposureSupport(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backend infra.Backend
		public  bool
		status  string
		invalid bool
	}{
		{"unsupported", exposureSupportBackend{}, true, "False", true},
		{"supported", exposureSupportBackend{supported: true}, true, "True", false},
		{"lookup failure", exposureSupportBackend{err: errors.New("unavailable")}, true, "Unknown", false},
		{"unknown backend", nil, true, "Unknown", false},
		{"private", exposureSupportBackend{}, false, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			condition, err := CheckExposureSupport(t.Context(), tc.backend, domain.Exposure{Public: tc.public, IPType: "IPV4"})
			if condition.Status != tc.status || errors.Is(err, ErrInvalid) != tc.invalid {
				t.Fatalf("condition=%+v err=%v", condition, err)
			}
		})
	}
}
