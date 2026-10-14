// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// ValidatePITRTarget accepts only points in continuous, backed recovery coverage.
// Bounds alone are insufficient: a target may lie in a gap between windows.
func ValidatePITRTarget(windows []domain.TimeWindow, target, now time.Time) error {
	if !target.After(now) {
		for _, w := range windows {
			if !target.Before(w.Start) && !target.After(w.End) {
				return nil
			}
		}
	}
	err := domain.InvalidField("source.pitrTime", domain.ReasonOutOfRange,
		"source.pitrTime must be within a continuous restorable PITR window and must not be in the future")
	err.Err = engine.ErrInvalid
	return err
}
