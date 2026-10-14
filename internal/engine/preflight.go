// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// PITRRestoreValidator optionally validates a source cluster's current recovery
// coverage before accepting a restore. Windows are returned even on a rejected
// target so callers can show the current bounds. No coverage is an invalid-state
// conflict; a target outside continuous coverage is a field error.
type PITRRestoreValidator interface {
	ValidatePITRRestore(context.Context, *Context, time.Time) ([]domain.TimeWindow, error)
}

// FCVValidator optionally checks the accepted version form and deployed members
// before accepting feature compatibility promotion.
type FCVValidator interface {
	ValidateFCV(context.Context, *Context, string) error
}

// DatabasePasswordValidator optionally enforces engine authentication rules.
// It validates supplied passwords only; omission retains/generates a password.
type DatabasePasswordValidator interface {
	ValidateDatabasePassword(string) error
}
