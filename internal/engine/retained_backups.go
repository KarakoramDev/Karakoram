// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// RetainedBackupDeletionPreflight checks deletion without mutating remote or
// local state. Deleters must repeat safety checks immediately before mutation.
type RetainedBackupDeletionPreflight interface {
	ValidateRetainedBackupDeletion(context.Context, domain.BackupStorage, domain.Backup) error
}

// RetainedBackupDeletionRefusal guarantees this attempt made no destructive
// changes. Ordinary errors (including ErrConflict) carry no such guarantee.
// Err should be a ResourceError when the refusal has a public explanation.
type RetainedBackupDeletionRefusal struct {
	Err error
}

func (e *RetainedBackupDeletionRefusal) Error() string { return e.Err.Error() }
func (e *RetainedBackupDeletionRefusal) Unwrap() error { return e.Err }
