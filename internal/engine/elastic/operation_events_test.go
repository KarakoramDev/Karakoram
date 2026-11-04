// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type backupEventRecorder struct{ event domain.Event }

func (r *backupEventRecorder) Record(_ context.Context, event domain.Event) error {
	r.event = event
	return nil
}

func TestBackupFailureEventSeverity(t *testing.T) {
	recorder := &backupEventRecorder{}
	rc := &engine.Context{Cluster: &domain.Cluster{Namespace: "default", Name: "db"}, Events: recorder}
	for _, tc := range []struct{ reason, kind string }{{"BackupFailed", "Warning"}, {"BackupCompleted", "Normal"}} {
		if err := operationEvent(t.Context(), rc, &domain.Operation{ID: "operation"}, tc.reason, "backup result"); err != nil {
			t.Fatal(err)
		}
		if recorder.event.Type != tc.kind || recorder.event.Reason != tc.reason || recorder.event.Operation != "operation" || recorder.event.Namespace != "default" || recorder.event.Cluster != "db" {
			t.Fatal(recorder.event)
		}
	}
}
