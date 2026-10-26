// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestDefinitionsMissingStoredSourceFailsRestoreAndClone(t *testing.T) {
	for _, clone := range []bool{false, true} {
		f := newDefinitionsFixture(t)
		seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
		f.s3.objects = map[string][]byte{}
		if clone {
			f.rc.Cluster.Spec.DataSource = &domain.DataSource{Backup: "daily"}
			_, err := f.p.cloneStep(t.Context(), f.rc)
			var field *engine.ValidationError
			if !errors.As(err, &field) || !strings.Contains(err.Error(), "no longer available") {
				t.Fatalf("clone did not reject missing object: %v", err)
			}
			continue
		}
		op := prepareDefinitionsRestore(t, f)
		advanceDefinitions(t, f, op, f.p.restoreOp)
		advanceDefinitions(t, f, op, f.p.restoreOp)
		result, err := f.p.restoreOp(t.Context(), f.rc, op)
		if err == nil || !result.Done || f.store.restore.Phase != domain.PhaseFailed || f.store.restore.CompletedAt == nil || !strings.Contains(err.Error(), "no longer available") || f.imports != 0 {
			t.Fatalf("missing source did not fail terminally: %+v %v %+v", result, err, f.store.restore)
		}
	}
}

func TestDefinitionsStorageDenialDoesNotProveAbsence(t *testing.T) {
	f := newDefinitionsFixture(t)
	seedDefinitions(t, f, definitionsGzip(t, definitionsFixture))
	f.s3.readStatus = 403
	op := prepareDefinitionsRestore(t, f)
	advanceDefinitions(t, f, op, f.p.restoreOp)
	result, err := f.p.restoreOp(t.Context(), f.rc, op)
	if err == nil || result.Done || result.Step != "import" || f.store.restore.Phase == domain.PhaseFailed {
		t.Fatalf("storage denial was treated as authoritative absence: %+v %v", result, err)
	}
}
