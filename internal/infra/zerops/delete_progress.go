// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Deletion supersedes deployment intent, not the platform's service process
// queue. Observe an accepted delete without consuming its deployment receipt,
// acquiring its deploy lock, cancelling running work, or submitting it again.
func (b *Backend) deleteProgress(ctx context.Context, op infra.Op, p Process) (string, error) {
	message := fmt.Sprintf("waiting for Zerops delete process %s for service %s: %s%s", op.ID, op.ServiceID, p.Status, processAge(p))
	if p.Status != processPending && p.Status != processWaiting {
		return boundedDiagnostic(message), nil
	}
	ctx, cancel := context.WithTimeout(ctx, b.waitTimeout)
	defer cancel()
	processes, err := getPages[Process](ctx, b.client, "/service-stack/"+escape(op.ServiceID)+"/process")
	if err != nil {
		// Missing diagnostic evidence cannot retire an accepted deletion or
		// justify retrying it. Preserve the observation error so reconciliation
		// honors platform Retry-After and failure backoff before any more reads.
		message = boundedDiagnostic(message + "; same-service process inventory unavailable; retaining the accepted deletion")
		return message, fmt.Errorf("%s: %w", message, err)
	}
	for _, other := range processes {
		if other.ID == "" || other.ID == p.ID || other.Status != processRunning ||
			other.ServiceStackID != "" && other.ServiceStackID != op.ServiceID ||
			other.ProjectID != "" && other.ProjectID != b.client.project {
			continue
		}
		message += fmt.Sprintf("; same-service platform process %s (%s) RUNNING%s; platform serializes service processes", other.ID, other.ActionName, processAge(other))
		break
	}
	return boundedDiagnostic(message + "; deletion already submitted, not replayed"), nil
}

func processAge(p Process) string {
	at := p.Created
	if at.IsZero() {
		at = p.Started
	}
	if at.IsZero() {
		return " (age unavailable from platform)"
	}
	return fmt.Sprintf(" (age %s)", max(time.Duration(0), time.Since(at)).Round(time.Second))
}
