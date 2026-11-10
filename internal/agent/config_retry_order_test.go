// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"sync"
	"testing"
)

// Done is evaluated at mutex admission, before select can acquire the lock.
// Pausing there also catches a regression that selects the old retry first:
// B applies while that old retry is deterministically paused before admission.
type retryAdmissionContext struct {
	context.Context
	once    sync.Once
	entered chan struct{}
	proceed <-chan struct{}
}

func (c *retryAdmissionContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.entered)
		<-c.proceed
	})
	return c.Context.Done()
}

func TestEqualVersionRetryCannotOverwriteReload(t *testing.T) {
	a, old, _ := fakeNode(t, "/bin/true")
	old.PullRevision = "old"
	old.ConfigVersion = 1
	old.DesiredProcesses = []string{}
	a.applyRetry = &old
	if err := a.reloadMu.LockContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	entered, proceed, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	admission := &retryAdmissionContext{Context: t.Context(), entered: entered, proceed: proceed}
	go func() { _, err := a.retryApply(admission); done <- err }()
	<-entered
	newer := old
	newer.PullRevision = "new"
	newer.EngineSpec = json.RawMessage(`{"port":1235}`)
	// The network reload owns reloadMu; retry must not capture pending until
	// that ordering owner has applied and published the equal-version update.
	if _, err := a.apply(t.Context(), newer); err != nil {
		close(proceed)
		a.reloadMu.Unlock()
		t.Fatal(err)
	}
	close(proceed)
	a.reloadMu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if a.config().PullRevision != "new" || a.pullRevision != "new" || a.applyRetry != nil {
		t.Fatal("superseded retry overwrote applied config", a.config().PullRevision, a.pullRevision)
	}
	failed := newer
	failed.PullRevision = "failed"
	a.applyRetry = &failed
	a.engine.(*fakeEngine).onApply = func(*Node, agentapi.MemberConfig) error {
		return &RetryableApplyError{Err: errors.New("retry refused")}
	}
	if _, err := a.retryApply(t.Context()); err == nil || a.pullRevision != "" {
		t.Fatal("failed retry retained advertised revision", err, a.pullRevision)
	}
}
