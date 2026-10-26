// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNamespaceMutationsCancelWhileLifecycleLockHeld(t *testing.T) {
	for _, method := range []string{"update", "rotate", "validate", "delete", "create"} {
		t.Run(method, func(t *testing.T) {
			r, _, _ := setupRegistry(t)
			n := createTestNamespace(t, r)
			lock := r.lock(n.Name)
			lock.Lock()
			unlocked := false
			defer func() {
				if !unlocked {
					lock.Unlock()
				}
			}()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				switch method {
				case "update":
					_, err = r.Update(ctx, n.Name, "changed")
				case "rotate":
					_, err = r.RotateToken(ctx, n.Name, "replacement")
				case "validate":
					_, err = r.Validate(ctx, n.Name)
				case "delete":
					err = r.Delete(ctx, n.Name)
				case "create":
					_, err = r.Create(ctx, n.Name, "another-project", "valid-token", "", "zerops", nil)
				}
				done <- err
			}()
			select {
			case <-done:
				t.Fatal("mutation bypassed held lifecycle lock")
			case <-time.After(10 * time.Millisecond):
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation not returned: %v", err)
				}
			case <-time.After(100 * time.Millisecond):
				lock.Unlock()
				unlocked = true
				<-done
				t.Error("cancelled request blocked on lifecycle lock")
			}
		})
	}
}
