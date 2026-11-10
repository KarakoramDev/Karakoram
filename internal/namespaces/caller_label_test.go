// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

func TestNamespaceRunAttributesValidation(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n, err := r.o.Store.GetNamespaceForUpdate(t.Context(), "default")
	if err != nil {
		t.Fatal(err)
	}
	n.Token.LastValidatedAt = time.Time{}
	if err = r.o.Store.UpdateNamespace(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	callers := make(chan string, 1)
	registryFactory(r.o.Backends).validate = func(ctx context.Context, _ infra.Target) (Backend, infra.Project, error) {
		callers <- infra.RequestCaller(ctx)
		cancel()
		return b, infra.Project{}, nil
	}
	_ = r.Run(ctx)
	select {
	case caller := <-callers:
		if caller != infra.CallerNamespace {
			t.Fatal("namespace validation caller", caller)
		}
	default:
		t.Fatal("automatic namespace validation did not run")
	}
}
