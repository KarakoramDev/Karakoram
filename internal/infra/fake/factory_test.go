// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package fake_test

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestFactoryValidationAndEnvironment(t *testing.T) {
	f := fake.NewFactory(fake.Options{Name: "memory", Project: infra.Project{ID: "target", Name: "Target"}})
	if f.Info().Kind != "memory" {
		t.Fatal(f.Info())
	}
	target, err := f.TargetFromEnv(func(key string) string {
		return map[string]string{"KARAKORAM_FAKE_TARGET": "target", "KARAKORAM_FAKE_TOKEN": "token"}[key]
	})
	if err != nil || target.Kind != "memory" || target.ID != "target" || target.Token != "token" {
		t.Fatalf("target: %+v %v", target, err)
	}
	for _, invalid := range []infra.Target{{ID: "target"}, {ID: "other", Token: "token"}} {
		if _, _, err := f.Validate(t.Context(), invalid); !errors.Is(err, infra.ErrUnauthorized) {
			t.Fatalf("Validate: %v", err)
		}
	}
	b, p, err := f.Validate(t.Context(), target)
	if err != nil || p.ID != "target" || b.Name() != "memory" {
		t.Fatalf("Validate: %+v %v", p, err)
	}
}
func TestFactorySharesTargetsAndIsolatesFactories(t *testing.T) {
	f := fake.NewFactory(fake.Options{Project: infra.Project{ID: "target"}})
	first, err := f.New(infra.Target{ID: "target", Token: "one"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.New(infra.Target{ID: "target", Token: "two"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.ImportServices(t.Context(), []infra.ServiceSpec{runtime("runtime", "")}); err != nil {
		t.Fatal(err)
	}
	if s, err := second.GetService(t.Context(), "runtime"); err != nil || s == nil {
		t.Fatalf("shared state: %+v %v", s, err)
	}
	other, err := f.New(infra.Target{ID: "other"})
	if err != nil {
		t.Fatal(err)
	}
	if s, err := other.GetService(t.Context(), "runtime"); err != nil || s != nil {
		t.Fatalf("targets not isolated: %+v %v", s, err)
	}
	isolated, err := fake.NewFactory(fake.Options{}).New(infra.Target{ID: "target"})
	if err != nil {
		t.Fatal(err)
	}
	if s, err := isolated.GetService(t.Context(), "runtime"); err != nil || s != nil {
		t.Fatalf("factories not isolated: %+v %v", s, err)
	}
	if _, err := (fake.Factory{}).New(infra.Target{ID: "default"}); err != nil {
		t.Fatal(err)
	}
}
