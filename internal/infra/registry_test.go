// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestRegistryKindsAndOrdering(t *testing.T) {
	a := fake.NewFactory(fake.Options{Name: "alpha"})
	z := fake.NewFactory(fake.Options{Name: "zulu"})
	registry, err := infra.NewRegistry(z, a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := infra.NewRegistry(a, a); err == nil {
		t.Fatal("duplicate factory accepted")
	}
	if err := registry.Register(a); err == nil {
		t.Fatal("duplicate registration accepted")
	}
	if err := registry.Register(nil); err == nil {
		t.Fatal("nil factory accepted")
	}
	infos := registry.List()
	kinds := make([]string, len(infos))
	for i, info := range infos {
		kinds[i] = info.Kind
	}
	if !reflect.DeepEqual(kinds, []string{"alpha", "zulu"}) {
		t.Fatal(kinds)
	}
	if f, err := registry.Get("alpha"); err != nil || f != a {
		t.Fatalf("Get: %v %v", f, err)
	}
	if _, err := registry.Get("missing"); !errors.Is(err, infra.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := registry.New(infra.Target{Kind: "missing"}); !errors.Is(err, infra.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, _, err := registry.Validate(t.Context(), infra.Target{Kind: "missing"}); !errors.Is(err, infra.ErrUnsupported) {
		t.Fatal(err)
	}
	var zero infra.Registry
	if err := zero.Register(a); err != nil {
		t.Fatal(err)
	}
	if len(zero.List()) != 1 {
		t.Fatal("zero registry failed registration")
	}
}
func TestRegistryConfigAndValidation(t *testing.T) {
	f := fake.NewFactory(fake.Options{Project: infra.Project{ID: "target"}})
	registry, err := infra.NewRegistry(f)
	if err != nil {
		t.Fatal(err)
	}
	target := infra.Target{Kind: "fake", ID: "target", Token: "credential"}
	b, err := registry.New(target)
	if err != nil || b.Name() != "fake" {
		t.Fatalf("New: %v %v", b, err)
	}
	b, p, err := registry.Validate(t.Context(), target)
	if err != nil || p.ID != "target" || b.Name() != "fake" {
		t.Fatalf("Validate: %v %+v %v", b, p, err)
	}
	target.Config = map[string]string{"unknown": "value"}
	check := func(err error) {
		t.Helper()
		var rejected *infra.RejectedError
		if !errors.Is(err, infra.ErrRejected) || !errors.As(err, &rejected) || rejected.Code != "unknownConfigKey" {
			t.Fatalf("unknown key: %v", err)
		}
	}
	_, err = registry.New(target)
	check(err)
	_, _, err = registry.Validate(t.Context(), target)
	check(err)
	target.Config = nil
	target.Token = ""
	if _, _, err := registry.Validate(t.Context(), target); !errors.Is(err, infra.ErrUnauthorized) {
		t.Fatal(err)
	}
}
