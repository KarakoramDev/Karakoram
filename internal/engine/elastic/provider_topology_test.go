// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestDedicatedMasterComponentRequiresDedicatedTopology(t *testing.T) {
	p := New(ProviderOptions{})
	for _, count := range []int{1, 3, 5} {
		c := &domain.Cluster{Name: "search"}
		p.ApplyDefaults(&c.Spec)
		resources, _ := DefaultResources("master")
		c.Spec.Components.Extra["master"] = domain.ComponentSpec{Replicas: count, Resources: resources}
		err := p.Validate(t.Context(), nil, c)
		var validation *engine.ValidationError
		if !errors.As(err, &validation) || validation.Field != "spec.components.extra.master" || validation.Reason != domain.ReasonUnsupported || !strings.Contains(validation.Message, "dedicated masters require the dedicatedMasters topology") {
			t.Fatalf("master count %d: expected topology diagnostic, got %v", count, err)
		}
	}
}
