// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestPublicStackRequiresRequestAcknowledgementOnEnable(t *testing.T) {
	for _, component := range []string{"kibana", "apm"} {
		t.Run(component, func(t *testing.T) {
			p, rc, _, _ := coreSetup(t, 1)
			next := cloneCluster(t, rc.Cluster)
			next.Spec.EngineOptions = json.RawMessage(fmt.Sprintf(`{"%s":{"public":true}}`, component))
			for _, old := range []*domain.Cluster{nil, rc.Cluster} {
				err := p.Validate(t.Context(), old, next)
				var field *engine.ValidationError
				if !errors.Is(err, engine.ErrInvalid) || !errors.As(err, &field) || field.Field != "spec.engineOptions."+component+".allowAllAddresses" {
					t.Fatalf("public route without acknowledgement: %v", err)
				}
			}
			next.Spec.EngineOptions = json.RawMessage(fmt.Sprintf(`{"%s":{"public":true,"allowAllAddresses":true}}`, component))
			if err := p.Validate(t.Context(), nil, next); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(next.Spec.EngineOptions), "allowAllAddresses") {
				t.Fatal("acknowledgement persisted", string(next.Spec.EngineOptions))
			}
			unchanged := cloneCluster(t, next)
			if err := p.Validate(t.Context(), next, unchanged); err != nil {
				t.Fatal("unchanged route rejected", err)
			}
			private := cloneCluster(t, next)
			private.Spec.EngineOptions = json.RawMessage(fmt.Sprintf(`{"%s":{"public":false,"allowAllAddresses":true}}`, component))
			if err := p.Validate(t.Context(), next, private); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(private.Spec.EngineOptions), "allowAllAddresses") {
				t.Fatal("private route retained acknowledgement")
			}
			// A later merge patch cannot inherit the acknowledgement from the private base.
			reenabled := cloneCluster(t, private)
			reenabled.Spec.EngineOptions = json.RawMessage(fmt.Sprintf(`{"%s":{"public":true}}`, component))
			if err := p.Validate(t.Context(), private, reenabled); err == nil {
				t.Fatal("re-enabling inherited acknowledgement")
			}
		})
	}
}
