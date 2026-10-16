// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestRequiredCapabilities(t *testing.T) {
	p := New(Options{})
	for _, tt := range []struct {
		name    string
		options string
		want    []string
	}{
		{name: "default private"},
		{name: "explicit private", options: `{"managementUI":{"public":false}}`},
		{name: "public management", options: `{"managementUI":{"public":true}}`, want: []string{"publicHttps"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spec := domain.ClusterSpec{EngineOptions: json.RawMessage(tt.options)}
			if got := p.RequiredCapabilities(spec); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			err := engine.CheckCapabilities(p, spec, "limited", infra.Capabilities{})
			if len(tt.want) > 0 && !errors.Is(err, engine.ErrNotSupported) || len(tt.want) == 0 && err != nil {
				t.Fatal(err)
			}
			if err := engine.CheckCapabilities(p, spec, "capable", infra.Capabilities{PublicHTTPS: true}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
