// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestRequiredCapabilities(t *testing.T) {
	p := New(ProviderOptions{})
	for _, tt := range []struct {
		name    string
		extra   map[string]domain.ComponentSpec
		options string
		want    []string
	}{
		{name: "private single"},
		{name: "data nodes are stateful", extra: map[string]domain.ComponentSpec{kindNode: {Replicas: 3}, kindMaster: {Replicas: 3}}},
		{name: "kibana replicas", extra: map[string]domain.ComponentSpec{kindKibana: {Replicas: 2}}, want: []string{"horizontalScaling"}},
		{name: "logstash replicas", extra: map[string]domain.ComponentSpec{kindLogstash: {Replicas: 2}}, want: []string{"horizontalScaling"}},
		{name: "apm replicas", extra: map[string]domain.ComponentSpec{kindAPM: {Replicas: 2}}, want: []string{"horizontalScaling"}},
		{name: "public kibana", options: `{"kibana":{"public":true}}`, want: []string{"publicHttps"}},
		{name: "public apm", options: `{"apm":{"public":true}}`, want: []string{"publicHttps"}},
		{name: "combined unique", extra: map[string]domain.ComponentSpec{kindKibana: {Replicas: 2}, kindAPM: {Replicas: 3}}, options: `{"kibana":{"public":true},"apm":{"public":true}}`, want: []string{"horizontalScaling", "publicHttps"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spec := domain.ClusterSpec{Components: domain.Components{Extra: tt.extra}, EngineOptions: json.RawMessage(tt.options)}
			if got := p.RequiredCapabilities(spec); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			if len(tt.want) > 0 {
				if err := engine.CheckCapabilities(p, spec, "limited", infra.Capabilities{}); !errors.Is(err, engine.ErrNotSupported) {
					t.Fatal(err)
				}
			}
		})
	}
}

type componentCapabilityInfra struct {
	*coreInfra
	caps        infra.Capabilities
	resourceErr error
}

func (f *componentCapabilityInfra) Capabilities() infra.Capabilities { return f.caps }
func (f *componentCapabilityInfra) SetResources(ctx context.Context, name string, resources infra.Resources) (infra.Op, error) {
	if f.resourceErr != nil {
		return infra.Op{}, f.resourceErr
	}
	return f.coreInfra.SetResources(ctx, name, resources)
}

func TestComponentHorizontalScalingGatesImportAndScale(t *testing.T) {
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		for _, existing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/existing=%v", kind, existing), func(t *testing.T) {
				p, rc, f, _ := coreSetup(t, 1)
				if existing {
					component := rc.Cluster.Spec.Components.Extra[kind]
					component.Replicas = 1
					rc.Cluster.Spec.Components.Extra[kind] = component
				}
				s := coreConverge(t, p, rc, f)
				component := rc.Cluster.Spec.Components.Extra[kind]
				component.Replicas = 2
				rc.Cluster.Spec.Components.Extra[kind] = component
				caps := f.Capabilities()
				caps.HorizontalScaling = false
				rc.Backend = &componentCapabilityInfra{coreInfra: f, caps: caps}
				before := f.mutations
				var def engine.ComponentDef
				for _, candidate := range stackComponents {
					if candidate.Name == kind {
						def = candidate
					}
				}
				if handled, err := p.reconcileComponent(t.Context(), rc, s, def); !handled || !errors.Is(err, engine.ErrNotSupported) {
					t.Fatalf("handled=%v err=%v", handled, err)
				}
				if existing {
					m := s.members(kind)[0]
					if err := p.issueAction(t.Context(), rc, s, &pending{Action: "containers", Member: m.Name, Containers: 2}); !errors.Is(err, engine.ErrNotSupported) {
						t.Fatal(err)
					}
				}
				if f.mutations != before || s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 {
					t.Fatal("unsupported scaling mutated infrastructure or scheduled retries")
				}
			})
		}
	}
}

func TestUnsupportedActionDoesNotScheduleRetry(t *testing.T) {
	for _, sentinel := range []error{infra.ErrUnsupported, engine.ErrNotSupported} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			p, rc, f, _ := coreSetup(t, 1)
			s := coreConverge(t, p, rc, f)
			m := s.Members[0]
			rc.Backend = &componentCapabilityInfra{coreInfra: f, caps: f.Capabilities(), resourceErr: fmt.Errorf("resize: %w", sentinel)}
			if err := p.issueAction(t.Context(), rc, s, &pending{Action: "resources", Member: m.Name, Resources: m.Resources}); !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
			if s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 {
				t.Fatal("unsupported action scheduled retries")
			}
		})
	}
}

func TestComponentReenableFinishesDeletion(t *testing.T) {
	for _, def := range stackComponents {
		for _, stage := range []string{"delete-volume", "removed"} {
			t.Run(def.Name+"/"+stage, func(t *testing.T) {
				p, rc, f, _ := coreSetup(t, 1)
				component := rc.Cluster.Spec.Components.Extra[def.Name]
				component.Replicas = 1
				rc.Cluster.Spec.Components.Extra[def.Name] = component
				s := coreConverge(t, p, rc, f)
				m := s.members(def.Name)[0]
				if err := p.deleteComponent(t.Context(), rc, s, m); err != nil {
					t.Fatal(err)
				}
				if _, err := p.observePending(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if f.services[m.Name] != nil {
					t.Fatal("runtime was not deleted")
				}
				m.Stage = stage
				before := len(f.imports)
				if handled, err := p.reconcileComponent(t.Context(), rc, s, def); !handled || err != nil {
					t.Fatalf("finish deletion: handled=%v err=%v", handled, err)
				}
				if s.find(m.Name) != nil || len(f.imports) != before {
					t.Fatal("old identity was not removed before re-import")
				}
				if token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name); token != nil || !missing(err) {
					t.Fatalf("old agent token retained: token present=%v err=%v", token != nil, err)
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				s = coreConverge(t, p, rc, f)
				if members := s.members(def.Name); len(members) != 1 || members[0].Stage != stageJoined || f.services[members[0].Name] == nil || len(f.imports) <= before {
					t.Fatal("re-enabled component did not re-import and become ready")
				}
			})
		}
	}
}
