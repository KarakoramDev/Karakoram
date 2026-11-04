// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"strings"
	"testing"
	"time"
)

type recoveryIntentBackend struct {
	*testBackend
	imports []infra.ServiceSpec
	prepare []string
}

func (b *recoveryIntentBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	b.imports = append(b.imports, specs...)
	return b.testBackend.ImportServices(ctx, specs)
}
func (b *recoveryIntentBackend) Deploy(ctx context.Context, name string, deployment infra.Deployment) (infra.Op, error) {
	b.prepare = append(b.prepare, deployment.Runtime.Prepare...)
	return b.testBackend.Deploy(ctx, name, deployment)
}

func TestMaintenanceVersionWaitRecoversAcceptedRuntimeIntent(t *testing.T) {
	p, rc, bindings, agents := fixture(t, "split")
	converge(t, p, rc)
	before, _ := loadState(rc.Cluster)
	m := before.Members[tspec.History]
	oldResources, oldReplicas := m.Resources, m.Replicas
	backend := &recoveryIntentBackend{testBackend: rc.Backend.(*testBackend)}
	rc.Backend = backend
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	rc.Cluster.Spec.Version = "1.32.1"
	desired := rc.Cluster.Spec.Components.Extra[tspec.History]
	desired.Resources.MinRAM++
	desired.Resources.MaxRAM++
	desired.Replicas++
	rc.Cluster.Spec.Components.Extra[tspec.History] = desired
	bindings.conn.Username = "rotated_owner"
	bindings.conn.CredentialGeneration++
	rc.Cluster.Generation++
	agents.unready = m.Name
	if _, err := backend.DeleteServiceByID(t.Context(), m.Name, m.ServiceID); err != nil {
		t.Fatal(err)
	}
	deployed := false
	for range 50 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, _ := loadState(rc.Cluster)
		if s.Members[tspec.History].Stage == engine.ComponentDeployed && s.Pending == nil {
			deployed = true
			break
		}
	}
	s, _ := loadState(rc.Cluster)
	recovered := s.Members[tspec.History]
	if !deployed || !recovered.Recovering || recovered.Resources != oldResources || recovered.Replicas != oldReplicas || recovered.Version != "1.31.4" {
		t.Fatal("runtime recovery consumed deferred deployment intent")
	}
	if len(backend.imports) != 1 || backend.imports[0].Resources != oldResources || backend.imports[0].MinContainers != oldReplicas {
		t.Fatal("recovery import used pending resources/replicas")
	}
	prepare := strings.Join(backend.prepare, "\n")
	if !strings.Contains(prepare, "1.31.4") || strings.Contains(prepare, "1.32.1") {
		t.Fatal("recovery prepared deferred server artifact")
	}
	config, err := p.MemberConfig(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	var spec tspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Version != "1.31.4" || spec.Persistence.Username != "rotated_owner" {
		t.Fatal("accepted recovery spec lost version or exempt credentials")
	}
}
