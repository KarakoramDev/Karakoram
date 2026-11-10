// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

type configNoticeNoReads struct{ infra.Backend }

func (configNoticeNoReads) GetService(context.Context, string) (*infra.Service, error) {
	return nil, fmt.Errorf("notification unexpectedly fetched a live peer")
}
func TestConfigNoticeContentIgnoresObservationJournals(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	rc.Backend = configNoticeNoReads{rc.Backend}
	before, err := p.MemberConfigNoticeContent(t.Context(), rc, "demomx")
	if err != nil || before == "" {
		t.Fatal(before, err)
	}
	for tick := range 20 {
		var internal map[string]json.RawMessage
		if err := json.Unmarshal(rc.Cluster.Internal, &internal); err != nil {
			t.Fatal(err)
		}
		internal["observationJournal"] = json.RawMessage(fmt.Sprint(tick))
		rc.Cluster.Internal, _ = json.Marshal(internal)
		rc.Cluster.Status.Phase = domain.PhaseRunning
		if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), "mysql/gtid/history", []byte(fmt.Sprint(tick))); err != nil {
			t.Fatal(err)
		}
		got, err := p.MemberConfigNoticeContent(t.Context(), rc, "demomx")
		if err != nil || got != before {
			t.Fatal("observation-only changes churned rendered config content", tick, err)
		}
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.find("demomx").ConfigVersion++
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	changed, err := p.MemberConfigNoticeContent(t.Context(), rc, "demomx")
	if err != nil || changed == before {
		t.Fatal("real config change did not change notice content", err)
	}
}
