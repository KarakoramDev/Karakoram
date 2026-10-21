package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestCompanionConfigRequiresPersistedControllerIdentity(t *testing.T) {
	s, _, _ := testServer(t)
	ref := domain.ClusterRef{Name: "prod"}
	if err := s.Store.SetAgentToken(t.Context(), &domain.AgentToken{Cluster: ref.Name, Member: "prodmcp", Token: "companion-token"}); err != nil {
		t.Fatal(err)
	}
	cfg := agentapi.MemberConfig{Cluster: "prod", Member: "prodmcp", Engine: "datamcp", ConfigVersion: 17}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Store.SetClusterSecret(t.Context(), ref, "datamcp/config", raw); err != nil {
		t.Fatal(err)
	}
	request := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/internal/agent/v1/config?cluster=other&member=othermcp", nil)
		r.Header.Set("Authorization", "Bearer companion-token")
		w := httptest.NewRecorder()
		s.InternalHandler().ServeHTTP(w, r)
		return w
	}
	if w := request(); w.Code == 200 {
		t.Fatal("companion suffix alone authorized config")
	}
	if err = s.Store.SetClusterSecret(t.Context(), ref, "datamcp/state", []byte(`{"stage":"ready","id":"service-id"}`)); err != nil {
		t.Fatal(err)
	}
	w := request()
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"engine":"datamcp"`) || !strings.Contains(w.Body.String(), `"member":"prodmcp"`) || !strings.Contains(w.Body.String(), `"configVersion":17`) {
		t.Fatal(w.Body.String())
	}
}
