package supabase

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

var _ engine.DataMcpProvider = (*Provider)(nil)

func TestDataMcpUsesDedicatedPostgresIdentity(t *testing.T) {
	p, rc, _ := setupProvider(t)
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}
	convergeProvider(t, p, rc)
	backend, err := p.DataMcp(rc.Cluster)
	if err != nil || backend.Kind != engine.DataMcpCompanion || backend.Upstream != "pgedge-postgres-mcp" {
		t.Fatal(backend, err)
	}
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		credentials, err := p.DataAccessCredentials(t.Context(), rc, level)
		if err != nil {
			t.Fatal(err)
		}
		expected := "kk_mcp_ro"
		if level == engine.DataAccessReadWrite {
			expected = "kk_mcp_rw"
		}
		if credentials.Username != expected || credentials.Password == "" || credentials.Database != "postgres" {
			t.Fatal("Supabase must use its dedicated, non-RLS-bypassing MCP identity")
		}
	}
}
