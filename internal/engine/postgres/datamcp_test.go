package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestDataMcpValidation(t *testing.T) {
	p := New(Options{})
	for _, scope := range []domain.DataMcpScope{{IndexPatterns: []string{"x"}}, {Vhosts: []string{"/"}}, {Schemas: []string{"pg_catalog"}}, {Schemas: []string{"information_schema"}}, {Databases: []string{""}}, {Schemas: []string{"a\x00b"}}} {
		c := &domain.Cluster{}
		c.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Scope: scope}
		if _, err := p.DataMcp(c); !errors.Is(err, engine.ErrInvalid) {
			t.Fatalf("accepted scope %+v: %v", scope, err)
		}
	}
	backend, err := p.DataMcp(&domain.Cluster{})
	if err != nil || backend.Kind != engine.DataMcpCompanion || backend.Upstream != "pgedge-postgres-mcp" {
		t.Fatal(backend, err)
	}
}

func TestDataMcpLifecycleAndCredentials(t *testing.T) {
	for _, proxy := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "proxy"}[proxy], func(t *testing.T) {
			topology := "single"
			if proxy {
				topology = "ha"
			}
			p, rc, _ := setup(t, topology)
			spec := rc.Cluster.Spec.Components.Extra["proxy"]
			spec.Replicas = 0
			if proxy {
				spec.Replicas = 1
			}
			rc.Cluster.Spec.Components.Extra["proxy"] = spec
			rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}
			s := converge(t, p, rc)
			for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
				cred, err := p.DataAccessCredentials(t.Context(), rc, level)
				if err != nil {
					t.Fatal(err)
				}
				if cred.Username != dataRoleName(level) || cred.Database != "postgres" || cred.Password == "" || !cred.TLS || len(cred.CAPEM) == 0 {
					t.Fatal("incomplete credentials")
				}
				u, err := url.Parse(cred.URI)
				if err != nil || u.Scheme != "postgresql" || u.Query().Get("sslmode") != "verify-full" {
					t.Fatal("bad URI", err)
				}
				attrs := "prefer-standby"
				if level == engine.DataAccessReadWrite {
					attrs = "read-write"
				}
				var hosts []string
				for _, m := range s.data() {
					if !m.Removing {
						hosts = append(hosts, m.Name+":5432")
					}
				}
				if !slices.Equal(cred.Hosts, hosts) || u.Host != strings.Join(hosts, ",") || u.Query().Get("target_session_attrs") != attrs {
					t.Fatalf("expected direct member routing with %s, got hosts %v and URI host %q, attributes %q", attrs, cred.Hosts, u.Host, u.Query().Get("target_session_attrs"))
				}
			}
			cfg, err := p.MemberConfig(t.Context(), rc, s.data()[0].Name)
			if err != nil {
				t.Fatal(err)
			}
			var sp pgspec.Spec
			if err = json.Unmarshal(cfg.EngineSpec, &sp); err != nil {
				t.Fatal(err)
			}
			if sp.DataAccess == nil || !sp.DataAccess.GlobalRead || len(sp.DataAccess.Roles) != 2 || sp.DataAccess.Roles[0].Settings["default_transaction_read_only"] != "on" {
				t.Fatal("role payload")
			}
			if proxy {
				cfg, err = p.MemberConfig(t.Context(), rc, "demopx")
				if err != nil {
					t.Fatal(err)
				}
				if err = json.Unmarshal(cfg.EngineSpec, &sp); err != nil {
					t.Fatal(err)
				}
				if sp.Proxy.ReaderPort {
					t.Fatal("Data MCP must not enable the proxy reader listener")
				}
				for _, name := range []string{"kk_mcp_ro", "kk_mcp_rw"} {
					if slices.ContainsFunc(sp.Proxy.Users, func(u pgspec.ProxyUser) bool { return u.Username == name }) {
						t.Fatal("Data MCP identity published to proxy")
					}
				}
			}
			rc.Cluster.Spec.DataMcp.Scope = domain.DataMcpScope{Databases: []string{"postgres"}, Schemas: []string{"application"}}
			roles, err := p.dataRoles(t.Context(), rc)
			if err != nil || roles.GlobalRead || !slices.Equal(roles.Schemas, []string{"application"}) {
				t.Fatal("scope not delivered", err)
			}
			rc.Cluster.Spec.DataMcp.Access = domain.DataMcpAccessRead
			converge(t, p, rc)
			if _, err = p.DataAccessCredentials(t.Context(), rc, engine.DataAccessReadWrite); !errors.Is(err, engine.ErrNotSupported) {
				t.Fatal("writer still enabled")
			}
			if _, err = secret(t.Context(), rc, "users/"+engine.DataAccessReadWriteUser); !missing(err) {
				t.Fatal("writer secret retained")
			}
			rc.Cluster.Spec.DataMcp.Enabled = false
			converge(t, p, rc)
			if _, err = secret(t.Context(), rc, "users/"+engine.DataAccessReadUser); !missing(err) {
				t.Fatal("reader secret retained")
			}
			roles, err = p.dataRoles(t.Context(), rc)
			if err != nil || roles.Roles[0].Enabled || roles.Roles[1].Enabled {
				t.Fatal("drop payload missing", err)
			}
		})
	}
}

func TestDataMcpRotationStaging(t *testing.T) {
	p, rc, _ := setup(t, "single")
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true}
	converge(t, p, rc)
	old, err := secret(t.Context(), rc, "users/"+engine.DataAccessReadUser)
	if err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "data-rotation", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"karakoram-mcp-ro"}`)}
	result, err := p.RunOperation(context.Background(), rc, op)
	if err != nil || result.Step != "password-staged" {
		t.Fatal(result, err)
	}
	staged, err := secret(t.Context(), rc, "rotations/"+op.ID+"/"+engine.DataAccessReadUser)
	if err != nil || string(staged) == string(old) {
		t.Fatal("password not staged", err)
	}
	result, err = p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	same, err := secret(t.Context(), rc, "rotations/"+op.ID+"/"+engine.DataAccessReadUser)
	if err != nil || string(same) != string(staged) {
		t.Fatal("replay changed staged secret")
	}
	op.Checkpoint = result.Checkpoint
	result, err = p.RunOperation(t.Context(), rc, op)
	if err != nil || result.Step != "reload" {
		t.Fatal(result, err)
	}
	active, err := secret(t.Context(), rc, "users/"+engine.DataAccessReadUser)
	if err != nil || string(active) != string(staged) {
		t.Fatal("staged password not promoted", err)
	}
	op.Checkpoint = result.Checkpoint
	for range 50 {
		result, err = p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			break
		}
	}
	if !result.Done {
		t.Fatal("rotation did not finish")
	}
	if _, err = secret(t.Context(), rc, "rotations/"+op.ID+"/"+engine.DataAccessReadUser); !missing(err) {
		t.Fatal("staging secret retained")
	}
}

func TestDataMcpTLSModesAndInvalidLevel(t *testing.T) {
	for _, mode := range []string{"requireTLS", "preferTLS", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			rc.Cluster.Spec.TLS.Mode = mode
			rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true}
			converge(t, p, rc)
			credentials, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessRead)
			if err != nil {
				t.Fatal(err)
			}
			uri, err := url.Parse(credentials.URI)
			if err != nil {
				t.Fatal(err)
			}
			wantTLS := mode != "disabled"
			if credentials.TLS != wantTLS || (len(credentials.CAPEM) > 0) != wantTLS {
				t.Fatal("TLS trust material does not match cluster policy")
			}
			wantMode := "disable"
			if wantTLS {
				wantMode = "verify-full"
			}
			if uri.Query().Get("sslmode") != wantMode {
				t.Fatal("wrong TLS URI mode")
			}
			if _, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessLevel("invalid")); !errors.Is(err, engine.ErrNotSupported) {
				t.Fatal("invalid access level accepted")
			}
		})
	}
}

func TestDataMcpCarriesApplicationRoleRevision(t *testing.T) {
	p, rc, _ := setup(t, "single")
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true}
	converge(t, p, rc)
	for _, revision := range []string{"first-owner", "second-owner"} {
		if err := putSecret(t.Context(), rc, "access/revision", []byte(revision)); err != nil {
			t.Fatal(err)
		}
		roles, err := p.dataRoles(t.Context(), rc)
		if err != nil || roles.Revision != revision {
			t.Fatal("application role catalog revision omitted from payload", err)
		}
	}
}

func TestDataMcpCredentialsExcludeRemovingMembers(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}
	s := converge(t, p, rc)
	members := s.data()
	members[0].Removing = true
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		credentials, err := p.DataAccessCredentials(t.Context(), rc, level)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{members[1].Name + ":5432", members[2].Name + ":5432"}
		if !slices.Equal(credentials.Hosts, want) {
			t.Fatalf("expected only active data members %v, got %v", want, credentials.Hosts)
		}
	}
	for _, m := range members {
		m.Removing = true
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessRead); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatalf("expected unavailable without active data members, got %v", err)
	}
}
