// Package datamcp supervises private, authenticated data MCP upstreams.
package datamcp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"gopkg.in/yaml.v3"
)

func processName(level string) string {
	switch level {
	case "read":
		return companionspec.ProcessRead
	case "readWrite":
		return companionspec.ProcessReadWrite
	}
	return ""
}

func validate(s companionspec.Spec) error {
	if s.Upstream != companionspec.UpstreamMongoDB && s.Upstream != companionspec.UpstreamPostgres {
		return errors.New("unsupported data MCP upstream")
	}
	if !filepath.IsAbs(s.InstallDir) || s.Version == "" {
		return errors.New("data MCP installation and version are required")
	}
	names, ports := map[string]bool{}, map[int]bool{}
	for _, p := range s.Processes {
		name := processName(p.Level)
		if name == "" || names[name] || p.Port < 1 || p.Port > 65535 || ports[p.Port] {
			return errors.New("invalid or duplicate data MCP process")
		}
		names[name], ports[p.Port] = true, true
		if p.MaxDocuments < 0 || p.MaxTimeMS < 0 {
			return errors.New("negative data MCP limit")
		}
		if s.Upstream == companionspec.UpstreamMongoDB && p.URI == "" {
			return errors.New("MongoDB connection URI required")
		}
		if s.Upstream == companionspec.UpstreamPostgres && (len(p.Hosts) == 0 || p.Username == "" || p.Password == "" || p.Database == "") {
			return errors.New("PostgreSQL credentials required")
		}
		if p.TLS && p.CAPEM == "" {
			return errors.New("database TLS CA required")
		}
	}
	return nil
}

// Render returns an executable specification and protected files relative to the
// agent's karakoram directory. The local token never appears in arguments.
func Render(s companionspec.Spec, p companionspec.Process, dataDir, token string) (agent.ProcessSpec, map[string][]byte, error) {
	if err := validate(s); err != nil {
		return agent.ProcessSpec{}, nil, err
	}
	if processName(p.Level) == "" || token == "" {
		return agent.ProcessSpec{}, nil, errors.New("process and local token required")
	}
	rel := filepath.Join("datamcp", processName(p.Level))
	base := filepath.Join(dataDir, "karakoram", rel)
	files := map[string][]byte{}
	files[filepath.Join(rel, "local-secret")] = []byte(token)
	if p.CAPEM != "" {
		files[filepath.Join(rel, "ca.pem")] = []byte(p.CAPEM)
	}
	spec := agent.ProcessSpec{Dir: base}
	if s.Upstream == companionspec.UpstreamMongoDB {
		headers, _ := json.Marshal(map[string]string{"Authorization": "Bearer " + token})
		documents, timeout := p.MaxDocuments, p.MaxTimeMS
		if documents == 0 {
			documents = 100
		}
		if timeout == 0 {
			timeout = 10000
		}
		uri := p.URI
		if p.TLS {
			sep := "?"
			if strings.Contains(uri, "?") {
				sep = "&"
			}
			uri += sep + "tlsCAFile=" + url.QueryEscape(filepath.Join(base, "ca.pem"))
		}
		spec.Binary = filepath.Join(s.InstallDir, "bin", "start")
		spec.Env = []string{"MDB_MCP_TRANSPORT=http", "MDB_MCP_HTTP_HOST=127.0.0.1", "MDB_MCP_HTTP_PORT=" + strconv.Itoa(p.Port), "MDB_MCP_HTTP_HEADERS=" + string(headers), "MDB_MCP_CONNECTION_STRING=" + uri, "MDB_MCP_READ_ONLY=" + strconv.FormatBool(p.Level == "read"), "MDB_MCP_DISABLED_TOOLS=connect,atlas,atlas-local-connect-deployment,atlas-local-create-deployment,atlas-local-delete-deployment,atlas-local-list-deployments,list-knowledge-sources,search-knowledge,export", "MDB_MCP_TELEMETRY=disabled", "DO_NOT_TRACK=1", "MDB_MCP_LOGGERS=stderr", "MDB_MCP_ALLOW_REQUEST_OVERRIDES=false", "MDB_MCP_DANGEROUS_HOST_BINDING=false", "MDB_MCP_MAX_DOCUMENTS_PER_QUERY=" + strconv.Itoa(documents), "MDB_MCP_MAX_TIME_M_S=" + strconv.Itoa(timeout), "MDB_MCP_DISABLE_SERVER_SIDE_JS=true", "HOME=" + base, "MDB_MCP_EXPORTS_PATH=" + filepath.Join(base, "exports")}
		return spec, files, nil
	}
	hash := sha256.Sum256([]byte(token))
	tokens := map[string]any{"tokens": map[string]any{"karakoram-local": map[string]any{"hash": hex.EncodeToString(hash[:]), "annotation": "private agent upstream", "database": "cluster", "expires_at": nil, "created_at": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)}}}
	files[filepath.Join(rel, "tokens.yaml")], _ = yaml.Marshal(tokens)
	hosts, err := postgresHosts(p.Hosts)
	if err != nil {
		return spec, nil, err
	}
	// Reader endpoints may target a standby. "read-write" also rejects a
	// primary when the read identity has default_transaction_read_only enabled.
	targetSessionAttrs := "any"
	if p.Level == "readWrite" {
		targetSessionAttrs = "primary"
	}
	db := map[string]any{"name": "cluster", "hosts": hosts, "target_session_attrs": targetSessionAttrs, "database": p.Database, "user": p.Username, "password": p.Password, "sslmode": "disable", "allow_writes": p.Level == "readWrite", "allow_llm_switching": false}
	if p.TLS {
		db["sslmode"] = "verify-full"
		db["sslrootcert"] = filepath.Join(base, "ca.pem")
	}
	config := map[string]any{"http": map[string]any{"enabled": true, "address": fmt.Sprintf("127.0.0.1:%d", p.Port), "tls": map[string]any{"enabled": false}, "auth": map[string]any{"enabled": true, "token_file": filepath.Join(base, "tokens.yaml")}}, "databases": []any{db}, "llm": map[string]any{"enabled": false}, "embedding": map[string]any{"enabled": false}, "knowledgebase": map[string]any{"enabled": false}, "builtins": map[string]any{"tools": map[string]bool{"similarity_search": false, "generate_embedding": false, "search_knowledgebase": false, "llm_connection_selection": false}}, "secret_file": filepath.Join(base, "encryption-secret"), "data_dir": filepath.Join(base, "data")}
	files[filepath.Join(rel, "config.yaml")], err = yaml.Marshal(config)
	if err != nil {
		return spec, nil, err
	}
	spec.Binary = filepath.Join(s.InstallDir, "pgedge-postgres-mcp")
	spec.Args = []string{"-config", filepath.Join(base, "config.yaml"), "-http", "-addr", fmt.Sprintf("127.0.0.1:%d", p.Port), "-tls=false"}
	return spec, files, nil
}

type postgresHost struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

func postgresHosts(addresses []string) ([]postgresHost, error) {
	hosts := make([]postgresHost, 0, len(addresses))
	for _, address := range addresses {
		host, rawPort, err := net.SplitHostPort(address)
		port := 5432
		if err != nil {
			if strings.Contains(address, ":") && net.ParseIP(address) == nil {
				return nil, errors.New("invalid PostgreSQL host address")
			}
			host = address
		} else {
			port, err = strconv.Atoi(rawPort)
			if err != nil || port < 1 || port > 65535 {
				return nil, errors.New("invalid PostgreSQL host port")
			}
		}
		if host == "" {
			return nil, errors.New("empty PostgreSQL hostname")
		}
		hosts = append(hosts, postgresHost{Host: host, Port: port})
	}
	return hosts, nil
}
