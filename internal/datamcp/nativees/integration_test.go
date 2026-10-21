//go:build integration

package nativees

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/datamcp"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestElasticsearchIntegration runs an isolated secured single node from an
// official Elasticsearch 8.x distribution. KARAKORAM_ES_HOME names its extracted
// directory; no existing cluster is contacted or modified.
func TestElasticsearchIntegration(t *testing.T) {
	home := os.Getenv("KARAKORAM_ES_HOME")
	if home == "" {
		t.Skip("set KARAKORAM_ES_HOME to an extracted Elasticsearch 8.x distribution")
	}
	home, err := filepath.Abs(home)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	conf := filepath.Join(root, "config")
	for _, dir := range []string{conf, filepath.Join(conf, "jvm.options.d"), filepath.Join(root, "data"), filepath.Join(root, "logs")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []string{"jvm.options", "log4j2.properties"} {
		b, err := os.ReadFile(filepath.Join(home, "config", file))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(conf, file), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ca, cert, key := integrationCertificates(t)
	for name, data := range map[string][]byte{"ca.pem": ca, "http.pem": cert, "http.key": key} {
		if err := os.WriteFile(filepath.Join(conf, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	config := fmt.Sprintf("cluster.name: datamcp-integration\nnode.name: datamcp-test\nnode.roles: [master, data, ingest]\nnode.processors: 1\nnetwork.host: 127.0.0.1\nhttp.port: %d\ntransport.port: 0\ndiscovery.type: single-node\npath.data: %s\npath.logs: %s\nxpack.security.enabled: true\nxpack.security.autoconfiguration.enabled: false\nxpack.security.http.ssl.enabled: true\nxpack.security.http.ssl.key: http.key\nxpack.security.http.ssl.certificate: http.pem\nxpack.security.http.ssl.certificate_authorities: [ca.pem]\nxpack.security.transport.ssl.enabled: false\nxpack.ml.enabled: false\ningest.geoip.downloader.enabled: false\n", port, filepath.Join(root, "data"), filepath.Join(root, "logs"))
	if err := os.WriteFile(filepath.Join(conf, "elasticsearch.yml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	password := "integration-bootstrap-password"
	environment := append(os.Environ(), "ES_PATH_CONF="+conf, "ES_JAVA_OPTS=-Xms512m -Xmx512m -XX:ActiveProcessorCount=2")
	for _, args := range [][]string{{"create"}, {"add", "-x", "bootstrap.password"}} {
		cmd := exec.Command(filepath.Join(home, "bin", "elasticsearch-keystore"), args...)
		cmd.Env = environment
		if args[0] == "add" {
			cmd.Stdin = strings.NewReader(password + "\n")
		}
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("keystore: %v: %s", err, output)
		}
	}
	logFile, err := os.Create(filepath.Join(root, "process.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(filepath.Join(home, "bin", "elasticsearch"))
	cmd.Env = environment
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-exited:
			case <-time.After(20 * time.Second):
				cmd.Process.Kill()
				<-exited
			}
		}
		logFile.Close()
		if t.Failed() {
			f, err := os.Open(filepath.Join(root, "process.log"))
			if err == nil {
				defer f.Close()
				b, _ := io.ReadAll(io.LimitReader(f, 64<<10))
				t.Logf("Elasticsearch process log:\n%s", b)
			}
		}
	})
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	endpoint := fmt.Sprintf("https://127.0.0.1:%d", port)
	request := func(method, path, user, pass string, body any) (int, []byte, error) {
		var data []byte
		if body != nil {
			data, _ = json.Marshal(body)
		}
		req, err := http.NewRequest(method, endpoint+path, bytes.NewReader(data))
		if err != nil {
			return 0, nil, err
		}
		req.SetBasicAuth(user, pass)
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer res.Body.Close()
		b, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
		return res.StatusCode, b, err
	}
	deadline := time.Now().Add(150 * time.Second)
	for {
		status, _, err := request("GET", "/", "elastic", password, nil)
		if err == nil && status == 200 {
			break
		}
		select {
		case err := <-exited:
			stopped = true
			t.Fatalf("Elasticsearch exited during startup: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Elasticsearch did not become ready")
		}
		time.Sleep(500 * time.Millisecond)
	}
	admin := func(method, path string, body any) []byte {
		t.Helper()
		status, b, err := request(method, path, "elastic", password, body)
		if err != nil || status < 200 || status >= 300 {
			t.Fatalf("admin %s %s: status %d, %v: %s", method, path, status, err, b)
		}
		return b
	}
	admin("PUT", "/logs-shared", map[string]any{"settings": map[string]any{"number_of_shards": 1, "number_of_replicas": 0}, "mappings": map[string]any{"properties": map[string]any{"tenant": map[string]any{"type": "keyword"}, "message": map[string]any{"type": "keyword"}}}})
	admin("PUT", "/.private", map[string]any{"settings": map[string]any{"number_of_shards": 1, "number_of_replicas": 0}})
	admin("PUT", "/outside", map[string]any{"settings": map[string]any{"number_of_shards": 1, "number_of_replicas": 0}})
	admin("PUT", "/logs-shared/_doc/mine?refresh=true", map[string]any{"tenant": "mine", "message": "own"})
	admin("PUT", "/logs-shared/_doc/theirs?refresh=true", map[string]any{"tenant": "theirs", "message": "other"})
	admin("POST", "/_aliases", map[string]any{"actions": []any{map[string]any{"add": map[string]any{"index": "logs-shared", "alias": "logs-tenant", "filter": map[string]any{"term": map[string]any{"tenant": "mine"}}}}}})
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		user := engine.DataAccessUser(level)
		admin("PUT", "/_security/role/"+user, elastic.DataAccessRoleDefinition([]string{"logs-*"}, level))
		admin("PUT", "/_security/user/"+user, map[string]any{"password": "integration-user-password", "roles": []string{user}})
	}
	open := func(level engine.DataAccessLevel) datamcp.Upstream {
		t.Helper()
		u, err := Open(context.Background(), datamcp.NativeInput{Credentials: engine.DataAccessCredentials{Hosts: []string{endpoint}, Username: engine.DataAccessUser(level), Password: "integration-user-password", CAPEM: ca, TLS: true}, Scope: domain.DataMcpScope{IndexPatterns: []string{"logs-*"}}, Level: level})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { u.Close() })
		return u
	}
	ro, rw := open(engine.DataAccessRead), open(engine.DataAccessReadWrite)
	call := func(u datamcp.Upstream, name string, args map[string]any) map[string]any {
		t.Helper()
		result, err := u.CallTool(context.Background(), name, args)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var out map[string]any
		if len(result.Content) != 1 {
			t.Fatal("missing JSON result")
		}
		if err := json.Unmarshal([]byte(result.Content[0].(*mcp.TextContent).Text), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, name := range []string{"indices_list", "mapping_get", "search", "count", "shards_get", "index_stats"} {
		t.Run(name, func(t *testing.T) { call(ro, name, map[string]any{"index": "logs-shared"}) })
	}
	t.Run("esql_query", func(t *testing.T) {
		out := call(ro, "esql_query", map[string]any{"query": "FROM logs-shared | KEEP tenant, message"})
		if len(out["values"].([]any)) != 2 {
			t.Fatal(out)
		}
	})
	for _, index := range []string{".private", ".security", "outside"} {
		if _, err := ro.CallTool(context.Background(), "search", map[string]any{"index": index}); err == nil {
			t.Fatalf("index %s allowed", index)
		}
	}
	for _, name := range []string{"doc_index", "doc_update", "delete_by_query"} {
		if _, err := ro.CallTool(context.Background(), name, map[string]any{"index": "logs-shared", "id": "forbidden", "document": map[string]any{}}); err == nil {
			t.Fatalf("read-only %s allowed", name)
		}
	}
	for _, op := range []struct {
		method, path string
		body         any
	}{
		{"PUT", "/logs-shared/_doc/forbidden", map[string]any{"tenant": "mine"}},
		{"POST", "/logs-shared/_update/mine", map[string]any{"doc": map[string]any{"message": "forbidden"}}},
		{"POST", "/logs-shared/_delete_by_query", map[string]any{"query": map[string]any{"match_all": map[string]any{}}}},
	} {
		status, b, err := request(op.method, op.path, engine.DataAccessReadUser, "integration-user-password", op.body)
		if err != nil || status != http.StatusForbidden {
			t.Fatalf("database read-only enforcement: %d %v %s", status, err, b)
		}
	}
	if out := call(ro, "count", map[string]any{"index": "logs-tenant"}); out["count"] != float64(1) {
		t.Fatalf("alias filter lost: %v", out)
	}
	out := call(ro, "search", map[string]any{"index": "logs-tenant"})
	if len(object(out["hits"])["hits"].([]any)) != 1 {
		t.Fatalf("alias search filter lost: %v", out)
	}
	call(rw, "doc_index", map[string]any{"index": "logs-shared", "id": "written", "document": map[string]any{"tenant": "mine", "message": "before"}})
	call(rw, "doc_update", map[string]any{"index": "logs-shared", "id": "written", "document": map[string]any{"message": "after"}})
	updated := admin("GET", "/logs-shared/_doc/written", nil)
	if !bytes.Contains(updated, []byte(`"after"`)) {
		t.Fatalf("update not persisted: %s", updated)
	}
	admin("POST", "/logs-shared/_refresh", nil)
	call(rw, "delete_by_query", map[string]any{"index": "logs-tenant", "query": map[string]any{"match_all": map[string]any{}}})
	admin("POST", "/logs-shared/_refresh", nil)
	if out := call(ro, "count", map[string]any{"index": "logs-shared"}); out["count"] != float64(1) {
		t.Fatalf("filtered-alias deletion escaped tenant: %v", out)
	}
	remaining := admin("GET", "/logs-shared/_doc/theirs", nil)
	if !bytes.Contains(remaining, []byte(`"found":true`)) {
		t.Fatalf("other tenant deleted: %s", remaining)
	}
}

func integrationCertificates(t *testing.T) ([]byte, []byte, []byte) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Data MCP integration CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	server := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	serverDER, err := x509.CreateCertificate(rand.Reader, server, ca, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}
