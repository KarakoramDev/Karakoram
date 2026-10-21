package datamcp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type authTransport struct{ base http.RoundTripper }

func (a authTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header = r.Header.Clone()
	r.Header.Set("Authorization", "Bearer agent-token")
	return a.base.RoundTrip(r)
}

type integrationAgent struct {
	url, dir string
	client   *http.Client
	runner   *Runner
	mu       sync.Mutex
	config   agentapi.MemberConfig
}

func startAgent(t *testing.T, s companionspec.Spec) *integrationAgent {
	t.Helper()
	a := &integrationAgent{dir: t.TempDir(), runner: New()}
	raw, _ := json.Marshal(s)
	a.config = agentapi.MemberConfig{Engine: companionspec.Engine, EngineSpec: raw, ConfigVersion: 1, Cluster: "integration", Member: "integrationmcp"}
	cp := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer agent-token" {
			w.WriteHeader(401)
			return
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		json.NewEncoder(w).Encode(a.config)
	}))
	t.Cleanup(cp.Close)
	cert := cp.TLS.Certificates[0]
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
	key, e := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if e != nil {
		t.Fatal(e)
	}
	a.config.AgentTLS = agentapi.Certificate{CertPEM: string(ca), KeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))}
	port := freePort(t)
	a.url = fmt.Sprintf("https://127.0.0.1:%d", port)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)
	transport := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	a.client = &http.Client{Transport: authTransport{transport}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- agent.Run(ctx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: string(ca), Token: "agent-token", Cluster: "integration", Member: "integrationmcp", DataDir: a.dir, Listen: fmt.Sprintf("127.0.0.1:%d", port), Engine: companionspec.Engine, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, a.runner)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(65 * time.Second):
			t.Error("agent shutdown timed out")
		}
		transport.CloseIdleConnections()
	})
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("POST", a.url+"/v1/datamcp/readiness", nil)
		res, e := a.client.Do(req)
		if e == nil {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode == 200 {
				t.Logf("ready: %s", body)
				return a
			}
		}
		select {
		case e := <-done:
			t.Fatalf("agent exited: %v", e)
		default:
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, path := range []string{"/v1/status", "/v1/logs?process=mcp-ro"} {
		req, _ := http.NewRequest("GET", a.url+path, nil)
		if response, err := a.client.Do(req); err == nil {
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			t.Logf("%s: %s", path, body)
		}
	}
	t.Fatal("agent readiness deadline exceeded")
	return nil
}
