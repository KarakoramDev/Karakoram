package agentapi

import (
	"context"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStreamingClientAuthenticationAndOrigin(t *testing.T) {
	got := make(chan string, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("Authorization")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "member-secret", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	streaming := client.StreamingClient()
	defer streaming.CloseIdleConnections()
	if streaming.Timeout != 0 || streaming.Transport.(*memberTransport).transport.ResponseHeaderTimeout != 0 {
		t.Fatal("streaming requests must not have a whole-request or response-header timeout")
	}
	request, _ := http.NewRequest("GET", server.URL+"/v1/datamcp/ro", nil)
	request.Header.Set("Authorization", "original")
	response, err := streaming.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if value := <-got; value != "Bearer member-secret" {
		t.Fatal(value)
	}
	if request.Header.Get("Authorization") != "original" {
		t.Fatal("caller request mutated")
	}
	for _, endpoint := range []string{"https://another.invalid/v1/datamcp/ro", strings.Replace(server.URL, "https:", "http:", 1)} {
		r, _ := http.NewRequest("GET", endpoint, nil)
		if _, err = streaming.Do(r); err == nil {
			t.Fatal("cross-origin request accepted")
		}
	}
}

func TestStreamingClientPinnedTrustHostnameAndNamespace(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	dialed := false
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		dialed = true
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	client, err := NewClient(ClientOptions{BaseURL: "https://wrong.invalid:9443", Token: "token", CAPEM: ca, DialContext: dial})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	streaming := client.StreamingClient()
	defer streaming.CloseIdleConnections()
	_, err = streaming.Get("https://wrong.invalid:9443/v1/datamcp/ro")
	if err == nil || !dialed {
		t.Fatal("hostname verification or namespace dial missing", err, dialed)
	}
}

func TestStreamingClientRefusesRedirects(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://other.invalid/", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "secret", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	streaming := client.StreamingClient()
	defer streaming.CloseIdleConnections()
	response, err := streaming.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 307 {
		t.Fatal(response.StatusCode)
	}
}
