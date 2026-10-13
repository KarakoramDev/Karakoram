// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package relay

import (
	"context"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestResponseBeforeCloseAndEOFClosesPair(t *testing.T) {
	f := newFixture(t)
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	payload := strings.Repeat("response", 2048)
	closed := make(chan error, 1)
	go func() {
		conn, err := target.Accept()
		if err != nil {
			closed <- err
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(time.Second))
		request := make([]byte, 4)
		if _, err = io.ReadFull(conn, request); err != nil {
			closed <- err
			return
		}
		if _, err = conn.Write([]byte(payload)); err != nil {
			closed <- err
			return
		}
		if err = conn.(*net.TCPConn).CloseWrite(); err != nil {
			closed <- err
			return
		}
		_, err = conn.Read(make([]byte, 1))
		closed <- err
	}()
	o := f.o
	o.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", target.Addr().String())
	}
	f.hub.SetPolicy("proj-test", []string{"member:27017"})
	f.runRelay(o)
	f.connected()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := f.hub.DialContext(ctx, "proj-test", "tcp", "member:27017")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Second))
	if _, err = conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	reply, err := io.ReadAll(conn)
	if err != nil || string(reply) != payload {
		t.Fatal("response not fully delivered", len(reply), err)
	}
	if err = <-closed; err != io.EOF {
		t.Fatal("target did not observe full close", err)
	}
}
func TestPendingHandshakesBounded(t *testing.T) {
	f := newFixture(t)
	f.hub.Close()
	f.server.Close()
	f.hubOptions.MaxPendingHandshakes = 1
	f.startHub("")
	endpoint, err := connectionURL(f.o.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: f.o.HTTPClient})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	_, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: f.o.HTTPClient})
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("pending handshake cap not enforced", err)
	}
	ws.CloseNow()
	eventually(t, func() bool { return len(f.hub.pending) == 0 })
}
func TestEnvironmentAndOriginValidation(t *testing.T) {
	f := newFixture(t)
	for key, value := range map[string]string{"KARAKORAM_RELAY_URL": "https://example.com", "KARAKORAM_RELAY_NAMESPACE": "proj-test", "KARAKORAM_RELAY_CERT": string(f.o.CertPEM), "KARAKORAM_RELAY_KEY": string(f.o.KeyPEM), "KARAKORAM_RELAY_CA": string(f.ca), "KARAKORAM_RELAY_LISTEN": "", "hostname": "relayhost"} {
		t.Setenv(key, value)
	}
	o, err := OptionsFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if o.ListenAgent != ":9443" || o.Hostname != "relayhost" {
		t.Fatal("incorrect environment defaults")
	}
	digest, err := hex.DecodeString(o.ExecutableSHA256)
	if err != nil || len(digest) != 32 {
		t.Fatal("missing executable hash")
	}
	for _, origin := range []string{"http://example.com", "wss://example.com", "https://example.com/path", "https://user:pass@example.com", "https://example.com?token=secret", "https://example.com#fragment"} {
		if _, err := connectionURL(origin); err == nil {
			t.Fatal("invalid origin accepted")
		}
	}
	if endpoint, err := connectionURL("https://example.com/"); err != nil || endpoint != "wss://example.com/relay/v1/connect" {
		t.Fatal(endpoint, err)
	}
}
