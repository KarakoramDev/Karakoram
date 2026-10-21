// SPDX-License-Identifier: Apache-2.0
package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestNotificationChannelCRUDAndSecretRetention(t *testing.T) {
	s, _, token := testServer(t)
	body := `{"name":"alerts","type":"webhook","enabled":true,"webhook":{"url":"https://example.com/private-token","signingSecret":"signing-value"}}`
	w := call(s, "POST", "/api/v1/notification-channels", body, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, secret := range []string{"private-token", "signing-value"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("secret in response")
		}
	}
	body = `{"name":"alerts","type":"webhook","enabled":true,"webhook":{}}`
	w = call(s, "PUT", "/api/v1/notification-channels/alerts", body, token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	channel, err := s.Store.NotificationChannelForDelivery(t.Context(), "alerts")
	if err != nil || channel.Secrets.WebhookSigningSecret != "signing-value" || !strings.Contains(channel.Secrets.WebhookURL, "private-token") {
		t.Fatal("secret not retained", err)
	}
	body = `{"name":"alerts","type":"webhook","enabled":true,"webhook":{"url":"https://other.example/path"}}`
	w = call(s, "PUT", "/api/v1/notification-channels/alerts", body, token)
	if w.Code != 400 {
		t.Fatal("destination credential reuse accepted", w.Code, w.Body.String())
	}
	for _, methodPath := range [][2]string{{"GET", "/api/v1/notification-channels"}, {"GET", "/api/v1/notification-channels/alerts"}} {
		w = call(s, methodPath[0], methodPath[1], "", token)
		if w.Code != 200 || strings.Contains(w.Body.String(), "private-token") || strings.Contains(w.Body.String(), "signing-value") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w = call(s, "DELETE", "/api/v1/notification-channels/alerts", "", token)
	if w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call(s, "GET", "/api/v1/notification-channels/alerts", "", token)
	if w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestNotificationChannelValidation(t *testing.T) {
	s, _, token := testServer(t)
	for _, body := range []string{
		`{"name":"A","type":"webhook","enabled":true,"webhook":{"url":"https://example.com"}}`,
		`{"name":"a","type":"webhook","enabled":true,"slack":{"webhookUrl":"https://example.com"}}`,
		`{"name":"a","type":"webhook","enabled":true,"webhook":{"url":"http://example.com"}}`,
		`{"name":"a","type":"webhook","enabled":true,"webhook":{"url":"https://example.com"},"slack":{"webhookUrl":"https://example.com"}}`,
		`{"name":"a","type":"email","enabled":true,"email":{"host":"example.com","port":587,"security":"starttls","from":"a@example.com","to":["b@example.com"],"username":"user"}}`,
	} {
		w := call(s, "POST", "/api/v1/notification-channels", body, token)
		if w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}
func TestNotificationSyntheticUsesSameTransport(t *testing.T) {
	var seen bool
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		seen = body["reason"] == "NotificationTest" && body["category"] == "operation-failed" && body["severity"] == "info"
		w.WriteHeader(204)
	}))
	defer remote.Close()
	parsed, _ := url.Parse(remote.URL)
	t.Setenv("KARAKORAM_NOTIFY_ENDPOINT_ALLOWLIST", parsed.Host)
	s, _, token := testServer(t)
	body := fmt.Sprintf(`{"name":"test","type":"webhook","enabled":false,"webhook":{"url":%q}}`, remote.URL)
	w := call(s, "POST", "/api/v1/notification-channels", body, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call(s, "POST", "/api/v1/notification-channels/test/test", "", token)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"delivered":true`) || !seen {
		t.Fatal(w.Code, w.Body.String(), seen)
	}
}

func TestNotificationSyntheticReplacementKeepsDestinationCredentialsTogether(t *testing.T) {
	s, _, token := testServer(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	checkSignature := func(w http.ResponseWriter, r *http.Request, secret string) {
		t.Helper()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		parts := strings.Split(r.Header.Get("X-Karakoram-Signature"), ",v1=")
		if len(parts) != 2 || !strings.HasPrefix(parts[0], "t=") {
			t.Error("missing notification signature")
			w.WriteHeader(400)
			return
		}
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(strings.TrimPrefix(parts[0], "t=") + "."))
		mac.Write(body)
		if !hmac.Equal([]byte(parts[1]), []byte(hex.EncodeToString(mac.Sum(nil)))) {
			t.Error("destination received another channel version's credentials")
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(204)
	}
	remoteA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		checkSignature(w, r, "secret-a")
	}))
	defer remoteA.Close()
	remoteB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		checkSignature(w, r, "secret-b")
	}))
	defer remoteB.Close()
	parsedA, _ := url.Parse(remoteA.URL)
	parsedB, _ := url.Parse(remoteB.URL)
	t.Setenv("KARAKORAM_NOTIFY_ENDPOINT_ALLOWLIST", parsedA.Host+","+parsedB.Host)
	body := fmt.Sprintf(`{"name":"test","type":"webhook","enabled":false,"webhook":{"url":%q,"signingSecret":"secret-a"}}`, remoteA.URL)
	w := call(s, "POST", "/api/v1/notification-channels", body, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- call(s, "POST", "/api/v1/notification-channels/test/test", "", token) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("test delivery did not start")
	}
	// PUT must complete while the old snapshot is in flight; no read lock may
	// span transport I/O and the new destination must get only its own secret.
	body = fmt.Sprintf(`{"name":"test","type":"webhook","enabled":false,"webhook":{"url":%q,"signingSecret":"secret-b"}}`, remoteB.URL)
	w = call(s, "PUT", "/api/v1/notification-channels/test", body, token)
	close(release)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	select {
	case w = <-done:
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"delivered":true`) {
			t.Fatal(w.Code, w.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("test delivery blocked after replacement")
	}
	w = call(s, "POST", "/api/v1/notification-channels/test/test", "", token)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"delivered":true`) {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call(s, "DELETE", "/api/v1/notification-channels/test", "", token)
	if w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call(s, "POST", "/api/v1/notification-channels/test/test", "", token)
	if w.Code != 404 {
		t.Fatal("deleted channel test did not return not found", w.Code, w.Body.String())
	}
}
