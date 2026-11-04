// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package baoapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testClient(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	c, e := New(s.URL, s.Client())
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func TestRedirectRefused(t *testing.T) {
	reached := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true }))
	defer target.Close()
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	})
	e := c.WithToken("secret-token").Do(context.Background(), "PUT", "sys/unseal", map[string]string{"key": "secret-share"}, nil)
	if !IsStatus(e, http.StatusTemporaryRedirect) || reached {
		t.Fatalf("redirect was not refused: reached=%v error=%v", reached, e)
	}
}
func TestErrorsDoNotReflectSecrets(t *testing.T) {
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_ = json.NewEncoder(w).Encode(map[string]any{"errors": []string{"secret-token secret-share"}})
	})
	e := c.WithToken("secret-token").Do(context.Background(), "PUT", "sys/unseal", map[string]string{"key": "secret-share"}, nil)
	if e == nil || strings.Contains(e.Error(), "secret-token") || strings.Contains(e.Error(), "secret-share") {
		t.Fatalf("unsafe error: %v", e)
	}
}
func TestUnsealWithResetsPartialProgress(t *testing.T) {
	calls := []string{}
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, `{"sealed":true,"progress":1}`)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["reset"] == true {
			calls = append(calls, "reset")
		} else {
			calls = append(calls, body["key"].(string))
		}
		fmt.Fprintf(w, `{"sealed":%t}`, len(calls) < 3)
	})
	s, e := c.UnsealWith(context.Background(), []string{"one", "two", "unused"})
	if e != nil || s.Sealed || strings.Join(calls, ",") != "reset,one,two" {
		t.Fatalf("state=%+v calls=%v error=%v", s, calls, e)
	}
}
func TestGenerateRootOTP(t *testing.T) {
	otp, token := "0123456789abcdef", "hvs.test-root-xx"
	encoded := make([]byte, len(token))
	for i := range token {
		encoded[i] = token[i] ^ otp[i]
	}
	count := 0
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "DELETE":
			w.WriteHeader(204)
		case strings.HasSuffix(r.URL.Path, "attempt"):
			_ = json.NewEncoder(w).Encode(map[string]any{"nonce": "nonce", "otp": otp, "required": 2})
		default:
			count++
			_ = json.NewEncoder(w).Encode(map[string]any{"complete": count == 2, "encoded_token": base64.RawStdEncoding.EncodeToString(encoded)})
		}
	})
	got, e := c.GenerateRoot(context.Background(), []string{"one", "two"})
	if e != nil || got != token {
		t.Fatalf("wrong generated token or error: %v", e)
	}
}
func TestResponseSizeLimits(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
		init bool
	}{{"ordinary", MaxResponseBytes + 1, false}, {"initialization", (64 << 10) + 1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(strings.Repeat(" ", tc.size))) })
			var e error
			if tc.init {
				_, e = c.Init(context.Background(), InitRequest{SecretShares: 1, SecretThreshold: 1})
			} else {
				var v any
				e = c.Do(context.Background(), "GET", "sys/health", nil, &v)
			}
			if e == nil || !strings.Contains(e.Error(), "too large") {
				t.Fatalf("missing size limit: %v", e)
			}
		})
	}
}
