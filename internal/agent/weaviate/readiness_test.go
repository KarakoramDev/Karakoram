// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
)

func TestReadinessRoute(t *testing.T) {
	mux := http.NewServeMux()
	New().Routes(mux, nil)
	for _, body := range []string{`{}`, `{"secret":"not accepted"}`, `{} {}`, `null`, strings.Repeat(" ", 4097) + `{}`} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest("POST", "/v1/weaviate/readiness", strings.NewReader(body)))
		if body != `{}` {
			if response.Code != 400 {
				t.Fatal(response.Code)
			}
			continue
		}
		var result wvspec.ReadinessResponse
		if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &result) != nil || result.Ready || result.ConfigVersion != 0 || result.Message == "" {
			t.Fatal(response.Body.String())
		}
	}
}
func TestReadinessHolds(t *testing.T) {
	for _, control := range []string{`{"maintenance":true}`, `{"stopped":{"weaviate":true}}`} {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "karakoram"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "karakoram", "controls.json"), []byte(control), 0600); err != nil {
			t.Fatal(err)
		}
		if err := New().Readiness(t.Context(), agent.Options{}, []string{"--data-dir=" + dir}); err != nil {
			t.Fatal(err)
		}
	}
	if err := New().Readiness(t.Context(), agent.Options{}, []string{"--data-dir=relative"}); err == nil {
		t.Fatal("relative path accepted")
	}
}
