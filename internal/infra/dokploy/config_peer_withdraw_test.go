// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package dokploy

import (
	"bytes"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"io"
	"net/http"
	"testing"
)

// ExportConfigPeerWithdrawal changes the fake platform outside its instrumented
// client, exactly as an external actor can change a peer between listing checks.
func ExportConfigPeerWithdrawal(t *testing.T, rc *engine.Context) func() {
	t.Helper()
	b := rc.Backend.(*Backend)
	app, err := b.application(t.Context(), "samplep0")
	if err != nil || app == nil {
		t.Fatal("peer fixture unavailable", err)
	}
	metadata, _ := parseMetadata(app.Description)
	metadata.Desired = "stopped"
	data, err := json.Marshal(map[string]string{"applicationId": app.ID, "description": metadata.description()})
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), "POST", b.c.origin+"/api/application.update", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("x-api-key", b.c.currentToken())
		request.Header.Set("Content-Type", "application/json")
		response, err := b.c.http.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		io.Copy(io.Discard, response.Body)
		if response.StatusCode != 200 {
			t.Fatal("external peer withdrawal failed", response.StatusCode)
		}
	}
}
