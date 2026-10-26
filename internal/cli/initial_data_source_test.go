// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestInitialRestoreConflictDisplaysOperation(t *testing.T) {
	const id = "restore-create-123"
	response := &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(strings.NewReader(`{"code":"conflict","message":"Initial restore operation restore-create-123 must complete before this mutation","details":{"resource":"operation","name":"restore-create-123","reason":"invalid_state"}}`))}
	_, err := decodeResponse(response)
	if err == nil || !strings.Contains(err.Error(), "HTTP 409") || !strings.Contains(err.Error(), id) {
		t.Fatalf("error=%v", err)
	}
}
