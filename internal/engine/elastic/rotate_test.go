// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestKibanaRotationInvalidatesOldTokenAfterRunning(t *testing.T) {
	var actions []string
	running := false
	p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasPrefix(path, "/_security/service/elastic/kibana/credential/token/"):
			if r.Method == "POST" {
				actions = append(actions, "create")
				opsJSON(w, map[string]any{"created": true, "token": map[string]string{"value": "new-secret-token"}})
				return
			}
			if strings.HasSuffix(path, "/kk-original") {
				actions = append(actions, "invalidate")
				if !running {
					t.Error("old token invalidated before Kibana is running")
				}
			}
			opsJSON(w, map[string]any{"found": true})
		case path == "/_security/_authenticate":
			opsJSON(w, map[string]string{"username": "elastic/kibana"})
		case path == "/v1/reload":
			actions = append(actions, "reload")
			opsJSON(w, map[string]any{})
		case path == "/v1/processes/kibana/restart":
			actions = append(actions, "restart")
			running = true
			opsJSON(w, map[string]any{})
		case path == "/v1/status":
			state := "stopped"
			if running {
				state = "running"
			}
			opsJSON(w, agentapi.Status{ConfigVersion: 10000, Processes: map[string]agentapi.ProcessStatus{"kibana": {State: state}}})
		default:
			opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
		}
	})
	s.Members = append(s.Members, &member{Name: "testkb", Kind: kindKibana, Stage: stageJoined})
	op := &domain.Operation{ID: "rotate-kibana", Kind: domain.OperationRotateSystemUser}
	cp := operationState{}
	for range 30 {
		result, err := p.runRotateUser(context.Background(), rc, s, op, cp, "kibana-token")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(result.Checkpoint), "new-secret-token") {
			t.Fatal("checkpoint contains token secret")
		}
		cp = opsCheckpoint(t, result)
		// Re-enter installation with the same generated credential, not a new token.
		if cp.Stage == "ensure" {
			result, err = p.runRotateUser(context.Background(), rc, s, op, operationState{Stage: "install", Names: cp.Names, OldTokenName: cp.OldTokenName}, "kibana-token")
			if err != nil {
				t.Fatal(err)
			}
			cp = opsCheckpoint(t, result)
		}
		if result.Done {
			break
		}
	}
	creates := 0
	for _, action := range actions {
		if action == "create" {
			creates++
		}
	}
	if cp.Stage != "done" || slices.Index(actions, "invalidate") <= slices.Index(actions, "restart") || creates != 1 {
		t.Fatalf("checkpoint=%+v actions=%v", cp, actions)
	}
	sec := rc.Secrets.(*opsSecrets)
	if string(sec.values[secretKibanaToken]) != "new-secret-token" {
		t.Fatal("rotation did not install token")
	}
	if _, exists := sec.values["operations/"+op.ID+"/rotation"]; exists {
		t.Fatal("staged secret was retained")
	}
}

func TestTLSRotationWaitsForAcknowledgement(t *testing.T) {
	version := int64(1)
	p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			opsJSON(w, agentapi.Status{ConfigVersion: version})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/processes/") {
			t.Error("TLS rotation restarted process")
		}
		opsJSON(w, map[string]any{})
	})
	cp := operationState{Stage: "publish", Names: []string{"example.com"}}
	result, err := p.runRotateTLS(context.Background(), rc, s, cp)
	if err != nil {
		t.Fatal(err)
	}
	cp = opsCheckpoint(t, result)
	result, err = p.runRotateTLS(context.Background(), rc, s, cp)
	if err != nil || result.Done {
		t.Fatalf("premature completion: %+v %v", result, err)
	}
	version = cp.ConfigVersion
	result, err = p.runRotateTLS(context.Background(), rc, s, cp)
	if err != nil || !result.Done {
		t.Fatalf("acknowledgement: %+v %v", result, err)
	}
}

// Every listed system user must be accepted by the rotation operation, so the
// UI never offers a rotation that fails as unsupported.
func TestSystemUsersAreRotatable(t *testing.T) {
	users, err := (&Provider{}).SystemUsers(context.Background(), nil)
	if err != nil || len(users) == 0 {
		t.Fatal(users, err)
	}
	for _, u := range users {
		if key, _ := rotationTarget(u.Name); key == "" {
			t.Errorf("system user %q is not rotatable", u.Name)
		}
	}
}
