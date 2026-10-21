// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func previewBody(t *testing.T, w *httptest.ResponseRecorder) dryRunPreview {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("preview: %d %s", w.Code, w.Body.String())
	}
	var result dryRunPreview
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if w.Header().Get("Karakoram-Operation-Id") != "" {
		t.Fatal("preview accepted an operation")
	}
	return result
}
func requirePreviewEffect(t *testing.T, result dryRunPreview, kind string) {
	t.Helper()
	for _, effect := range result.Effects {
		if effect.Kind == kind {
			return
		}
	}
	t.Fatalf("missing %s: %+v", kind, result.Effects)
}

func TestDryRunDomainSideEffects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dry-run.db")
	s, _, token := testServerWithDatabase(t, path)
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = s.Store.CreateBackup(t.Context(), &domain.Backup{Cluster: "prod", Name: "source", Provider: "fake", Phase: domain.PhaseSucceeded, Type: "logical"}); err != nil {
		t.Fatal(err)
	}
	snapshot := func() []string {
		t.Helper()
		var out []string
		// Compare complete durable domain state, not just row counts. Auth token
		// last-used metadata is intentionally excluded from the records snapshot.
		for _, query := range []string{"SELECT * FROM clusters ORDER BY namespace,name", "SELECT * FROM events ORDER BY id", "SELECT * FROM records WHERE kind IN ('operation','restore','secret','backup') ORDER BY kind,scope,key", "SELECT * FROM idempotency_requests ORDER BY identity"} {
			rows, e := db.Query(query)
			if e != nil {
				t.Fatal(e)
			}
			cols, e := rows.Columns()
			if e != nil {
				t.Fatal(e)
			}
			values := make([]any, len(cols))
			targets := make([]any, len(cols))
			for i := range values {
				targets[i] = &values[i]
			}
			for rows.Next() {
				if e = rows.Scan(targets...); e != nil {
					t.Fatal(e)
				}
				data, e := json.Marshal(values)
				if e != nil {
					t.Fatal(e)
				}
				out = append(out, string(data))
			}
			if e = rows.Err(); e != nil {
				t.Fatal(e)
			}
			rows.Close()
		}
		return out
	}
	live, unsubscribe := s.Hub.Subscribe("", "")
	defer unsubscribe()
	before := snapshot()
	create := `{"name":"preview","spec":{"provider":"fake"}}`
	for range 2 {
		result := previewBody(t, idempotentCall(s, "POST", "/api/v1/dry-run/clusters", create, token, "preview-key"))
		if result.Operation != "create" || result.Cluster.Spec.Components.Engine.Replicas != 3 || result.Cluster.Spec.Version != "8.0" || len(result.Changes) != 0 {
			t.Fatalf("defaults: %+v", result)
		}
	}
	for _, tc := range []struct{ patch, effect, path string }{
		{`{"spec":{"components":{"engine":{"replicas":1}}}}`, "member-removal", "spec.components.engine.replicas"},
		{`{"spec":{"version":"8.1"}}`, "version-upgrade", "spec.version"},
		{`{"spec":{"exposure":{"public":true,"allowlist":["2001:db8::/32"]}}}`, "public-exposure", "spec.exposure.public"},
	} {
		result := previewBody(t, call(s, "PATCH", "/api/v1/dry-run/clusters/prod", tc.patch, token))
		requirePreviewEffect(t, result, tc.effect)
		found := false
		for _, change := range result.Changes {
			found = found || change.Path == tc.path
		}
		if !found {
			t.Fatalf("missing change %s: %+v", tc.path, result.Changes)
		}
	}
	result := previewBody(t, call(s, "PUT", "/api/v1/dry-run/clusters/prod", `{"spec":{"provider":"fake","components":{"engine":{"replicas":5}}}}`, token))
	if result.Operation != "replace" || len(result.Changes) == 0 {
		t.Fatalf("replace: %+v", result)
	}
	result = previewBody(t, call(s, "POST", "/api/v1/dry-run/clusters/prod/restores", `{"name":"preview-restore","source":{"backup":"source"}}`, token))
	requirePreviewEffect(t, result, "in-place-restore")
	if result.Restore == nil || result.Restore.OperationId != nil || result.Restore.StartedAt != nil {
		t.Fatalf("fabricated restore identity: %+v", result.Restore)
	}
	if after := snapshot(); !reflect.DeepEqual(before, after) {
		t.Fatal("dry-run changed durable domain state")
	}
	spy := s.Trigger.(*triggerSpy)
	spy.mu.Lock()
	triggered := len(spy.names)
	spy.mu.Unlock()
	if triggered != 0 {
		t.Fatal("dry-run triggered reconcile")
	}
	select {
	case event := <-live:
		t.Fatalf("dry-run published event: %+v", event)
	default:
	}
	real := idempotentCall(s, "POST", "/api/v1/clusters", create, token, "preview-key")
	if real.Code != 201 {
		t.Fatal(real.Code, real.Body.String())
	}
}

func TestDryRunValidationAndRBAC(t *testing.T) {
	s, _, token := testServer(t)
	body := `{"name":"badspec","spec":{"provider":"fake","version":"invalid"}}`
	dry := call(s, "POST", "/api/v1/dry-run/clusters", body, token)
	real := call(s, "POST", "/api/v1/clusters", body, token)
	if dry.Code != real.Code || dry.Code < 400 || dry.Body.String() != real.Body.String() {
		t.Fatal("validation differs", dry.Code, dry.Body.String(), real.Code, real.Body.String())
	}
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "preview-viewer", Roles: []string{"role:viewer"}}, "long-test-password"); err != nil {
		t.Fatal(err)
	}
	viewer, _, err := s.Auth.CreateToken(t.Context(), "preview-viewer", "preview", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ method, path, body string }{
		{"POST", "/dry-run/clusters", `{"name":"preview","spec":{"provider":"fake"}}`},
		{"PUT", "/dry-run/clusters/prod", `{"spec":{"provider":"fake"}}`},
		{"PATCH", "/dry-run/clusters/prod", `{"spec":{"version":"8.0"}}`},
		{"POST", "/dry-run/clusters/prod/restores", `{"name":"preview","source":{"backup":"source"}}`},
	} {
		w := call(s, tc.method, "/api/v1"+tc.path, tc.body, viewer)
		if w.Code != 403 {
			t.Fatal(tc.path, w.Code, w.Body.String())
		}
	}
}

func TestDryRunAdmission(t *testing.T) {
	s, _, token := testServer(t)
	w := call(s, "POST", "/api/v1/dry-run/clusters", `{"name":"prod","spec":{"provider":"fake"}}`, token)
	if w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if err := s.Store.CreateRestore(t.Context(), &domain.Restore{Cluster: "prod", Name: "existing", Phase: domain.PhasePending}); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.CreateBackup(t.Context(), &domain.Backup{Cluster: "prod", Name: "source", Provider: "fake", Phase: domain.PhaseSucceeded, Type: "logical"}); err != nil {
		t.Fatal(err)
	}
	w = call(s, "POST", "/api/v1/dry-run/clusters/prod/restores", `{"name":"existing","source":{"backup":"source"}}`, token)
	if w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if err := s.Store.MarkClusterDeleting(t.Context(), domain.ClusterRef{Name: "prod"}, domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"PUT", "PATCH"} {
		w = call(s, method, "/api/v1/dry-run/clusters/prod", `{"spec":{"provider":"fake"}}`, token)
		if w.Code != 409 {
			t.Fatal(method, w.Code, w.Body.String())
		}
	}
	w = call(s, "POST", "/api/v1/dry-run/clusters/prod/restores", `{"name":"another","source":{"backup":"source"}}`, token)
	if w.Code != 409 || !strings.Contains(w.Body.String(), "being deleted") {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestDryRunEffects(t *testing.T) {
	s, _, _ := testServer(t)
	base := domain.Cluster{Name: "preview", Spec: domain.ClusterSpec{Provider: "fake"}}
	(&fakeProvider{}).ApplyDefaults(&base.Spec)
	for _, tc := range []struct {
		name    string
		change  func(*domain.Cluster, *domain.Cluster)
		effects []string
	}{
		{"resources", func(_, next *domain.Cluster) { next.Spec.Components.Engine.Resources.MinCPU = 2 }, []string{"resource-change"}},
		{"members", func(_, next *domain.Cluster) { next.Spec.Components.Engine.Replicas = 5 }, []string{"member-addition"}},
		{"shards", func(old, next *domain.Cluster) { old.Spec.Topology.Shards = 3; next.Spec.Topology.Shards = 1 }, []string{"member-removal"}},
		{"tls-optional", func(_, next *domain.Cluster) { next.Spec.TLS.Mode = "preferTLS" }, []string{"tls-downgrade"}},
		{"tls-disabled", func(old, next *domain.Cluster) { old.Spec.TLS.Mode = "preferTLS"; next.Spec.TLS.Mode = "disabled" }, []string{"tls-downgrade"}},
		{"pitr", func(old, _ *domain.Cluster) { old.Spec.Backup.PITR.Enabled = true }, []string{"backups-disabled"}},
		{"schedule", func(old, _ *domain.Cluster) {
			old.Spec.Backup.Schedules = []domain.BackupSchedule{{Name: "daily", Enabled: true}}
		}, []string{"backups-disabled"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old, next := base, base
			tc.change(&old, &next)
			result := s.dryRunResult("patch", &old, &next, nil).body.(dryRunPreview)
			for _, kind := range tc.effects {
				requirePreviewEffect(t, result, kind)
			}
		})
	}
	clone := base
	clone.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "saved"}
	requirePreviewEffect(t, s.dryRunResult("create", nil, &clone, nil).body.(dryRunPreview), "clone")
	for _, tc := range []struct {
		old, next string
		widened   bool
	}{
		{"2001:db8::/32", "2001:db8:1::/48", false},
		{"2001:db8:1::/48", "2001:db8::/32", true},
		{"2001:db8::/32", "2001:db9::/32", true},
		{"2001:db8::/32", "2001:db8::/32", false},
	} {
		old := domain.Exposure{Public: true, IPType: "IPV6", Allowlist: []string{tc.old}}
		next := domain.Exposure{Public: true, IPType: "IPV6", Allowlist: []string{tc.next}}
		if got := exposureWidens(old, next); got != tc.widened {
			t.Fatalf("%s -> %s: widened=%v", tc.old, tc.next, got)
		}
	}
}
