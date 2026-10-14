// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/version"
)

func TestAbsentResponseValuesAreOmittedOrEmptyArrays(t *testing.T) {
	spec := specAPI(domain.ClusterSpec{})
	if spec.Exposure.Allowlist == nil || *spec.Exposure.Allowlist == nil {
		t.Fatal("allowlist must be []")
	}
	if spec.Backup.Storage != nil || spec.Backup.Pitr != nil {
		t.Fatal("absent backup storage and PITR must be omitted")
	}
	connection := connectionAPI(domain.Connection{})
	if connection.Hosts == nil || connection.PublicHosts == nil || *connection.PublicHosts == nil || connection.ReplicaSet != nil {
		t.Fatal("empty connection arrays/optional replicaSet", connection)
	}
	if event := eventAPI(domain.Event{Type: "Audit"}); event.Cluster != nil || event.Namespace != nil {
		t.Fatal("global audit identifiers must be omitted", event)
	}
	if backup := backupAPI(domain.Backup{}); backup.Schedule != nil {
		t.Fatal("manual backup schedule must be omitted")
	}
	if restore := restoreAPI(domain.Restore{}); restore.Source.Backup != nil {
		t.Fatal("PITR restore backup selector must be omitted")
	}
	spec = specAPI(domain.ClusterSpec{Backup: domain.BackupConfig{PITR: domain.PITRConfig{Enabled: true, OplogSpanMinutes: 10}}})
	if spec.Backup.Pitr == nil || spec.Backup.Pitr.OplogSpanMinutes == nil || *spec.Backup.Pitr.OplogSpanMinutes != 10 {
		t.Fatal("configured PITR span lost")
	}
	spec = specAPI(domain.ClusterSpec{Backup: domain.BackupConfig{Storage: "archive", Schedules: []domain.BackupSchedule{{Name: "daily", RetainCount: 1}}}})
	if (*spec.Backup.Schedules)[0].Storage != nil || specDomain(spec).Backup.Schedules[0].Storage != "" {
		t.Fatal("an inherited schedule storage override must be omitted and round-trip as absent")
	}
}

func TestVersionExplainsMissingBuildMetadata(t *testing.T) {
	oldVersion, oldCommit := version.Version, version.Commit
	t.Cleanup(func() { version.Version, version.Commit = oldVersion, oldCommit })
	version.Version, version.Commit = "dev", "unknown"
	s, _, _ := testServer(t)
	w := call(s, "GET", "/api/v1/version", "", "")
	var body struct{ Version, Commit string }
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || body.Version != "unknown (built without git metadata)" || body.Commit != body.Version {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestMetricsAcceptsSignedInBrowserSession(t *testing.T) {
	s, _, _ := testServer(t)
	login := call(s, "POST", "/api/v1/auth/login", `{"username":"admin","password":"admin-test-password"}`, "")
	if login.Code != 200 {
		t.Fatal(login.Code, login.Body.String())
	}
	r := httptest.NewRequest("GET", "/metrics", nil)
	for _, cookie := range login.Result().Cookies() {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	testHandler(s).ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if unauthenticated := call(s, "GET", "/metrics", "", ""); unauthenticated.Code != 401 {
		t.Fatal(unauthenticated.Code)
	}
}
