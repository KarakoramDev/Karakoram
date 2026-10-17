// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func postgresInfoFixture() []byte {
	return renderPostgresInfo([]infoEntry{
		{"backrest", "backrest-checksum", `""`},
		{"backrest", "backrest-format", `5`},
		{"backrest", "backrest-version", `"2.59.1"`},
		{"backup:current", "20261017-021859F", `{"backup-archive-start":"00000001000000000000000A","backup-prior":null,"backup-reference":null,"db-id":1}`},
		{"backup:current", "20261017-021859F_20261017-021921D", `{"backup-prior":"20261017-021859F","backup-reference":["20261017-021859F"],"db-id":1}`},
		{"backup:current", "20261017-021859F_20261017-021931I", `{"backup-prior":"20261017-021859F_20261017-021921D","backup-reference":["20261017-021859F","20261017-021859F_20261017-021921D"],"db-id":1}`},
		{"backup:current", "20261018-021859F", `{"backup-archive-start":"000000020000000000000020","backup-prior":null,"backup-reference":null,"db-id":1}`},
		{"db", "db-id", `1`},
		{"db:history", "1", `{"db-system-id":7620575555724598831,"db-version":"17"}`},
	})
}
func TestPostgresExpirationPlan(t *testing.T) {
	data := postgresInfoFixture()
	plan, err := planPostgresExpire(data, "20261017-021859F")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Empty || len(plan.Removed) != 3 || plan.Oldest[1] != "000000020000000000000020" {
		t.Fatalf("invalid expiration plan: %+v", plan)
	}
	if _, err = parsePostgresInfo(plan.Info); err != nil {
		t.Fatal("rewritten checksum invalid", err)
	}
	if strings.Contains(string(plan.Info), "20261017-021859F") || !strings.Contains(string(plan.Info), "20261018-021859F") {
		t.Fatal("dependency closure incorrect")
	}
	last, err := planPostgresExpire(plan.Info, "20261018-021859F")
	if err != nil || !last.Empty {
		t.Fatal("last set must retire whole repository", err)
	}
	corrupted := strings.Replace(string(data), `"17"`, `"18"`, 1)
	if _, err = parsePostgresInfo([]byte(corrupted)); err == nil {
		t.Fatal("corrupt metadata accepted")
	}
}
func TestPostgresRepositoryProvenance(t *testing.T) {
	storage := domain.BackupStorage{Namespace: "prod", Prefix: "archive"}
	good := "archive/postgres/prod/orders-incarnation"
	if directory, err := PostgresDirectory(storage, "prod", "orders", good); err != nil || directory != good+"/" {
		t.Fatal(directory, err)
	}
	storage.Prefix = "/replacement-prefix/"
	if directory, err := PostgresDirectory(storage, "prod", "orders", good); err != nil || directory != good+"/" {
		t.Fatal("changing storage prefix invalidated recorded repository provenance", directory, err)
	}
	for _, prefix := range []string{"archive/postgres/prod/orders-", "archive/postgres/dev/orders-incarnation", "archive/postgres/prod/orders-incarnation/other", "archive/postgres/prod/other-incarnation", "archive/postgres/prod/orders-../..", "archive/other/orders-incarnation", "/" + good} {
		if _, err := PostgresDirectory(storage, "prod", "orders", prefix); err == nil {
			t.Fatal("unsafe repository accepted", prefix)
		}
	}
	if _, err := PostgresDirectory(storage, "dev", "orders", good); err == nil {
		t.Fatal("cross-namespace storage accepted")
	}
}
