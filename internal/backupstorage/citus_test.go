// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package backupstorage

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
)

func TestCitusRepositoryOwnership(t *testing.T) {
	storage := domain.BackupStorage{Namespace: "default"}
	for _, repo := range []string{"prefix/postgres/default/demo-incarnation/g0", "prefix/postgres/default/demo-incarnation/g8", "prefix/postgres/default/demo-incarnation-g4/g1"} {
		got, err := PostgresDirectory(storage, "default", "demo", repo)
		if err != nil || got != repo+"/" {
			t.Fatal(got, err)
		}
	}
	for _, suffix := range []string{"g0-i1", "g1-i12", "g8-i999"} {
		repo := "prefix/postgres/default/demo-incarnation/" + suffix
		if got, err := PostgresDirectory(storage, "default", "demo", repo); err != nil || got != repo+"/" {
			t.Fatal(got, err)
		}
	}
	for _, suffix := range []string{"g9-i1", "g1-i0", "g1-i01", "g1-i-1", "g1-i", "g1-i1/g2", "g1-i1/..", "g1-i1x"} {
		if _, err := PostgresDirectory(storage, "default", "demo", "prefix/postgres/default/demo-incarnation/"+suffix); err == nil {
			t.Fatal("unsafe incarnation accepted", suffix)
		}
	}
	for _, repo := range []string{"prefix/postgres/default/demo-incarnation/g9", "prefix/postgres/other/demo-incarnation/g0", "prefix/postgres/default/foreign-incarnation/g0", "prefix/postgres/default/demo-incarnation/g0/..", "prefix/postgres/default/demo-incarnation/g0/g1"} {
		if _, err := PostgresDirectory(storage, "default", "demo", repo); err == nil {
			t.Fatal("unsafe group repository accepted", repo)
		}
	}
}
