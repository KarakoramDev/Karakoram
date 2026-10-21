// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"database/sql"
	"errors"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestDeletionDecision(t *testing.T) {
	original := encodeDeletionMarker("delete_one", "hash", "delete-prepared")
	foreign := encodeDeletionMarker("create_two", "other", "applied")
	for _, test := range []struct {
		name, saved, current string
		err                  error
		want                 string
	}{
		{"prepared same account", original, original, nil, "drop"},
		{"recreated account", original, foreign, nil, "conflict"},
		{"recreated unmarked account", original, encodeDeletionMarker("", "", ""), nil, "conflict"},
		{"initial absence then foreign creation", "", foreign, nil, "conflict"},
		{"drop committed before receipt", original, "", sql.ErrNoRows, "applied"},
		{"already absent", "", "", sql.ErrNoRows, "applied"},
		{"native observation failed", original, original, errors.New("connection lost"), "unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := deletionDecision(test.saved, test.current, test.err); got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}
func TestDeleteNativeFailure(t *testing.T) {
	for _, err := range []error{errors.New("transport lost"), &mysqldriver.MySQLError{Number: 1396, Message: "private details"}} {
		if got := deleteNativeFailure(err); got.Outcome != "unknown" {
			t.Fatalf("ambiguous deletion: %+v", got)
		}
	}
	got := deleteNativeFailure(&mysqldriver.MySQLError{Number: 1227, Message: "private details"})
	if got.Outcome != "rejected" || got.Status != 403 || got.Message != "MySQL rejected the account operation" {
		t.Fatalf("unsafe rejection: %+v", got)
	}
}
