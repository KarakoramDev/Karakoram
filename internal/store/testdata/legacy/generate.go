// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build ignore

// Run from a temporary module requiring modernc.org/sqlite v1.40.1:
// go run generate.go /path/to/repository /path/to/legacy-v1.40.1.db
package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

func main() {
	if err := generate(); err != nil {
		panic(err)
	}
}

func generate() error {
	db, err := sql.Open("sqlite", os.Args[2])
	if err != nil {
		return err
	}
	defer db.Close()
	var version string
	if err := db.QueryRow("SELECT sqlite_version()").Scan(&version); err != nil {
		return err
	}
	if version != "3.50.4" {
		return fmt.Errorf("fixture requires SQLite 3.50.4, got %s", version)
	}
	script, err := os.ReadFile(filepath.Join(os.Args[1], "internal/store/migrations/001.sql"))
	if err != nil {
		return err
	}
	if _, err := db.Exec(string(script)); err != nil {
		return err
	}
	_, err = db.Exec(`CREATE TABLE schema_migrations(name TEXT PRIMARY KEY);
 INSERT INTO schema_migrations VALUES('001.sql');
 INSERT INTO records(kind,scope,key,data,created,updated) VALUES('setting','','legacy-driver','{"sqlite":"3.50.4"}',1,1);`)
	return err
}
