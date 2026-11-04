// SPDX-License-Identifier: Apache-2.0
package cpbackup

import (
	"context"
	"database/sql"
	"net/url"
)

func snapshotSchema(ctx context.Context, path string) (string, error) {
	db, err := sql.Open("sqlite", "file:"+url.PathEscape(path)+"?mode=ro")
	if err != nil {
		return "", err
	}
	defer db.Close()
	var version string
	err = db.QueryRowContext(ctx, "SELECT COALESCE(MAX(name),'') FROM schema_migrations").Scan(&version)
	return version, err
}
