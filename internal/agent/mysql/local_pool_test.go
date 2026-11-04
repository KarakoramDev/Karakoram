// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

type localPoolConnector struct {
	groupTestConnector
	failure error
	pings   int
}

func (c *localPoolConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *localPoolConnector) Ping(context.Context) error {
	c.pings++
	return c.failure
}

func TestLocalAdministrationPoolRepairsOnlyKnownClosedState(t *testing.T) {
	for _, closed := range []bool{true, false} {
		t.Run(fmt.Sprintf("closed-db=%t", closed), func(t *testing.T) {
			originalDriver := &localPoolConnector{groupTestConnector: groupTestConnector{t: t}}
			if !closed {
				originalDriver.failure = fmt.Errorf("closed native session: %w", sql.ErrConnDone)
			}
			original := sql.OpenDB(originalDriver)
			defer original.Close()
			if closed {
				if err := original.Close(); err != nil {
					t.Fatal(err)
				}
			}
			replacementDriver := &localPoolConnector{groupTestConnector: groupTestConnector{t: t}}
			replacement := sql.OpenDB(replacementDriver)
			defer replacement.Close()
			opens := 0
			open := func() (*sql.DB, error) { opens++; return replacement, nil }
			pool, err := availableLocalPool(t.Context(), original, open)
			if err != nil || pool != replacement || opens != 1 || replacementDriver.pings != 1 {
				t.Fatalf("known closed pool was not replaced exactly once: pool=%p err=%v opens=%d pings=%d", pool, err, opens, replacementDriver.pings)
			}
			pool, err = availableLocalPool(t.Context(), pool, open)
			if err != nil || pool != replacement || opens != 1 || replacementDriver.pings != 2 {
				t.Fatal("ordinary cached administration access reopened a healthy replacement")
			}
		})
	}
}

func TestLocalAdministrationPoolPreservesOrdinaryPingFailures(t *testing.T) {
	for name, failure := range map[string]error{
		"authentication":        &mysqldriver.MySQLError{Number: 1045, Message: "Access denied"},
		"deadline":              context.DeadlineExceeded,
		"cancellation":          context.Canceled,
		"unrelated-closed-text": errors.New("query failed while checking whether database is closed"),
	} {
		t.Run(name, func(t *testing.T) {
			connector := &localPoolConnector{groupTestConnector: groupTestConnector{t: t}, failure: failure}
			original := sql.OpenDB(connector)
			defer original.Close()
			opens := 0
			pool, err := availableLocalPool(t.Context(), original, func() (*sql.DB, error) {
				opens++
				return nil, errors.New("unexpected replacement")
			})
			if pool != original || err != failure || opens != 0 || connector.pings != 1 {
				t.Fatalf("ordinary SQL failure was hidden or retried: err=%v opens=%d pings=%d", err, opens, connector.pings)
			}
		})
	}
}

func TestLocalAdministrationPoolSurfacesReplacementPingFailure(t *testing.T) {
	original := sql.OpenDB(&localPoolConnector{groupTestConnector: groupTestConnector{t: t}})
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	failure := &mysqldriver.MySQLError{Number: 1045, Message: "Access denied after replacement"}
	connector := &localPoolConnector{groupTestConnector: groupTestConnector{t: t}, failure: failure}
	replacement := sql.OpenDB(connector)
	defer replacement.Close()
	opens := 0
	open := func() (*sql.DB, error) { opens++; return replacement, nil }
	pool, err := availableLocalPool(t.Context(), original, open)
	if pool != replacement || err != failure || opens != 1 || connector.pings != 1 {
		t.Fatalf("replacement failure was hidden or retried: %v opens=%d pings=%d", err, opens, connector.pings)
	}
	if _, err := availableLocalPool(t.Context(), pool, open); err != failure || opens != 1 || connector.pings != 2 {
		t.Fatal("replacement's ordinary SQL failure triggered another lifecycle repair")
	}
}
