// SPDX-License-Identifier: Apache-2.0
package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// snapshotConnector mutates the channel after the record SELECT releases its
// rows, exactly where the former second credentials SELECT could see a new row.
// It also proves that no read transaction remains open during the mutation.
type snapshotConnector struct {
	db        *sql.DB
	afterRead func()
}

func (c *snapshotConnector) Connect(context.Context) (driver.Conn, error) {
	return &snapshotConn{connector: c}, nil
}
func (*snapshotConnector) Driver() driver.Driver { return snapshotDriver{} }

type snapshotDriver struct{}

func (snapshotDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type snapshotConn struct{ connector *snapshotConnector }

func (*snapshotConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (*snapshotConn) Close() error              { return nil }
func (*snapshotConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c *snapshotConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values := make([]any, len(args))
	for i := range args {
		values[i] = args[i].Value
	}
	rows, err := c.connector.db.QueryContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, err
	}
	afterRead := c.connector.afterRead
	c.connector.afterRead = nil
	return &snapshotRows{rows: rows, columns: columns, afterRead: afterRead}, nil
}

type snapshotRows struct {
	rows      *sql.Rows
	columns   []string
	afterRead func()
}

func (r *snapshotRows) Columns() []string { return r.columns }
func (r *snapshotRows) Close() error {
	err := r.rows.Close()
	if r.afterRead != nil {
		afterRead := r.afterRead
		r.afterRead = nil
		afterRead()
	}
	return err
}
func (r *snapshotRows) Next(dest []driver.Value) error {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	values := make([]any, len(dest))
	for i := range dest {
		values[i] = &dest[i]
	}
	return r.rows.Scan(values...)
}

func TestNotificationDeliverySnapshotSurvivesConcurrentMutation(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name := "replace"
		if remove {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := t.Context()
			channel := domain.NotificationChannel{Name: "mail", Type: "email", Enabled: true,
				Email:   &domain.NotificationEmail{Host: "smtp-a.example", Username: "user-a"},
				Secrets: domain.NotificationChannelSecrets{EmailPassword: "password-a"}}
			if err := s.SaveNotificationChannel(ctx, &channel, true); err != nil {
				t.Fatal(err)
			}
			original := s.reader
			mutated := false
			wrapped := sql.OpenDB(&snapshotConnector{db: original, afterRead: func() {
				mutated = true
				if remove {
					if err := s.DeleteNotificationChannel(ctx, channel.Name); err != nil {
						t.Fatal(err)
					}
					return
				}
				channel.Email.Host = "smtp-b.example"
				channel.Email.Username = "user-b"
				channel.Secrets.EmailPassword = "password-b"
				if err := s.SaveNotificationChannel(ctx, &channel, false); err != nil {
					t.Fatal(err)
				}
			}})
			s.reader = wrapped
			defer func() { s.reader = original; wrapped.Close() }()
			got, err := s.NotificationChannelForDelivery(ctx, channel.Name)
			if err != nil {
				t.Fatal(err)
			}
			if !mutated || got.Email.Host != "smtp-a.example" || got.Email.Username != "user-a" || got.Secrets.EmailPassword != "password-a" {
				t.Fatalf("mixed snapshot: host=%s user=%s mutated=%v", got.Email.Host, got.Email.Username, mutated)
			}
			next, err := s.NotificationChannelForDelivery(ctx, channel.Name)
			if remove {
				if !errors.Is(err, ErrNotFound) {
					t.Fatal("deleted channel still readable", err)
				}
			} else if err != nil || next.Email.Host != "smtp-b.example" || next.Secrets.EmailPassword != "password-b" {
				t.Fatal("replacement not visible", err)
			}
		})
	}
}
