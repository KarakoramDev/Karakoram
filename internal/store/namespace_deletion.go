// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// NamespaceConflictError identifies the resources or state blocking a transition.
type NamespaceConflictError struct {
	Namespace string
	Blockers  []string
}

func (e *NamespaceConflictError) Error() string {
	return "Namespace " + e.Namespace + ": " + strings.Join(e.Blockers, ", ")
}
func (e *NamespaceConflictError) Unwrap() error { return ErrConflict }

func namespaceForInsert(ctx context.Context, tx *sql.Tx, namespace string) error {
	namespace = domain.NormalizedNamespace(namespace)
	if err := domain.ValidateNamespaceName(namespace); err != nil {
		return err
	}
	if namespace == domain.DefaultNamespace {
		return nil
	}
	var data []byte
	if err := tx.QueryRowContext(ctx, "SELECT data FROM namespaces WHERE name=?", namespace).Scan(&data); err != nil {
		if err == sql.ErrNoRows {
			return &NamespaceConflictError{Namespace: namespace, Blockers: []string{"does not exist"}}
		}
		return err
	}
	var n domain.Namespace
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	if n.Phase == "Deleting" {
		return &NamespaceConflictError{Namespace: namespace, Blockers: []string{"deletion is in progress"}}
	}
	return nil
}

// BeginNamespaceDeletion checks inventory and establishes deletion intent in the
// same serialized writer transaction used by cluster and storage creation.
func (s *Store) BeginNamespaceDeletion(ctx context.Context, namespace string) error {
	if err := domain.ValidateNamespaceName(namespace); err != nil {
		return err
	}
	if namespace == domain.DefaultNamespace {
		return &NamespaceConflictError{Namespace: namespace, Blockers: []string{"default namespace cannot be deleted"}}
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var data []byte
	if err = tx.QueryRowContext(ctx, "SELECT data FROM namespaces WHERE name=?", namespace).Scan(&data); err != nil {
		return typed(err)
	}
	var n domain.Namespace
	if err = json.Unmarshal(data, &n); err != nil {
		return err
	}
	blockers := []string{}
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM clusters WHERE namespace=?", namespace).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		blockers = append(blockers, fmt.Sprintf("%d clusters", count))
	}
	for _, item := range []struct{ kind, label string }{{"storage", "backup storages"}, {"backup", "backup records"}, {"orphaned-volume", "orphaned volume records"}} {
		scope := " AND substr(scope,1,?)=?"
		args := []any{item.kind, len(namespace) + 1, namespace + ":"}
		if item.kind != "backup" {
			scope = " AND scope=?"
			args = []any{item.kind, namespace}
		}
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind=?"+scope, args...).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			blockers = append(blockers, fmt.Sprintf("%d %s", count, item.label))
		}
	}
	if len(blockers) != 0 {
		return &NamespaceConflictError{Namespace: namespace, Blockers: blockers}
	}
	n.Phase = "Deleting"
	n.UpdatedAt = time.Now().UTC()
	if err = putNamespace(ctx, tx, &n); err != nil {
		return err
	}
	return tx.Commit()
}
