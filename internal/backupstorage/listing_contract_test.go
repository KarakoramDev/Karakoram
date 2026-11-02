// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package backupstorage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// MinIO's channel ListObjects adapter requires complete draining, even after
// cancellation: its cancellation-error send can block on a full output channel.
// Production listings must use the synchronous iterator and own cancellation.
func TestProductionS3ListingsUseOwnedIterators(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source path unavailable")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	set := token.NewFileSet()
	err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&fs.ModeSymlink != 0 || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(set, name, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch selector.Sel.Name {
				case "ListObjects":
					t.Errorf("%s: channel ListObjects can strand a producer; use scoped ListObjectsIter", set.Position(call.Pos()))
				case "ListObjectsIter":
					if !listingOwnsContext(function, call) {
						t.Errorf("%s: iterator must use a local context.WithCancel/WithTimeout and defer its cancel", set.Position(call.Pos()))
					}
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func listingOwnsContext(function *ast.FuncDecl, listing *ast.CallExpr) bool {
	if len(listing.Args) == 0 {
		return false
	}
	ctx, ok := listing.Args[0].(*ast.Ident)
	if !ok {
		return false
	}
	owned := false
	ast.Inspect(function.Body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || assignment.Pos() >= listing.Pos() || len(assignment.Lhs) != 2 || len(assignment.Rhs) != 1 {
			return true
		}
		name, ok := assignment.Lhs[0].(*ast.Ident)
		if !ok || name.Name != ctx.Name {
			return true
		}
		cancel, ok := assignment.Lhs[1].(*ast.Ident)
		if !ok {
			return true
		}
		call, ok := assignment.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		namespace, ok := selector.X.(*ast.Ident)
		if !ok || namespace.Name != "context" {
			return true
		}
		switch selector.Sel.Name {
		case "WithCancel", "WithTimeout", "WithDeadline":
		default:
			return true
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			deferred, ok := node.(*ast.DeferStmt)
			if !ok || deferred.Pos() <= assignment.Pos() || deferred.Pos() >= listing.Pos() {
				return true
			}
			name, ok := deferred.Call.Fun.(*ast.Ident)
			if ok && name.Name == cancel.Name {
				owned = true
			}
			return true
		})
		return true
	})
	return owned
}
