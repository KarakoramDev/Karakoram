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

// The channel adapter's cancellation-error send can block after early return.
// Inspect selectors, not just calls: capturing a method value is equally unsafe.
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
		ast.Walk(&listingContractVisitor{report: func(pos token.Pos, message string) { t.Errorf("%s: %s", set.Position(pos), message) }}, file)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type listingContractVisitor struct {
	report               func(token.Pos, string)
	body                 *ast.BlockStmt
	params               *ast.FieldList
	parent               *listingContractVisitor
	directIteratorCallee *ast.SelectorExpr
}

func (v *listingContractVisitor) Visit(node ast.Node) ast.Visitor {
	switch value := node.(type) {
	case *ast.FuncDecl:
		return &listingContractVisitor{report: v.report, body: value.Body, params: value.Type.Params, parent: v}
	case *ast.FuncLit:
		return &listingContractVisitor{report: v.report, body: value.Body, params: value.Type.Params, parent: v}
	case *ast.SelectorExpr:
		if value.Sel.Name == "ListObjects" {
			v.report(value.Pos(), "channel ListObjects reference can strand a producer; use scoped ListObjectsIter")
		}
		if value.Sel.Name == "ListObjectsIter" && value != v.directIteratorCallee {
			v.report(value.Pos(), "iterator method values bypass cancellation ownership checks; invoke the scoped iterator directly")
		}
	case *ast.CallExpr:
		selector, ok := value.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "ListObjectsIter" {
			if !v.ownsContext(value) {
				v.report(value.Pos(), "iterator must use a local context.WithCancel/WithTimeout and defer its cancel")
			}
			child := *v
			child.directIteratorCallee = selector
			return &child
		}
	}
	return v
}
func (v *listingContractVisitor) ownsContext(call *ast.CallExpr) bool {
	if len(call.Args) == 0 {
		return false
	}
	ctx, ok := call.Args[0].(*ast.Ident)
	if !ok {
		return false
	}
	for scope := v; scope != nil; scope = scope.parent {
		if listingBodyOwnsContext(scope.body, call, ctx.Name) {
			return true
		}
		if scope.params != nil {
			for _, field := range scope.params.List {
				for _, name := range field.Names {
					if name.Name == ctx.Name {
						return false
					}
				}
			}
		}
	}
	return false
}
func listingBodyOwnsContext(body *ast.BlockStmt, listing *ast.CallExpr, ctx string) bool {
	if body == nil {
		return false
	}
	for _, statement := range body.List {
		assignment, ok := statement.(*ast.AssignStmt)
		if !ok || assignment.Pos() >= listing.Pos() || len(assignment.Lhs) != 2 || len(assignment.Rhs) != 1 {
			continue
		}
		name, ok := assignment.Lhs[0].(*ast.Ident)
		if !ok || name.Name != ctx {
			continue
		}
		cancel, ok := assignment.Lhs[1].(*ast.Ident)
		if !ok {
			continue
		}
		call, ok := assignment.Rhs[0].(*ast.CallExpr)
		if !ok {
			continue
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		namespace, ok := selector.X.(*ast.Ident)
		if !ok || namespace.Name != "context" {
			continue
		}
		switch selector.Sel.Name {
		case "WithCancel", "WithTimeout", "WithDeadline":
		default:
			continue
		}
		for _, statement := range body.List {
			deferred, ok := statement.(*ast.DeferStmt)
			if !ok || deferred.Pos() <= assignment.Pos() || deferred.Pos() >= listing.Pos() {
				continue
			}
			name, ok := deferred.Call.Fun.(*ast.Ident)
			if ok && name.Name == cancel.Name {
				return true
			}
		}
	}
	return false
}

func TestListingContractRejectsAliasesAndPackageInitializers(t *testing.T) {
	for _, source := range []string{
		`package p; func bad(ctx context.Context) { list := client.ListObjects; for range list(ctx, "b", options) {} }`,
		`package p; var list = client.ListObjects`,
		`package p; var objects = client.ListObjects(context.Background(), "b", options)`,
		`package p; var work = func(ctx context.Context) { for range client.ListObjects(ctx, "b", options) {} }`,
		`package p; var work = func(ctx context.Context) { for range client.ListObjectsIter(ctx, "b", options) {} }`,
		`package p; func bad(ctx context.Context) { list := client.ListObjectsIter; for range list(ctx, "b", options) {} }`,
		`package p; var list = client.ListObjectsIter`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "negative.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		failures := 0
		ast.Walk(&listingContractVisitor{report: func(token.Pos, string) { failures++ }}, file)
		if failures == 0 {
			t.Fatalf("unsafe listing escaped guard: %s", source)
		}
	}
	for _, source := range []string{
		`package p; func good(ctx context.Context) { ctx, cancel := context.WithCancel(ctx); defer cancel(); for range client.ListObjectsIter(ctx, "b", options) {} }`,
		`package p; var good = func(ctx context.Context) { ctx, cancel := context.WithTimeout(ctx, timeout); defer cancel(); for range client.ListObjectsIter(ctx, "b", options) {} }`,
		`package p; func good(ctx context.Context) { ctx, cancel := context.WithCancel(ctx); defer cancel(); go func() { for range client.ListObjectsIter(ctx, "b", options) {} }() }`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "positive.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Walk(&listingContractVisitor{report: func(_ token.Pos, message string) { t.Errorf("safe listing rejected: %s (%s)", source, message) }}, file)
	}
}
