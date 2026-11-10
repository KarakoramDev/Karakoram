// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestPeriodicConfigPullSchedulerContract guards the actual production Run
// source, independently of provider-budget simulations. It is a source contract,
// not virtual-time execution of Run's TLS/native lifecycle: one 60s ticker and
// one unconditional reload dispatch per tick. Cadence changes or duplicate/
// conditional dispatch require updating this contract and request-budget inputs.
func TestPeriodicConfigPullSchedulerContract(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "agent.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var run *ast.FuncDecl
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == "Run" {
			run = fn
			break
		}
	}
	if run == nil {
		t.Fatal("production Run declaration missing")
	}
	selector := func(expr ast.Expr, object, name string) bool {
		field, ok := expr.(*ast.SelectorExpr)
		if !ok || field.Sel.Name != name {
			return false
		}
		value, ok := field.X.(*ast.Ident)
		return ok && value.Name == object
	}
	tickerName := ""
	tickerCalls := 0
	ast.Inspect(run.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !selector(call.Fun, "time", "NewTicker") {
			return true
		}
		tickerCalls++
		if len(call.Args) != 1 {
			t.Fatal("production ticker has unexpected arguments")
		}
		interval, ok := call.Args[0].(*ast.BinaryExpr)
		if !ok || interval.Op != token.MUL || !selector(interval.Y, "time", "Second") {
			t.Fatal("production periodic interval is no longer the explicit60s contract")
		}
		seconds, ok := interval.X.(*ast.BasicLit)
		if !ok || seconds.Kind != token.INT || seconds.Value != "60" {
			t.Fatal("production periodic cadence changed from60s")
		}
		return true
	})
	ast.Inspect(run.Body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || len(assignment.Rhs) != 1 {
			return true
		}
		call, ok := assignment.Rhs[0].(*ast.CallExpr)
		if !ok || !selector(call.Fun, "time", "NewTicker") {
			return true
		}
		if len(assignment.Lhs) != 1 {
			t.Fatal("periodic ticker binding changed")
		}
		name, ok := assignment.Lhs[0].(*ast.Ident)
		if !ok {
			t.Fatal("periodic ticker binding is not named")
		}
		tickerName = name.Name
		return true
	})
	if tickerCalls != 1 || tickerName == "" {
		t.Fatal("production Run must have exactly one bound periodic ticker", tickerCalls, tickerName)
	}
	dispatches := 0
	ast.Inspect(run.Body, func(node ast.Node) bool {
		clause, ok := node.(*ast.CommClause)
		if !ok {
			return true
		}
		expression, ok := clause.Comm.(*ast.ExprStmt)
		if !ok {
			return true
		}
		receive, ok := expression.X.(*ast.UnaryExpr)
		if !ok || receive.Op != token.ARROW || !selector(receive.X, tickerName, "C") {
			return true
		}
		dispatches++
		if len(clause.Body) != 1 {
			t.Fatal("periodic tick has extra dispatch statements")
		}
		statement, ok := clause.Body[0].(*ast.IfStmt)
		if !ok {
			t.Fatal("periodic reload dispatch structure changed")
		}
		init, ok := statement.Init.(*ast.AssignStmt)
		if !ok || len(init.Rhs) != 1 {
			t.Fatal("periodic reload is no longer unconditional per tick")
		}
		call, ok := init.Rhs[0].(*ast.CallExpr)
		if !ok || !selector(call.Fun, "a", "reload") || len(call.Args) != 1 {
			t.Fatal("periodic tick no longer invokes the authoritative reload path exactly once")
		}
		ctx, ok := call.Args[0].(*ast.Ident)
		if !ok || ctx.Name != "ctx" {
			t.Fatal("periodic reload lost its agent lifetime context")
		}
		reloads := 0
		ast.Inspect(clause, func(n ast.Node) bool {
			switch n.(type) {
			case *ast.ForStmt, *ast.RangeStmt, *ast.GoStmt, *ast.FuncLit:
				t.Fatal("periodic tick can repeat or fork reload work")
			}
			if c, ok := n.(*ast.CallExpr); ok && selector(c.Fun, "a", "reload") {
				reloads++
			}
			return true
		})
		if reloads != 1 {
			t.Fatal("periodic tick dispatches more than one reload", reloads)
		}
		return false
	})
	if dispatches != 1 {
		t.Fatal("production Run must receive its periodic ticker in exactly one select arm", dispatches)
	}
}
