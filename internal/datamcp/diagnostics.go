// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package datamcp

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"reflect"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/oauth2"
)

const maxDiagnosticErrors = 16

// listToolsFailure contains only type metadata and fixed classifications, never
// error text or error values. Walk nodes directly: errors.Is/As would recurse
// without a bound on cyclic or very large error trees.
type listToolsFailure struct {
	types            []string
	truncated        bool
	category         string
	contextCanceled  bool
	deadlineExceeded bool
	eof              bool
	netClosed        bool
	sessionMissing   bool
	connectionClosed bool
}

func classifyListToolsFailure(err error) listToolsFailure {
	d := listToolsFailure{category: "unknown"}
	var pending [maxDiagnosticErrors]error
	n := 0
	if err != nil {
		pending[0], n = err, 1
	}
	priority := 0
	for n > 0 && len(d.types) < maxDiagnosticErrors {
		n--
		e := pending[n]
		pending[n] = nil
		if e == nil {
			continue
		}
		t := reflect.TypeOf(e)
		name := "unnamed"
		base := t
		for base.Kind() == reflect.Pointer {
			base = base.Elem()
		}
		if base.Name() != "" {
			name = t.String()
			if len(name) > 160 {
				name = name[:160]
			}
		}
		d.types = append(d.types, name)
		if t.Comparable() {
			switch e {
			case context.Canceled:
				d.contextCanceled = true
			case context.DeadlineExceeded:
				d.deadlineExceeded = true
			case io.EOF:
				d.eof = true
			case net.ErrClosed:
				d.netClosed = true
			case mcp.ErrSessionMissing:
				d.sessionMissing = true
			case mcp.ErrConnectionClosed:
				d.connectionClosed = true
			}
		}
		category, rank := "unknown", 0
		switch v := e.(type) {
		case *oauth2.RetrieveError:
			if v != nil && v.Response != nil {
				switch v.Response.StatusCode / 100 {
				case 4:
					category, rank = "http_4xx", 4
				case 5:
					category, rank = "http_5xx", 4
				}
			}
		case *jsonrpc.Error:
			category, rank = "jsonrpc", 3
		case *json.SyntaxError, *json.UnmarshalTypeError:
			category, rank = "schema_decoding", 2
		case net.Error:
			category, rank = "transport", 1
		}
		if rank > priority {
			d.category, priority = category, rank
		}
		// Cap pending children as well as visited nodes. Repeated/cyclic nodes
		// consume the same budget; no map or unbounded recursive traversal.
		switch v := e.(type) {
		case interface{ Unwrap() []error }:
			children := v.Unwrap()
			room := maxDiagnosticErrors - n
			if len(children) > room {
				children = children[:room]
				d.truncated = true
			}
			for i := len(children) - 1; i >= 0; i-- {
				if children[i] != nil {
					pending[n] = children[i]
					n++
				}
			}
		case interface{ Unwrap() error }:
			if child := v.Unwrap(); child != nil {
				pending[n] = child
				n++
			}
		}
	}
	d.truncated = d.truncated || n > 0
	if priority == 0 && (d.eof || d.netClosed || d.connectionClosed || d.sessionMissing) {
		d.category = "transport"
	}
	return d
}

func (d listToolsFailure) attrs() []slog.Attr {
	return []slog.Attr{
		slog.Any("errorTypes", d.types),
		slog.Bool("errorChainTruncated", d.truncated),
		slog.String("upstreamCategory", d.category),
		slog.Bool("contextCanceled", d.contextCanceled),
		slog.Bool("deadlineExceeded", d.deadlineExceeded),
		slog.Bool("eof", d.eof),
		slog.Bool("netClosed", d.netClosed),
		slog.Bool("sessionMissing", d.sessionMissing),
		slog.Bool("connectionClosed", d.connectionClosed),
	}
}
