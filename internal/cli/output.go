// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
)

func (a *app) print(v any) error {
	if a.format == "json" {
		e := json.NewEncoder(a.opts.Out)
		e.SetIndent("", "  ")
		return e.Encode(v)
	}
	if a.format == "yaml" {
		e := yaml.NewEncoder(a.opts.Out)
		defer e.Close()
		e.SetIndent(2)
		return e.Encode(v)
	}
	if s, ok := v.(string); ok {
		_, err := fmt.Fprint(a.opts.Out, s)
		if !strings.HasSuffix(s, "\n") {
			fmt.Fprintln(a.opts.Out)
		}
		return err
	}
	if m, ok := v.(map[string]any); ok {
		if items, ok := m["items"].([]any); ok {
			if err := a.table(items); err != nil {
				return err
			}
			if cursor, ok := m["nextCursor"].(string); ok && cursor != "" {
				fmt.Fprintf(a.opts.Err, "Next page: --cursor %s\n", cursor)
			}
			return nil
		}
		// Detailed resources retain every field, including catalog versions and upgrade paths.
		w := tabwriter.NewWriter(a.opts.Out, 0, 4, 2, ' ', 0)
		keys := sortedKeys(m)
		for _, k := range keys {
			fmt.Fprintf(w, "%s\t%s\n", k, cell(m[k]))
		}
		return w.Flush()
	}
	if rows, ok := v.([]any); ok {
		return a.table(rows)
	}
	_, err := fmt.Fprintln(a.opts.Out, cell(v))
	return err
}
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
func cell(v any) string {
	var s string
	if v == nil {
		return "-"
	}
	if str, ok := v.(string); ok {
		s = str
	} else if f, ok := v.(float64); ok {
		s = strconv.FormatFloat(f, 'f', -1, 64)
	} else if m, ok := v.(map[string]any); ok {
		parts := make([]string, 0, len(m))
		for _, key := range sortedKeys(m) {
			parts = append(parts, key+"="+cell(m[key]))
		}
		s = strings.Join(parts, "; ")
	} else if values, ok := v.([]any); ok {
		parts := make([]string, 0, len(values))
		for _, value := range values {
			if m, ok := value.(map[string]any); ok {
				if name, ok := m["name"].(string); ok {
					parts = append(parts, name)
					continue
				}
				if version, ok := m["version"].(string); ok {
					parts = append(parts, version)
					continue
				}
			}
			parts = append(parts, cell(value))
		}
		s = strings.Join(parts, ", ")
	} else {
		s = fmt.Sprint(v)
	}
	return strings.Map(func(r rune) rune {
		if r < ' ' || r == 127 {
			return ' '
		}
		return r
	}, s)
}
func (a *app) table(rows []any) error {
	if len(rows) == 0 {
		_, err := fmt.Fprintln(a.opts.Out, "No results.")
		return err
	}
	all := map[string]any{}
	for _, row := range rows {
		if m, ok := row.(map[string]any); ok {
			for k := range m {
				all[k] = nil
			}
		}
	}
	keys := sortedKeys(all)
	preferred := []string{"namespace", "name", "id", "username", "cluster", "resource", "target", "database", "type", "phase", "actor", "state", "version", "role", "health", "message"}
	ordered := make([]string, 0, len(keys))
	for _, p := range preferred {
		if _, ok := all[p]; ok {
			ordered = append(ordered, p)
			delete(all, p)
		}
	}
	ordered = append(ordered, sortedKeys(all)...)
	w := tabwriter.NewWriter(a.opts.Out, 0, 4, 2, ' ', 0)
	for i, k := range ordered {
		if i > 0 {
			fmt.Fprint(w, "\t")
		}
		fmt.Fprint(w, strings.ToUpper(k))
	}
	fmt.Fprintln(w)
	for _, row := range rows {
		m, ok := row.(map[string]any)
		if !ok {
			fmt.Fprintln(w, cell(row))
			continue
		}
		for i, k := range ordered {
			if i > 0 {
				fmt.Fprint(w, "\t")
			}
			v := m[k]
			if k == "status" {
				if status, ok := v.(map[string]any); ok {
					v = status["phase"]
				}
			}
			fmt.Fprint(w, cell(v))
		}
		fmt.Fprintln(w)
	}
	return w.Flush()
}

func (a *app) orphanedVolumesTable(v any) error {
	list, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("unexpected orphaned volumes response")
	}
	rows, ok := list["items"].([]any)
	if !ok {
		return fmt.Errorf("unexpected orphaned volumes items")
	}
	if len(rows) == 0 {
		_, err := fmt.Fprintln(a.opts.Out, "No results.")
		return err
	}
	w := tabwriter.NewWriter(a.opts.Out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAMESPACE\tHOSTNAME\tSERVICE ID\tCLUSTER\tMEMBER\tATTEMPTS\tNEXT ATTEMPT\tLAST PROCESS")
	for _, row := range rows {
		m, ok := row.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected orphaned volume record")
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			cell(m["namespace"]), cell(m["hostname"]), cell(m["serviceId"]), cell(m["cluster"]),
			cell(m["member"]), cell(m["attempts"]), cell(m["nextAttemptAt"]),
			cell(m["lastProcessId"]))
	}
	return w.Flush()
}
