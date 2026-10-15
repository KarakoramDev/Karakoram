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
	"unicode/utf8"
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
		w := tabwriter.NewWriter(a.opts.Out, 0, 4, 2, ' ', 0)
		detailFields(w, m, "")
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

func nestedTableValue(v any) bool {
	switch value := v.(type) {
	case map[string]any:
		return true
	case []any:
		for _, item := range value {
			switch item.(type) {
			case map[string]any, []any:
				return true
			}
		}
	}
	return false
}

func compactObjectCell(m map[string]any) (string, bool) {
	if len(m) > 3 {
		return "", false
	}
	for _, value := range m {
		switch value.(type) {
		case nil, string, bool, float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		default:
			return "", false
		}
	}
	if len(m) == 0 {
		return "-", true
	}
	var text strings.Builder
	kind, hasKind := m["kind"].(string)
	name, hasName := m["name"].(string)
	target := hasKind && hasName
	if target {
		text.WriteString(cell(kind))
		text.WriteByte('/')
		text.WriteString(cell(name))
	}
	for _, key := range sortedKeys(m) {
		if target && (key == "kind" || key == "name") {
			continue
		}
		if text.Len() > 0 {
			text.WriteByte(',')
		}
		text.WriteString(cell(key))
		text.WriteByte('=')
		text.WriteString(cell(m[key]))
	}
	summary := text.String()
	return summary, utf8.RuneCountInString(summary) <= 40
}

// Detail fields retain hierarchy without squeezing whole documents into a cell.
func detailFields(w *tabwriter.Writer, m map[string]any, prefix string) {
	for _, key := range sortedKeys(m) {
		label := prefix + key
		switch value := m[key].(type) {
		case map[string]any:
			fmt.Fprintf(w, "%s:\n", label)
			detailFields(w, value, prefix+"  ")
		case []any:
			if !nestedTableValue(value) {
				fmt.Fprintf(w, "%s\t%s\n", label, cell(value))
				continue
			}
			fmt.Fprintf(w, "%s:\n", label)
			for i, item := range value {
				fmt.Fprintf(w, "%s  %d:\n", prefix, i+1)
				if fields, ok := item.(map[string]any); ok {
					detailFields(w, fields, prefix+"    ")
				} else {
					fmt.Fprintf(w, "%s    %s\n", prefix, cell(item))
				}
			}
		default:
			fmt.Fprintf(w, "%s\t%s\n", label, cell(value))
		}
	}
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
	summarized := false
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
			if nestedTableValue(v) {
				switch value := v.(type) {
				case map[string]any:
					if summary, compact := compactObjectCell(value); compact {
						fmt.Fprint(w, summary)
					} else {
						summarized = true
						fmt.Fprintf(w, "%d fields", len(value))
					}
				case []any:
					summarized = true
					fmt.Fprintf(w, "%d items", len(value))
				}
			} else {
				fmt.Fprint(w, cell(v))
			}
		}
		fmt.Fprintln(w)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if summarized {
		fmt.Fprintln(w, "Nested fields summarized. Use -o json or -o yaml for full details.")
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

func providerDefault(m map[string]any) string {
	versions, _ := m["versions"].([]any)
	for _, version := range versions {
		if nested(version, "default") == true {
			return cell(nested(version, "version"))
		}
	}
	return "-"
}

func topologyDefaults(topology any) string {
	defaults, _ := nested(topology, "defaults").(map[string]any)
	parts := []string{}
	if components, ok := defaults["components"].(map[string]any); ok {
		for _, name := range sortedKeys(components) {
			if replicas := nested(components[name], "replicas"); replicas != nil {
				parts = append(parts, name+" replicas="+cell(replicas))
			}
		}
	}
	if shards := nested(defaults, "topology", "shards"); shards != nil {
		parts = append(parts, "shards="+cell(shards))
	}
	if tls := nested(defaults, "tls", "mode"); tls != nil {
		parts = append(parts, "TLS="+cell(tls))
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, ", ")
}

func (a *app) providersTable(v any, list bool) error {
	if list {
		items, _ := nested(v, "items").([]any)
		rows := make([]any, 0, len(items))
		for _, item := range items {
			provider, ok := item.(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected provider record")
			}
			topologies, _ := provider["topologies"].([]any)
			types := make([]any, 0, len(topologies))
			for _, topology := range topologies {
				types = append(types, nested(topology, "type"))
			}
			rows = append(rows, map[string]any{
				"name": provider["name"], "engine": provider["displayName"],
				"versions": cell(provider["versions"]), "defaultVersion": providerDefault(provider),
				"topologies": types, "presets": cell(provider["presets"]), "backupTypes": provider["backupTypes"],
			})
		}
		if err := a.print(map[string]any{"items": rows, "nextCursor": nested(v, "nextCursor")}); err != nil {
			return err
		}
		_, err := fmt.Fprintln(a.opts.Out, "Use providers get NAME for topology defaults, limits and upgrade paths. Use -o json or -o yaml for the full document, including uiSchema.")
		return err
	}
	provider, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("unexpected provider response")
	}
	w := tabwriter.NewWriter(a.opts.Out, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "Provider\t%s\nEngine\t%s\nVersions\t%s\nDefault version\t%s\nPresets\t%s\nBackup types\t%s\n",
		cell(provider["name"]), cell(provider["displayName"]), cell(provider["versions"]), providerDefault(provider), cell(provider["presets"]), cell(provider["backupTypes"]))
	fmt.Fprintln(w, "\nTopologies\nTYPE\tNAME\tKEY DEFAULTS")
	topologies, _ := provider["topologies"].([]any)
	for _, topology := range topologies {
		fmt.Fprintf(w, "%s\t%s\t%s\n", cell(nested(topology, "type")), cell(nested(topology, "displayName")), topologyDefaults(topology))
	}
	fmt.Fprintln(w, "\nLimits")
	for _, topology := range topologies {
		fmt.Fprintf(w, "%s:\n", cell(nested(topology, "type")))
		if limits, ok := nested(topology, "limits").(map[string]any); ok {
			detailFields(w, limits, "  ")
		}
	}
	fmt.Fprintln(w, "\nUpgrade paths")
	paths, _ := provider["upgradePaths"].([]any)
	if len(paths) == 0 {
		fmt.Fprintln(w, "None.")
	}
	for _, path := range paths {
		fmt.Fprintf(w, "%s → %s", cell(nested(path, "from")), cell(nested(path, "to")))
		if fcv := nested(path, "requiresFCV"); fcv != nil && fcv != "" {
			fmt.Fprintf(w, " (requires FCV %s)", cell(fcv))
		}
		if nested(path, "requiresBackup") == true {
			fmt.Fprint(w, " (requires backup)")
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintln(w, "\nUse -o json or -o yaml for the full document, including uiSchema, component schemas and resource defaults.")
	return w.Flush()
}
