// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func (a *app) specialFlags(c *cobra.Command, s commandSpec) {
	switch {
	case s.operation == "listPostgresGrants":
		_ = c.MarkFlagRequired("database")
		validateArgs := c.Args
		c.Args = func(cmd *cobra.Command, args []string) error {
			if err := validateArgs(cmd, args); err != nil {
				return err
			}
			database, _ := cmd.Flags().GetString("database")
			if strings.TrimSpace(database) == "" {
				return usageError("--database is required")
			}
			return nil
		}
	case strings.HasPrefix(s.path, "namespaces "):
		a.namespaceFlags(c, s.operation)
	case strings.HasPrefix(s.path, "clusters create"), strings.HasPrefix(s.path, "clusters scale"):
		c.Flags().Int("members", 3, "Engine replicas per replica set")
		c.Flags().Int("replicas", 3, "Engine replicas")
		c.Flags().Int("shards", 1, "Shard count")
		c.Flags().Int("config-servers", 3, "Config-server replicas")
		c.Flags().Int("routers", 2, "Router replicas")
		c.Flags().String("cpu-mode", "SHARED", "SHARED or DEDICATED")
		for _, r := range []struct {
			name  string
			value float64
		}{{"min-cpu", 1}, {"max-cpu", 2}, {"min-ram", 1}, {"max-ram", 2}, {"min-disk", 5}, {"max-disk", 10}, {"start-cpu", 1}} {
			unit := "Resource size in GiB"
			if strings.Contains(r.name, "cpu") {
				unit = "CPU cores"
			}
			c.Flags().Float64(r.name, r.value, unit)
		}
		c.Flags().String("component", "engine", "Scale resources of engine, configServer or router")
		if strings.HasPrefix(s.path, "clusters create") {
			c.Long = "Create a cluster. Convenience flags use PSMDB defaults; --provider defaults to psmdb.\nFor other providers, presets or optional components, submit the complete JSON/YAML body with -f.\nRun karactl providers get <provider> -o json to inspect versions, presets and components,\nthen copy a preset spec into a body containing name and spec. There is no --preset flag."
			c.Flags().String("provider", "psmdb", "Engine provider")
			c.Flags().String("version", "", "Exact engine version (defaults to provider catalog default; see providers get <provider>)")
			c.Flags().String("topology", "replicaSet", "Provider topology (replicaSet, sharded, cluster, dedicatedMasters, single or ha)")
			c.Flags().String("backup-storage", "", "Default backup storage")
			c.Flags().StringArray("schedule", nil, "Schedule: NAME=CRON,type=TYPE,retain=COUNT[,storage=NAME]")
			c.Flags().Bool("pitr", false, "Enable continuous point-in-time recovery")
			c.Flags().Int("oplog-span-minutes", 10, "PITR oplog slice interval")
			c.Flags().String("tls-mode", "requireTLS", "disabled, preferTLS or requireTLS")
			c.Flags().Bool("public", false, "Expose public TCP endpoints")
			c.Flags().String("ip-type", "IPV6", "Public IP type: IPV6 or IPV4")
			c.Flags().StringSlice("allow", nil, "Public firewall CIDR allowlist")
			c.Flags().String("from-backup", "", "Seed from a backup")
			c.Flags().String("from-pitr", "", "Seed from RFC3339 point in time")
			c.Flags().String("source-cluster", "", "Source cluster for initial data")
			c.Flags().String("source-namespace", "", "Namespace of the initial data source (defaults to target)")
			c.Flags().String("engine-config", "", "Provider configuration overrides file")
			c.Flags().String("engine-options", "", "Provider options as a JSON object or @file.json")
			c.Flags().Bool("monitoring", true, "Enable monitoring")
		}
	case strings.HasPrefix(s.path, "clusters upgrade"), s.operation == "raiseClusterFCV":
		c.Flags().String("version", "", "Target version")
	case s.operation == "getClusterConnection":
		c.Flags().Bool("reveal", false, "Explicitly display database credentials")
	case s.operation == "replaceClusterBalancer":
		c.Flags().Bool("enabled", true, "Enable balancing")
	case s.operation == "createBackup":
		c.Flags().String("type", "logical", "Backup type (logical, physical, definitions, snapshot, full, diff or incr)")
		c.Flags().String("storage", "", "Backup storage (defaults to cluster configuration)")
		c.Annotations[operationAnnotation] += ",getCluster"
	case s.operation == "createRestore":
		c.Flags().String("backup", "", "Backup name")
		c.Flags().String("time", "", "RFC3339 restore time")
		c.Flags().String("source-cluster", "", "Source cluster (defaults to target)")
		c.Flags().String("source-namespace", "", "Source namespace (defaults to target namespace)")
		c.Flags().String("into-new", "", "Clone source spec and restore into a new cluster")
	case strings.HasPrefix(s.path, "operations wait"), s.path == "clusters wait <cluster>":
		c.Flags().Duration("timeout", 30*time.Minute, "Maximum wait duration")
	case s.operation == "checkPermission":
		c.Flags().String("resource", "", "RBAC resource")
		c.Flags().String("action", "read", "RBAC action")
		c.Flags().String("object", "*", "RBAC object")
		c.Flags().String("subject", "", "Optional subject")
	}
}

func (a *app) body(cmd *cobra.Command, s commandSpec, args []string) (any, error) {
	if strings.HasPrefix(s.path, "namespaces ") {
		return a.namespaceBody(cmd, s.operation)
	}
	if !s.body {
		return nil, nil
	}
	file, _ := cmd.Flags().GetString("file")
	if file != "" {
		if s.operation == "replaceRBACPolicy" {
			var b []byte
			var err error
			if file == "-" {
				b, err = readInput(a.opts.In)
			} else {
				b, err = os.ReadFile(file)
			}
			if err != nil {
				return nil, err
			}
			return map[string]any{"policy": string(b)}, nil
		}
		return a.fileBody(cmd)
	}
	switch {
	case strings.HasPrefix(s.path, "clusters create"):
		if len(args) != 1 {
			return nil, usageError("cluster name is required without -f")
		}
		return a.clusterCreate(cmd, args[0])
	case strings.HasPrefix(s.path, "clusters scale"):
		spec := map[string]any{}
		components := map[string]any{}
		component, _ := cmd.Flags().GetString("component")
		if component != "engine" && component != "configServer" && component != "router" {
			return nil, usageError("invalid --component")
		}
		c := map[string]any{}
		if cmd.Flags().Changed("members") || cmd.Flags().Changed("replicas") {
			n, _ := cmd.Flags().GetInt("members")
			if cmd.Flags().Changed("replicas") {
				n, _ = cmd.Flags().GetInt("replicas")
			}
			c["replicas"] = n
		}
		r := resourceFlags(cmd, false)
		if len(r) > 0 {
			c["resources"] = r
		}
		if len(c) > 0 {
			components[component] = c
		}
		for flag, component := range map[string]string{"config-servers": "configServer", "routers": "router"} {
			if cmd.Flags().Changed(flag) {
				n, _ := cmd.Flags().GetInt(flag)
				existing, _ := components[component].(map[string]any)
				if existing == nil {
					existing = map[string]any{}
					components[component] = existing
				}
				existing["replicas"] = n
			}
		}
		if len(components) > 0 {
			spec["components"] = components
		}
		if cmd.Flags().Changed("shards") {
			n, _ := cmd.Flags().GetInt("shards")
			if n < 1 || n > 128 {
				return nil, usageError("--shards must be from 1 to 128")
			}
			spec["topology"] = map[string]any{"shards": n}
		}
		if len(spec) == 0 {
			return nil, usageError("set a replica, shard or resource flag to scale")
		}
		return map[string]any{"spec": spec}, nil
	case strings.HasPrefix(s.path, "clusters upgrade"):
		version, _ := cmd.Flags().GetString("version")
		if version == "" {
			return nil, usageError("--version is required")
		}
		return map[string]any{"spec": map[string]any{"version": version}}, nil
	case s.operation == "raiseClusterFCV":
		version, _ := cmd.Flags().GetString("version")
		if version == "" {
			return nil, usageError("--version is required")
		}
		return map[string]any{"version": version}, nil
	case s.operation == "replaceCluster":
		return a.editSpec(cmd, args[0])
	case s.operation == "replaceClusterBalancer":
		enabled, _ := cmd.Flags().GetBool("enabled")
		return map[string]any{"enabled": enabled}, nil
	case s.operation == "createBackup":
		name := "backup-" + time.Now().UTC().Format("20060102-150405")
		if len(args) > 1 {
			name = args[1]
		}
		typ, _ := cmd.Flags().GetString("type")
		if typ != "logical" && typ != "physical" && typ != "definitions" && typ != "snapshot" && typ != "full" && typ != "diff" && typ != "incr" {
			return nil, usageError("backup type must be logical, physical, definitions, snapshot, full, diff or incr")
		}
		storage, _ := cmd.Flags().GetString("storage")
		if storage == "" {
			cluster, err := a.request(cmd.Context(), "getCluster", args[:1], nil, nil)
			if err != nil {
				return nil, err
			}
			storage, _ = nested(cluster, "spec", "backup", "storage").(string)
		}
		if storage == "" {
			return nil, usageError("--storage or cluster backup.storage is required")
		}
		return map[string]any{"name": name, "type": typ, "storage": storage}, nil
	case s.operation == "createRestore":
		backup, _ := cmd.Flags().GetString("backup")
		point, _ := cmd.Flags().GetString("time")
		sourceCluster, _ := cmd.Flags().GetString("source-cluster")
		if sourceCluster == "" {
			sourceCluster = args[0]
		}
		source, err := restoreSource(sourceCluster, backup, point)
		if err != nil {
			return nil, err
		}
		name := "restore-" + time.Now().UTC().Format("20060102-150405")
		if len(args) > 1 {
			name = args[1]
		}
		body := map[string]any{"name": name, "source": source}
		if ns, _ := cmd.Flags().GetString("source-namespace"); ns != "" {
			body["sourceNamespace"] = ns
		}
		return body, nil
	case s.operation == "checkPermission":
		resource, _ := cmd.Flags().GetString("resource")
		if resource == "" {
			return nil, usageError("--resource is required")
		}
		action, _ := cmd.Flags().GetString("action")
		object, _ := cmd.Flags().GetString("object")
		subject, _ := cmd.Flags().GetString("subject")
		body := map[string]any{"resource": resource, "action": action, "object": object}
		if subject != "" {
			body["subject"] = subject
		}
		return body, nil
	default:
		return nil, usageError("--file is required")
	}
}

func resourceFlags(cmd *cobra.Command, defaults bool) map[string]any {
	r := map[string]any{}
	if defaults || cmd.Flags().Changed("cpu-mode") {
		v, _ := cmd.Flags().GetString("cpu-mode")
		r["cpuMode"] = v
	}
	for flag, key := range map[string]string{"min-cpu": "minCpu", "max-cpu": "maxCpu", "min-ram": "minRam", "max-ram": "maxRam", "min-disk": "minDisk", "max-disk": "maxDisk", "start-cpu": "startCpu"} {
		if (defaults && flag != "start-cpu") || cmd.Flags().Changed(flag) {
			v, _ := cmd.Flags().GetFloat64(flag)
			r[key] = v
		}
	}
	return r
}
func (a *app) clusterCreate(cmd *cobra.Command, name string) (any, error) {
	version, _ := cmd.Flags().GetString("version")
	provider, _ := cmd.Flags().GetString("provider")
	topology, _ := cmd.Flags().GetString("topology")
	if topology != "replicaSet" && topology != "sharded" && topology != "cluster" && topology != "dedicatedMasters" && topology != "single" && topology != "ha" {
		return nil, usageError("invalid provider topology %q", topology)
	}
	mode, _ := cmd.Flags().GetString("cpu-mode")
	if mode != "SHARED" && mode != "DEDICATED" {
		return nil, usageError("CPU mode must be SHARED or DEDICATED")
	}
	members, _ := cmd.Flags().GetInt("members")
	if cmd.Flags().Changed("replicas") {
		members, _ = cmd.Flags().GetInt("replicas")
	}
	if members < 1 {
		return nil, usageError("members must be positive")
	}
	engine := map[string]any{}
	t := map[string]any{}
	if provider == "psmdb" || cmd.Flags().Changed("members") || cmd.Flags().Changed("replicas") {
		engine["replicas"] = members
	}
	if resources := resourceFlags(cmd, provider == "psmdb"); len(resources) > 0 {
		engine["resources"] = resources
	}
	components := map[string]any{"engine": engine}
	if provider == "psmdb" || cmd.Flags().Changed("topology") {
		t["type"] = topology
	}
	if topology != "sharded" {
		for _, flag := range []string{"shards", "config-servers", "routers"} {
			if cmd.Flags().Changed(flag) {
				return nil, usageError("--%s requires --topology sharded", flag)
			}
		}
	}
	if topology == "sharded" && (provider == "psmdb" || cmd.Flags().Changed("topology")) {
		shards, _ := cmd.Flags().GetInt("shards")
		if shards < 1 || shards > 128 {
			return nil, usageError("--shards must be from 1 to 128")
		}
		t["shards"] = shards
		for flag, key := range map[string]string{"config-servers": "configServer", "routers": "router"} {
			replicas, _ := cmd.Flags().GetInt(flag)
			components[key] = map[string]any{"replicas": replicas, "resources": resourceFlags(cmd, true)}
		}
	}
	tlsMode, _ := cmd.Flags().GetString("tls-mode")
	if tlsMode != "requireTLS" && tlsMode != "preferTLS" && tlsMode != "disabled" {
		return nil, usageError("invalid TLS mode")
	}
	public, _ := cmd.Flags().GetBool("public")
	allow, _ := cmd.Flags().GetStringSlice("allow")
	ipType, _ := cmd.Flags().GetString("ip-type")
	monitoring, _ := cmd.Flags().GetBool("monitoring")
	spec := map[string]any{"provider": provider, "topology": t, "components": components, "tls": map[string]any{"mode": tlsMode, "ca": "selfSigned"}, "exposure": map[string]any{"public": public, "allowlist": allow, "ipType": ipType}, "monitoring": map[string]any{"enabled": monitoring}}
	if version != "" {
		spec["version"] = version
	}
	storage, _ := cmd.Flags().GetString("backup-storage")
	pitr, _ := cmd.Flags().GetBool("pitr")
	schedules, _ := cmd.Flags().GetStringArray("schedule")
	span, _ := cmd.Flags().GetInt("oplog-span-minutes")
	if storage != "" || pitr || len(schedules) > 0 {
		b := map[string]any{"pitr": map[string]any{"enabled": pitr, "oplogSpanMinutes": span}}
		if storage != "" {
			b["storage"] = storage
		}
		parsed := []any{}
		for _, schedule := range schedules {
			v, err := parseSchedule(schedule, storage)
			if err != nil {
				return nil, err
			}
			parsed = append(parsed, v)
		}
		b["schedules"] = parsed
		spec["backup"] = b
	}
	backup, _ := cmd.Flags().GetString("from-backup")
	point, _ := cmd.Flags().GetString("from-pitr")
	if backup != "" || point != "" {
		sourceCluster, _ := cmd.Flags().GetString("source-cluster")
		if sourceCluster == "" {
			return nil, usageError("--source-cluster is required")
		}
		source, err := restoreSource(sourceCluster, backup, point)
		if err != nil {
			return nil, err
		}
		source["cluster"] = source["sourceCluster"]
		delete(source, "sourceCluster")
		if ns, _ := cmd.Flags().GetString("source-namespace"); ns != "" {
			source["namespace"] = ns
		}
		spec["dataSource"] = source
	}
	engineConfig, _ := cmd.Flags().GetString("engine-config")
	if engineConfig != "" {
		b, err := os.ReadFile(engineConfig)
		if err != nil {
			return nil, err
		}
		spec["engineConfig"] = string(b)
	}
	options, _ := cmd.Flags().GetString("engine-options")
	if options != "" {
		data := []byte(options)
		if strings.HasPrefix(options, "@") {
			var err error
			data, err = os.ReadFile(strings.TrimPrefix(options, "@"))
			if err != nil {
				return nil, err
			}
		}
		var value map[string]any
		if err := json.Unmarshal(data, &value); err != nil || value == nil {
			return nil, fmt.Errorf("--engine-options must be a JSON object")
		}
		spec["engineOptions"] = value
	}
	return map[string]any{"name": name, "spec": spec}, nil
}
func parseSchedule(input, storage string) (map[string]any, error) {
	parts := strings.Split(input, ",")
	name, expression, ok := strings.Cut(parts[0], "=")
	if !ok || name == "" {
		return nil, usageError("schedule must begin NAME=CRON")
	}
	if _, err := cron.ParseStandard(expression); err != nil {
		return nil, usageError("schedule %s: %s", name, err)
	}
	result := map[string]any{"name": name, "cron": expression, "type": "logical", "retainCount": 7, "enabled": true, "storage": storage}
	for _, part := range parts[1:] {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return nil, usageError("invalid schedule option %q", part)
		}
		switch key {
		case "type":
			if value != "logical" && value != "physical" && value != "definitions" && value != "snapshot" && value != "full" && value != "diff" && value != "incr" {
				return nil, usageError("invalid schedule type %q", value)
			}
			result[key] = value
		case "storage":
			result[key] = value
		case "retain":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 {
				return nil, usageError("schedule retention must be positive")
			}
			result["retainCount"] = n
		default:
			return nil, usageError("unknown schedule option %q", key)
		}
	}
	if result["storage"] == "" {
		return nil, usageError("schedule requires a backup storage")
	}
	return result, nil
}
func restoreSource(cluster, backup, point string) (map[string]any, error) {
	if (backup == "") == (point == "") {
		return nil, usageError("exactly one backup or point-in-time source is required")
	}
	s := map[string]any{"sourceCluster": cluster}
	if backup != "" {
		s["backup"] = backup
	} else {
		t, err := time.Parse(time.RFC3339, point)
		if err != nil {
			return nil, usageError("restore time: %s", err)
		}
		s["pitrTime"] = t.UTC().Format(time.RFC3339Nano)
	}
	return s, nil
}
func nested(v any, keys ...string) any {
	for _, key := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[key]
	}
	return v
}
func (a *app) editSpec(cmd *cobra.Command, cluster string) (any, error) {
	current, err := a.request(cmd.Context(), "getCluster", []string{cluster}, nil, nil)
	if err != nil {
		return nil, err
	}
	spec := nested(current, "spec")
	if spec == nil {
		return nil, fmt.Errorf("cluster response lacks spec")
	}
	b, err := yaml.Marshal(spec)
	if err != nil {
		return nil, err
	}
	f, err := os.CreateTemp("", "karactl-spec-*.yaml")
	if err != nil {
		return nil, err
	}
	path := f.Name()
	defer os.Remove(path)
	if _, err = f.Write(b); err != nil {
		f.Close()
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = "vi"
	}
	process := exec.CommandContext(cmd.Context(), "sh", "-c", editor+" \"$1\"", "karactl-editor", path)
	process.Stdin = a.opts.In
	process.Stdout = a.opts.Out
	process.Stderr = a.opts.Err
	if err = process.Run(); err != nil {
		return nil, fmt.Errorf("editor: %w", err)
	}
	b, err = os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var edited map[string]any
	if err = yaml.Unmarshal(b, &edited); err != nil {
		return nil, err
	}
	if edited == nil {
		return nil, fmt.Errorf("edited spec must be an object")
	}
	return map[string]any{"spec": edited}, nil
}
func (a *app) clone(cmd *cobra.Command, cluster, name string, body any) error {
	source, ok := nested(body, "source").(map[string]any)
	if !ok {
		return fmt.Errorf("restore request lacks source")
	}
	if sourceCluster, ok := source["sourceCluster"].(string); ok && sourceCluster != "" {
		cluster = sourceCluster
	}
	sourceNamespace, _ := nested(body, "sourceNamespace").(string)
	if sourceNamespace == "" {
		sourceNamespace = a.namespace
	}
	current, err := a.request(cmd.Context(), "getCluster", []string{cluster}, map[string]any{"namespace": sourceNamespace}, nil)
	if err != nil {
		return err
	}
	spec, ok := nested(current, "spec").(map[string]any)
	if !ok {
		return fmt.Errorf("cluster response lacks spec")
	}
	if sourceNamespace != a.namespace {
		delete(spec, "backup")
	}
	dataSource := map[string]any{"cluster": source["sourceCluster"], "namespace": sourceNamespace}
	if dataSource["cluster"] == nil || dataSource["cluster"] == "" {
		dataSource["cluster"] = cluster
	}
	for _, key := range []string{"backup", "pitrTime"} {
		if v, ok := source[key]; ok {
			dataSource[key] = v
		}
	}
	spec["dataSource"] = dataSource
	result, err := a.request(cmd.Context(), "createCluster", nil, nil, map[string]any{"name": name, "spec": spec})
	if err != nil {
		return err
	}
	return a.print(result)
}
