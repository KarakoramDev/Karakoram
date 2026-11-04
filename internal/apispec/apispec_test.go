// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package apispec

import (
	"slices"
	"testing"
)

func TestEmbeddedSpecLoadsAndValidates(t *testing.T) {
	doc, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if doc.Paths.Find("/clusters") == nil {
		t.Fatal("embedded spec has no /clusters path")
	}
}

func TestRequestIDRemainsATransportHeader(t *testing.T) {
	doc, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if doc.Components.Headers["RequestID"] == nil {
		t.Fatal("request correlation response header is undocumented")
	}
	for path, item := range doc.Paths.Map() {
		for _, parameter := range item.Parameters {
			if parameter.Value.Name == "X-Request-ID" {
				t.Errorf("%s declares transport RequestID as a path-wide parameter", path)
			}
		}
		for method, operation := range item.Operations() {
			for _, parameter := range operation.Parameters {
				if parameter.Value.Name == "X-Request-ID" {
					t.Errorf("%s %s declares transport RequestID as an operation parameter", method, path)
				}
			}
		}
	}
}

func TestMySQLAndPXCTopologyAndBackupEnums(t *testing.T) {
	doc, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Topology", "TopologyInput", "TopologyPatch", "ProviderTopology"} {
		values := doc.Components.Schemas[name].Value.Properties["type"].Value.Enum
		for _, topology := range []string{"single", "group", "cluster"} {
			if !slices.Contains(values, any(topology)) {
				t.Errorf("%s.type excludes %s", name, topology)
			}
		}
	}
	for _, name := range []string{"Backup", "BackupSchedule", "BackupScheduleInput"} {
		values := doc.Components.Schemas[name].Value.Properties["type"].Value.Enum
		if !slices.Contains(values, any("incremental")) {
			t.Errorf("%s.type excludes incremental", name)
		}
	}
	values := doc.Components.Schemas["Provider"].Value.Properties["backupTypes"].Value.Items.Value.Enum
	if !slices.Contains(values, any("incremental")) {
		t.Error("Provider.backupTypes excludes incremental")
	}
}
