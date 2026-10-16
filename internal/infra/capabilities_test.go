// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra_test

import (
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestCapabilityNamesAndList(t *testing.T) {
	want := []string{"publicTcp", "tcpAllowlist", "publicHttps", "diskGrowth", "volumeQuota", "placement", "exec", "logs", "metrics", "scopedTokens", "dedicatedCpu", "horizontalScaling", "envChangeRestarts"}
	if got := infra.CapabilityNames(); !reflect.DeepEqual(got, want) {
		t.Fatalf("names: %v", got)
	}
	all := infra.Capabilities{PublicTCP: true, TCPAllowlist: true, PublicHTTPS: true, DiskGrowth: true, VolumeQuota: true, Placement: true, Exec: true, Logs: true, Metrics: true, ScopedTokens: true, DedicatedCPU: true, HorizontalScaling: true, EnvChangeRestarts: true}
	if got := all.List(); !reflect.DeepEqual(got, want) {
		t.Fatalf("all flags: %v", got)
	}
	if got := (infra.Capabilities{}).List(); got == nil || len(got) != 0 {
		t.Fatalf("empty flags: %v", got)
	}
	subset := infra.Capabilities{PublicTCP: true, Logs: true, EnvChangeRestarts: true}
	if got := subset.List(); !reflect.DeepEqual(got, []string{"publicTcp", "logs", "envChangeRestarts"}) {
		t.Fatalf("subset: %v", got)
	}
	names := infra.CapabilityNames()
	names[0] = "changed"
	if infra.CapabilityNames()[0] != "publicTcp" {
		t.Fatal("catalog aliases result")
	}
	listed := all.List()
	listed[0] = "changed"
	if all.List()[0] != "publicTcp" {
		t.Fatal("list aliases result")
	}
}
