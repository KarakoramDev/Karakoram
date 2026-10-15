// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"math"
	"strings"
	"testing"
)

func TestHeapSizing(t *testing.T) {
	for _, tt := range []struct {
		name string
		fn   func(float64) int
		ram  float64
		want int
	}{
		{"es minimum", ElasticsearchHeapMB, 0, 512}, {"es master", ElasticsearchHeapMB, 1, 512}, {"es 1.5", ElasticsearchHeapMB, 1.5, 576}, {"es 2", ElasticsearchHeapMB, 2, 768}, {"es 4", ElasticsearchHeapMB, 4, 1600}, {"es over 4", ElasticsearchHeapMB, 4.01, 2048}, {"es 8", ElasticsearchHeapMB, 8, 4096}, {"es cap", ElasticsearchHeapMB, 128, 31744}, {"es infinity", ElasticsearchHeapMB, math.Inf(1), 31744}, {"es nan", ElasticsearchHeapMB, math.NaN(), 512},
		{"kibana minimum", KibanaHeapMB, 0.5, 512}, {"kibana 1.5", KibanaHeapMB, 1.5, 921}, {"kibana 2", KibanaHeapMB, 2, 1228},
		{"logstash minimum", LogstashHeapMB, 0.5, 384}, {"logstash 1", LogstashHeapMB, 1, 512}, {"logstash 1.5", LogstashHeapMB, 1.5, 768}, {"logstash 2", LogstashHeapMB, 2, 1024},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fn(tt.ram); got != tt.want {
				t.Fatalf("got %d want %d", got, tt.want)
			}
		})
	}
}

func TestComponentResources(t *testing.T) {
	for _, tt := range []struct {
		component                                 string
		minRAM, maxRAM, minDisk, maxDisk, minimum float64
	}{
		{"engine", 2, 2, 5, 20, 1.5}, {"master", 1.5, 1.5, 5, 20, 1}, {"kibana", 1.5, 2, 1, 5, 1}, {"logstash", 1.5, 1.5, 1, 5, 1}, {"apm", 0.5, 0.5, 1, 5, 0.25},
	} {
		t.Run(tt.component, func(t *testing.T) {
			r, err := DefaultResources(tt.component)
			if err != nil {
				t.Fatal(err)
			}
			if r.MinRAM != tt.minRAM || r.MaxRAM != tt.maxRAM || r.MinDisk != tt.minDisk || r.MaxDisk != tt.maxDisk || r.CPUMode != "SHARED" || r.MinCPU != 1 || r.MaxCPU != 2 {
				t.Fatalf("resources: %+v", r)
			}
			for _, ram := range []float64{tt.minimum, tt.minRAM} {
				if err := ValidateMinimumRAM(tt.component, ram); err != nil {
					t.Fatal(err)
				}
			}
			for _, ram := range []float64{tt.minimum - 0.01, math.NaN(), math.Inf(1)} {
				if err := ValidateMinimumRAM(tt.component, ram); err == nil || !strings.Contains(err.Error(), tt.component) {
					t.Fatal("missing descriptive minimum RAM error")
				}
			}
		})
	}
	if _, err := DefaultResources("unknown"); err == nil {
		t.Fatal("unknown component")
	}
	if err := ValidateMinimumRAM("unknown", 2); err == nil {
		t.Fatal("unknown component")
	}
}
