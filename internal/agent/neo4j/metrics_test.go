// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"encoding/json"
	"testing"
)

func TestHeapUsageQueryAPICompositeData(t *testing.T) {
	// JMX CompositeData uses the Query API's properties wrapper, not a plain map.
	const response = `{"data":{"fields":["heap"],"values":[[{"description":"java.lang.management.MemoryUsage","properties":{"init":268435456,"committed":268435456,"used":197324600,"max":268435456}}]]}}`
	var result queryResult
	if err := json.Unmarshal([]byte(response), &result); err != nil {
		t.Fatal(err)
	}
	got, err := heapUsage(result)
	want := [3]float64{197324600, 268435456, 268435456}
	if err != nil || got != want {
		t.Fatalf("heap mapping got %v, %v; want %v", got, err, want)
	}
	for _, invalid := range []string{`{}`, `{"data":{"fields":["heap"],"values":[[null]]}}`, `{"data":{"fields":["heap"],"values":[[{"properties":{"used":1}}]]}}`, `{"data":{"fields":["heap"],"values":[[{"used":1,"committed":2,"max":3}]]}}`} {
		var malformed queryResult
		if err := json.Unmarshal([]byte(invalid), &malformed); err != nil {
			t.Fatal(err)
		}
		if _, err := heapUsage(malformed); err == nil {
			t.Fatal("accepted incomplete heap metric response")
		}
	}
}
