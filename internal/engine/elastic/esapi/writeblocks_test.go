// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package esapi

import (
	"io"
	"net/http"
	"reflect"
	"testing"
)

func TestIndexWriteBlocks(t *testing.T) {
	for _, tc := range []struct {
		name, response string
		want           []string
		fail           bool
	}{
		{"mixed", `{"logs":{"settings":{"index.blocks.read_only_allow_delete":"true","index.blocks.read_only":false}},".system":{"settings":{"index.blocks.write":true}},"open":{"settings":{}}}`, []string{".system (index.blocks.write)", "logs (index.blocks.read_only_allow_delete)"}, false},
		{"released", `{"logs":{"settings":{"index.blocks.read_only_allow_delete":"false"}}}`, nil, false},
		{"malformed", `{"logs":{"settings":{"index.blocks.write":"invalid"}}}`, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/_all/_settings" || r.URL.Query().Get("expand_wildcards") != "all" || r.URL.Query().Get("flat_settings") != "true" {
					t.Errorf("unsafe observation: %s %s", r.Method, r.URL)
				}
				_, _ = io.WriteString(w, tc.response)
			})
			got, err := c.IndexWriteBlocks(t.Context())
			if (err != nil) != tc.fail || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("blocks=%v err=%v", got, err)
			}
		})
	}
}
