// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestMalformedClusterPreservesHealthyRowsAndNamesRecovery(t *testing.T) {
	for _, field := range []string{"spec", "status", "delete_options", "internal"} {
		t.Run(field, func(t *testing.T) {
			st, _ := testStore(t)
			for _, name := range []string{"broken", "healthy"} {
				if err := st.CreateCluster(t.Context(), &domain.Cluster{Name: name}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := st.writer.Exec("UPDATE clusters SET "+field+"=? WHERE name='broken'", []byte("{")); err != nil {
				t.Fatal(err)
			}
			clusters, err := st.ListClusters(t.Context(), "")
			if err == nil || !strings.Contains(err.Error(), "default/broken") || !strings.Contains(err.Error(), field) || !strings.Contains(err.Error(), "restore") {
				t.Fatalf("nonactionable corruption error: %v", err)
			}
			if len(clusters) != 1 || clusters[0].Name != "healthy" {
				t.Fatalf("healthy rows not isolated: %d", len(clusters))
			}
			if _, err := st.GetCluster(t.Context(), domain.ClusterRef{Name: "broken"}); err == nil {
				t.Fatal("corrupt row was trusted")
			}
			var stored []byte
			if err := st.reader.QueryRow("SELECT " + field + " FROM clusters WHERE name='broken'").Scan(&stored); err != nil || string(stored) != "{" {
				t.Fatal("damaged source altered", err)
			}
		})
	}
}
