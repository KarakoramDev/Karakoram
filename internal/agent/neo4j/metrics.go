// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

func (e *Engine) Metrics(ctx context.Context, n *agent.Node) []agent.Metric {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metric := func(name, help string, v float64) agent.Metric {
		return agent.Metric{Name: "neo4j_" + name, Help: help, Value: v}
	}
	failed := []agent.Metric{metric("up", "Authenticated Neo4j query availability", 0)}
	s, err := decodeSpec(n.Config())
	if err != nil {
		return failed
	}
	states, maintenance := n.Snapshot()
	if maintenance || states[neo4jspec.Process].State != "running" {
		return failed
	}
	_, port := e.ports(false)
	run := func(db, statement string) (queryResult, error) {
		return query(ctx, s, port, s.AdminPassword, db, statement, nil)
	}
	memory, err := run("neo4j", `CALL dbms.queryJmx('java.lang:type=Memory') YIELD attributes RETURN attributes.HeapMemoryUsage.value AS heap`)
	if err != nil {
		return failed
	}
	heap, err := heapUsage(memory)
	if err != nil {
		return failed
	}
	result := []agent.Metric{metric("up", "Authenticated Neo4j query availability", 1), metric("heap_configured_bytes", "Configured JVM heap bytes", float64(s.HeapMB)*1024*1024), metric("pagecache_configured_bytes", "Configured page cache bytes", float64(s.PageCacheMB)*1024*1024)}
	for i, key := range []string{"used", "committed", "max"} {
		result = append(result, metric("heap_"+key+"_bytes", "JVM heap "+key+" bytes", heap[i]))
	}
	for _, q := range []struct{ name, db, statement, help string }{
		{"nodes", "neo4j", "MATCH (n) RETURN count(n) AS value", "Total node count"},
		{"relationships", "neo4j", "MATCH ()-[r]->() RETURN count(r) AS value", "Total relationship count"},
		{"transactions_active", "system", "SHOW TRANSACTIONS YIELD transactionId RETURN count(transactionId) AS value", "Active transactions including the scrape transaction"},
	} {
		data, err := run(q.db, q.statement)
		if err != nil {
			return failed
		}
		value, err := scalar(data)
		if err != nil {
			return failed
		}
		result = append(result, metric(q.name, q.help, value))
	}
	size := storeBytes(ctx, filepath.Join(dataDir(n), "databases"))
	if ctx.Err() != nil {
		return failed
	}
	return append(result, metric("store_size_bytes", "Database store bytes on disk", float64(size)))
}
func scalar(r queryResult) (float64, error) {
	rows := r.rows()
	if len(rows) == 1 {
		if v, ok := rows[0]["value"].(float64); ok {
			return v, nil
		}
	}
	return 0, errors.New("invalid Neo4j metric response")
}

// Query API serializes JMX CompositeData as a description plus a properties
// object, even after projecting HeapMemoryUsage.value in Cypher.
func heapUsage(result queryResult) ([3]float64, error) {
	var values [3]float64
	rows := result.rows()
	if len(rows) != 1 {
		return values, errors.New("invalid Neo4j heap metric response")
	}
	heap, ok := rows[0]["heap"].(map[string]any)
	if !ok {
		return values, errors.New("invalid Neo4j heap metric response")
	}
	properties, ok := heap["properties"].(map[string]any)
	if !ok {
		return values, errors.New("invalid Neo4j heap metric response")
	}
	for i, key := range []string{"used", "committed", "max"} {
		value, ok := properties[key].(float64)
		if !ok {
			return values, errors.New("invalid Neo4j heap metric response")
		}
		values[i] = value
	}
	return values, nil
}
