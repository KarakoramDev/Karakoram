// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

// Metric is one Prometheus sample emitted by a product's local monitoring API.
type Metric struct {
	Name, Help string
	Labels     map[string]string
	Value      float64
	Counter    bool
}

// Metrics scrapes only local product APIs, with a five-second overall deadline.
// Any failed request or invalid response yields only the component's up=0.
func (r *Runner) Metrics(ctx context.Context, _ Host, spec agentspec.Spec) []Metric {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	prefix := metricPrefix(spec.Component)
	down := []Metric{metric(prefix, "up", "Whether the local product APIs are reachable.", 0, false)}
	transport := &http.Transport{TLSHandshakeTimeout: 3 * time.Second}
	defer transport.CloseIdleConnections()
	if spec.Component == agentspec.ComponentElasticsearch {
		if spec.Elasticsearch == nil {
			return down
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(spec.CAPEM)) {
			return down
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, ServerName: spec.Elasticsearch.NodeName, MinVersion: tls.VersionTLS12}
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	result, err := scrape(ctx, client, spec)
	if err != nil {
		return down
	}
	return append([]Metric{metric(prefix, "up", "Whether the local product APIs are reachable.", 1, false)}, result...)
}

func metricPrefix(component string) string {
	if component == agentspec.ComponentAPM {
		return "karakoram_apm_"
	}
	return "karakoram_" + component + "_"
}

func metric(prefix, name, help string, value float64, counter bool) Metric {
	return Metric{Name: prefix + name, Help: help, Value: value, Counter: counter}
}

func getJSON(ctx context.Context, client *http.Client, url string, auth agentspec.Credentials, allowUnavailable bool, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if auth.Username != "" {
		req.SetBasicAuth(auth.Username, auth.Password)
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("local metrics request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && !(allowUnavailable && resp.StatusCode == http.StatusServiceUnavailable) {
		return errors.New("local metrics API unavailable")
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 16<<20))
	if err := dec.Decode(dst); err != nil {
		return errors.New("invalid local metrics JSON")
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("invalid local metrics JSON suffix")
	}
	return nil
}

func scrape(ctx context.Context, client *http.Client, spec agentspec.Spec) ([]Metric, error) {
	prefix := metricPrefix(spec.Component)
	switch spec.Component {
	case agentspec.ComponentElasticsearch:
		if spec.Elasticsearch == nil {
			return nil, errors.New("missing Elasticsearch spec")
		}
		var stats esStats
		if err := getJSON(ctx, client, "https://127.0.0.1:9200/_nodes/_local/stats/jvm,indices,fs,thread_pool,process", spec.Elasticsearch.Monitor, false, &stats); err != nil {
			return nil, err
		}
		var health esHealth
		if err := getJSON(ctx, client, "https://127.0.0.1:9200/_cluster/health?local=true", spec.Elasticsearch.Monitor, false, &health); err != nil {
			return nil, err
		}
		return elasticMetrics(stats, health)
	case agentspec.ComponentKibana:
		var status struct {
			Status struct {
				Overall struct {
					Level string `json:"level"`
				} `json:"overall"`
			} `json:"status"`
		}
		if err := getJSON(ctx, client, "http://127.0.0.1:5601/api/status", agentspec.Credentials{}, true, &status); err != nil {
			return nil, err
		}
		level, ok := map[string]float64{"available": 0, "degraded": 1, "unavailable": 2, "critical": 2}[status.Status.Overall.Level]
		if !ok {
			return nil, errors.New("invalid Kibana overall status")
		}
		return []Metric{metric(prefix, "status_level", "Kibana status: available=0, degraded=1, unavailable=2.", level, false)}, nil
	case agentspec.ComponentLogstash:
		var stats struct {
			Events *struct{ In, Out float64 } `json:"events"`
			JVM    struct {
				Mem struct {
					HeapUsed float64 `json:"heap_used_in_bytes"`
				} `json:"mem"`
			} `json:"jvm"`
		}
		if err := getJSON(ctx, client, "http://127.0.0.1:9600/_node/stats", agentspec.Credentials{}, false, &stats); err != nil {
			return nil, err
		}
		if stats.Events == nil {
			return nil, errors.New("missing Logstash events")
		}
		return []Metric{
			metric(prefix, "events_in_total", "Events received by Logstash.", stats.Events.In, true),
			metric(prefix, "events_out_total", "Events emitted by Logstash.", stats.Events.Out, true),
			metric(prefix, "jvm_heap_used_bytes", "Used JVM heap bytes.", stats.JVM.Mem.HeapUsed, false),
		}, nil
	case agentspec.ComponentAPM:
		var stats struct {
			APM *struct {
				Server struct {
					Request struct {
						Count float64 `json:"count"`
					} `json:"request"`
				} `json:"server"`
				Processor struct {
					Stream struct {
						Accepted float64 `json:"accepted"`
					} `json:"stream"`
				} `json:"processor"`
			} `json:"apm-server"`
		}
		if err := getJSON(ctx, client, "http://127.0.0.1:5066/stats", agentspec.Credentials{}, false, &stats); err != nil {
			return nil, err
		}
		if stats.APM == nil {
			return nil, errors.New("missing APM Server stats")
		}
		// The Beats stats bridge exports apm-server.server.request.count and
		// apm-server.processor.stream.accepted. The latter counts intake events,
		// not successful HTTP responses or Elasticsearch output acknowledgements.
		return []Metric{
			metric(prefix, "requests_total", "APM intake HTTP requests received.", stats.APM.Server.Request.Count, true),
			metric(prefix, "events_accepted_total", "APM intake stream events accepted.", stats.APM.Processor.Stream.Accepted, true),
		}, nil
	default:
		return nil, errors.New("unknown Elastic component")
	}
}

type esHealth struct {
	Status     string  `json:"status"`
	Nodes      float64 `json:"number_of_nodes"`
	Active     float64 `json:"active_shards"`
	Unassigned float64 `json:"unassigned_shards"`
	Relocating float64 `json:"relocating_shards"`
}

type esStats struct {
	Nodes map[string]struct {
		JVM struct {
			Mem struct {
				Used float64 `json:"heap_used_in_bytes"`
				Max  float64 `json:"heap_max_in_bytes"`
			} `json:"mem"`
			GC struct {
				Collectors map[string]struct {
					Time float64 `json:"collection_time_in_millis"`
				} `json:"collectors"`
			} `json:"gc"`
		} `json:"jvm"`
		Indices struct {
			Docs struct {
				Count float64 `json:"count"`
			} `json:"docs"`
			Store struct {
				Size float64 `json:"size_in_bytes"`
			} `json:"store"`
			Indexing struct {
				Total float64 `json:"index_total"`
			} `json:"indexing"`
			Search struct {
				Total float64 `json:"query_total"`
			} `json:"search"`
		} `json:"indices"`
		FS struct {
			Total struct {
				Available float64 `json:"available_in_bytes"`
			} `json:"total"`
		} `json:"fs"`
		ThreadPools map[string]struct {
			Rejected float64 `json:"rejected"`
		} `json:"thread_pool"`
	} `json:"nodes"`
}

func elasticMetrics(stats esStats, health esHealth) ([]Metric, error) {
	status, ok := map[string]float64{"green": 0, "yellow": 1, "red": 2}[health.Status]
	if !ok || len(stats.Nodes) != 1 {
		return nil, errors.New("invalid local Elasticsearch stats")
	}
	const prefix = "karakoram_elasticsearch_"
	result := []Metric{
		metric(prefix, "cluster_status", "Cluster status: green=0, yellow=1, red=2.", status, false),
		metric(prefix, "cluster_nodes", "Cluster node count.", health.Nodes, false),
		metric(prefix, "active_shards", "Active shard count.", health.Active, false),
		metric(prefix, "unassigned_shards", "Unassigned shard count.", health.Unassigned, false),
		metric(prefix, "relocating_shards", "Relocating shard count.", health.Relocating, false),
	}
	for _, n := range stats.Nodes {
		result = append(result,
			metric(prefix, "jvm_heap_used_bytes", "Used JVM heap bytes.", n.JVM.Mem.Used, false),
			metric(prefix, "jvm_heap_max_bytes", "Maximum JVM heap bytes.", n.JVM.Mem.Max, false),
			metric(prefix, "docs", "Live indexed document count.", n.Indices.Docs.Count, false),
			metric(prefix, "store_bytes", "Index store bytes.", n.Indices.Store.Size, false),
			metric(prefix, "indexing_total", "Indexing operations.", n.Indices.Indexing.Total, true),
			metric(prefix, "search_query_total", "Search query operations.", n.Indices.Search.Total, true),
			metric(prefix, "fs_available_bytes", "Available filesystem bytes.", n.FS.Total.Available, false),
		)
		collectors := make([]string, 0, len(n.JVM.GC.Collectors))
		for name := range n.JVM.GC.Collectors {
			collectors = append(collectors, name)
		}
		sort.Strings(collectors)
		for _, name := range collectors {
			m := metric(prefix, "gc_collection_seconds_total", "Cumulative garbage collection seconds.", n.JVM.GC.Collectors[name].Time/1000, true)
			m.Labels = map[string]string{"collector": name}
			result = append(result, m)
		}
		pools := make([]string, 0, len(n.ThreadPools))
		for name := range n.ThreadPools {
			pools = append(pools, name)
		}
		sort.Strings(pools)
		for _, name := range pools {
			m := metric(prefix, "thread_pool_rejected_total", "Rejected thread pool tasks.", n.ThreadPools[name].Rejected, true)
			m.Labels = map[string]string{"pool": name}
			result = append(result, m)
		}
	}
	return result, nil
}
