// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"gopkg.in/yaml.v3"
)

var pipelineID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

func render(s agentspec.Spec, data, home, cluster, member string) (map[string][]byte, Process, error) {
	conf := filepath.Join(data, "config")
	ca := filepath.Join(conf, "certs", "ca.crt")
	files := map[string][]byte{"config/certs/ca.crt": []byte(s.CAPEM)}
	p := Process{Binary: filepath.Join(home, "bin", s.Component), Dir: home}
	var config map[string]any
	var name string
	var extra map[string]any
	var reserved []string
	switch s.Component {
	case agentspec.ComponentElasticsearch:
		e := s.Elasticsearch
		if e == nil {
			return nil, p, errors.New("missing Elasticsearch spec")
		}
		nodeName, clusterName := e.NodeName, e.ClusterName
		if nodeName == "" {
			nodeName = member
		}
		if clusterName == "" {
			clusterName = cluster
		}
		config = map[string]any{
			"cluster.name": clusterName, "node.name": nodeName, "node.roles": e.Roles,
			"path.data": filepath.Join(data, "data"), "path.logs": filepath.Join(data, "logs"),
			"network.host": "::", "network.publish_host": nodeName, "http.port": 9200, "transport.port": 9300,
			"discovery.seed_providers": "file", "discovery.seed_hosts": []string{},
			"xpack.security.enabled": true, "xpack.security.autoconfiguration.enabled": false,
			"xpack.security.transport.ssl.enabled":                 true,
			"xpack.security.transport.ssl.verification_mode":       "certificate",
			"xpack.security.transport.ssl.client_authentication":   "required",
			"xpack.security.transport.ssl.key":                     "certs/transport.key",
			"xpack.security.transport.ssl.certificate":             "certs/transport.crt",
			"xpack.security.transport.ssl.certificate_authorities": []string{"certs/ca.crt"},
			"xpack.security.http.ssl.enabled":                      true,
			"xpack.security.http.ssl.key":                          "certs/http.key",
			"xpack.security.http.ssl.certificate":                  "certs/http.crt",
			"xpack.security.http.ssl.certificate_authorities":      []string{"certs/ca.crt"},
			"xpack.security.authc.realms.file.file1.order":         0,
			"xpack.security.authc.realms.native.native1.order":     1,
		}
		if len(e.InitialMasterNodes) > 0 {
			config["cluster.initial_master_nodes"] = e.InitialMasterNodes
		}
		files["config/certs/transport.crt"] = []byte(e.TransportCert.CertPEM)
		files["config/certs/transport.key"] = []byte(e.TransportCert.KeyPEM)
		files["config/certs/http.crt"] = []byte(e.HTTPCert.CertPEM)
		files["config/certs/http.key"] = []byte(e.HTTPCert.KeyPEM)
		files["config/users"] = []byte(e.FileUsers)
		files["config/users_roles"] = []byte(e.FileUsersRoles)
		files["config/roles.yml"] = []byte(e.FileRoles)
		seeds := ""
		if len(e.SeedHosts) > 0 {
			seeds = strings.Join(e.SeedHosts, "\n") + "\n"
		}
		files["config/unicast_hosts.txt"] = []byte(seeds)
		heap := ""
		if s.HeapMB > 0 {
			heap = fmt.Sprintf("-Xms%dm\n-Xmx%dm\n", s.HeapMB, s.HeapMB)
		}
		files["config/jvm.options.d/karakoram-heap.options"] = []byte(heap)
		p.Env = []string{"ES_PATH_CONF=" + conf, "ES_TMPDIR=" + filepath.Join(data, "tmp")}
		name, extra = "elasticsearch.yml", e.Settings
		// S3 client settings are provider-generated, not managed runner keys.
		reserved = []string{"cluster.name", "node.name", "node.roles", "path", "network", "http.port", "transport.port", "discovery", "cluster.initial_master_nodes", "xpack.security", "bootstrap.memory_lock", "readiness"}
	case agentspec.ComponentKibana:
		k := s.Kibana
		if k == nil {
			return nil, p, errors.New("missing Kibana spec")
		}
		config = map[string]any{
			"server.host": "::", "server.port": 5601, "elasticsearch.hosts": k.ESHosts,
			"elasticsearch.serviceAccountToken":        k.ServiceAccountToken,
			"elasticsearch.ssl.certificateAuthorities": []string{ca}, "elasticsearch.ssl.verificationMode": "full",
			"xpack.security.encryptionKey":              k.SecurityKey,
			"xpack.encryptedSavedObjects.encryptionKey": k.SavedObjectsKey,
			"xpack.reporting.encryptionKey":             k.ReportingKey,
			"path.data":                                 filepath.Join(data, "data"), "telemetry.optIn": false, "telemetry.allowChangingOptInStatus": false,
			"logging.appenders.default.type": "console", "logging.appenders.default.layout.type": "json",
			"logging.root.appenders": []string{"default"},
		}
		if k.PublicBaseURL != "" {
			config["server.publicBaseUrl"] = k.PublicBaseURL
			config["xpack.security.secureCookies"] = true
		}
		opts := ""
		if s.HeapMB > 0 {
			opts = fmt.Sprintf("--max-old-space-size=%d\n", s.HeapMB)
		}
		files["config/node.options"] = []byte(opts)
		p.Env = []string{"KBN_PATH_CONF=" + conf}
		name, extra = "kibana.yml", k.Settings
		reserved = []string{"server.host", "server.port", "server.publicBaseUrl", "elasticsearch", "xpack.security.encryptionKey", "xpack.security.secureCookies", "xpack.encryptedSavedObjects.encryptionKey", "xpack.reporting.encryptionKey", "path.data", "telemetry", "logging"}
	case agentspec.ComponentLogstash:
		l := s.Logstash
		if l == nil {
			return nil, p, errors.New("missing Logstash spec")
		}
		queue := "memory"
		if l.PersistentQueue {
			queue = "persisted"
		}
		config = map[string]any{"api.http.host": "127.0.0.1", "api.http.port": 9600, "path.data": filepath.Join(data, "data"), "queue.type": queue, "pipeline.ecs_compatibility": "v8"}
		pipelines := l.Pipelines
		if len(pipelines) == 0 {
			pipelines = []agentspec.Pipeline{{ID: "main", Config: defaultPipeline(l.ESHosts, ca)}}
		}
		entries := make([]map[string]any, 0, len(pipelines))
		seen := map[string]bool{}
		for _, pipeline := range pipelines {
			if !pipelineID.MatchString(pipeline.ID) || seen[pipeline.ID] {
				return nil, p, errors.New("invalid or duplicate Logstash pipeline ID")
			}
			seen[pipeline.ID] = true
			rel := filepath.Join("config", "pipelines", pipeline.ID+".conf")
			files[rel] = []byte(pipeline.Config)
			entries = append(entries, map[string]any{"pipeline.id": pipeline.ID, "path.config": filepath.Join(data, rel)})
		}
		content, err := yaml.Marshal(entries)
		if err != nil {
			return nil, p, errors.New("encoding Logstash pipelines failed")
		}
		files["config/pipelines.yml"] = content
		first := ""
		if len(l.ESHosts) > 0 {
			first = l.ESHosts[0]
		}
		p.Args = []string{"--path.settings", conf}
		p.Env = []string{"KARAKORAM_ES_HOSTS=" + strings.Join(l.ESHosts, " "), "KARAKORAM_ES_HOST=" + first, "KARAKORAM_ES_USER=" + l.Writer.Username, "KARAKORAM_ES_PASSWORD=" + l.Writer.Password, "KARAKORAM_ES_CA=" + ca}
		if s.HeapMB > 0 {
			p.Env = append(p.Env, fmt.Sprintf("LS_JAVA_OPTS=-Xms%dm -Xmx%dm", s.HeapMB, s.HeapMB))
		}
		name = "logstash.yml"
	case agentspec.ComponentAPM:
		a := s.APM
		if a == nil {
			return nil, p, errors.New("missing APM Server spec")
		}
		config = map[string]any{
			"apm-server.host": "[::]:8200", "apm-server.auth.secret_token": a.SecretToken, "apm-server.auth.api_key.enabled": a.APIKeys,
			"output.elasticsearch.hosts": a.ESHosts, "output.elasticsearch.username": a.Writer.Username, "output.elasticsearch.password": a.Writer.Password,
			"output.elasticsearch.ssl.certificate_authorities": []string{ca},
			"http.enabled": true, "http.host": "127.0.0.1", "http.port": 5066, "logging.to_stderr": true,
		}
		p.Binary = filepath.Join(home, "apm-server")
		p.Args = []string{"-e", "-c", filepath.Join(conf, "apm-server.yml"), "--path.home", home, "--path.data", filepath.Join(data, "data")}
		// Leave strict.perms enabled: the configuration is owned by the agent user
		// and mode 0600, satisfying the product's ownership/write-access check.
		name = "apm-server.yml"
	default:
		return nil, p, errors.New("unknown Elastic component")
	}
	if err := mergeSettings(config, extra, reserved, ""); err != nil {
		return nil, p, err
	}
	content, err := yaml.Marshal(config)
	if err != nil {
		return nil, p, errors.New("encoding Elastic configuration failed")
	}
	files[filepath.Join("config", name)] = content
	return files, p, nil
}

func mergeSettings(dst, src map[string]any, reserved []string, prefix string) error {
	for key, value := range src {
		key = prefix + key
		if nested, ok := value.(map[string]any); ok {
			if err := mergeSettings(dst, nested, reserved, key+"."); err != nil {
				return err
			}
			continue
		}
		for _, managed := range reserved {
			if key == managed || strings.HasPrefix(key, managed+".") || strings.HasPrefix(managed, key+".") {
				return fmt.Errorf("setting %s is managed by Karakoram", key)
			}
		}
		if _, exists := dst[key]; exists {
			return fmt.Errorf("setting %s is managed by Karakoram", key)
		}
		dst[key] = value
	}
	return nil
}

func defaultPipeline(hosts []string, ca string) string {
	quoted := make([]string, len(hosts))
	for i, host := range hosts {
		quoted[i] = strconv.Quote(host)
	}
	return `input {
  beats { host => "::" port => 5044 }
  http { host => "::" port => 8080 }
}
output {
  elasticsearch {
    hosts => [` + strings.Join(quoted, ", ") + `]
    user => "${KARAKORAM_ES_USER}"
    password => "${KARAKORAM_ES_PASSWORD}"
    ssl_enabled => true
    ssl_certificate_authorities => [` + strconv.Quote(ca) + `]
    data_stream => true
  }
}
`
}
