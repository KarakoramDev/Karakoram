// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func isES(m *member) bool { return m.Kind == kindNode || m.Kind == kindMaster }
func component(spec domain.ClusterSpec, m *member) domain.ComponentSpec {
	if m.Kind == kindNode {
		return spec.Components.Engine
	}
	return spec.Components.Extra[m.Kind]
}
func product(m *member) string {
	switch m.Kind {
	case kindKibana:
		return agentspec.ComponentKibana
	case kindLogstash:
		return agentspec.ComponentLogstash
	case kindAPM:
		return agentspec.ComponentAPM
	default:
		return agentspec.ComponentElasticsearch
	}
}
func roles(spec domain.ClusterSpec, m *member) []string {
	if m.Kind == kindMaster {
		return []string{"master"}
	}
	r := []string{"data", "ingest", "remote_cluster_client", "transform"}
	if spec.Topology.Type == "cluster" {
		r = append([]string{"master"}, r...)
	}
	return r
}
func masterEligible(spec domain.ClusterSpec, m *member) bool {
	return m.Kind == kindMaster || m.Kind == kindNode && spec.Topology.Type == "cluster"
}
func runtimeResources(m *member, r domain.Resources) domain.Resources {
	if isES(m) {
		r.MinDisk = 1
		r.MaxDisk = 3
	}
	return r
}
func (s *state) componentMembers() []*member { return s.members(kindKibana, kindLogstash, kindAPM) }
func (p *Provider) bumpConfig(s *state) {
	s.ConfigVersion++
	for _, m := range s.Members {
		m.ConfigVersion = s.ConfigVersion
	}
}
func (p *Provider) reloadMembers(ctx context.Context, rc *engine.Context, names ...string) {
	for _, name := range names {
		a, e := p.agentClient(ctx, rc, name)
		if e == nil {
			_ = a.Reload(ctx)
			a.Close()
		}
	}
}
func (p *Provider) agentClient(ctx context.Context, rc *engine.Context, name string) (*agentapi.Client, error) {
	return rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
}
func (p *Provider) staticSettings(ctx context.Context, rc *engine.Context, s *state) (map[string]any, map[string]string, error) {
	settings := map[string]any{}
	secure := map[string]string{}
	for _, name := range s.Storages {
		namespace, storageName := storageRef(rc.Cluster.Ref().Namespace, name)
		storage, err := rc.Store.GetBackupStorage(ctx, namespace, storageName)
		if err != nil {
			return nil, nil, err
		}
		client := S3ClientName(domain.NormalizedNamespace(storage.Namespace), storage.Name)
		for k, v := range S3ClientSettings(client, storage.EndpointURL, storage.Region) {
			settings[k] = v
		}
		for k, v := range S3SecureSettings(client, storage.Credentials.AccessKeyID, storage.Credentials.SecretAccessKey) {
			secure[k] = v
		}
	}
	return settings, secure, nil
}
func (p *Provider) desiredSpec(ctx context.Context, rc *engine.Context, s *state, m *member, version string) (agentspec.Spec, error) {
	o, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return agentspec.Spec{}, err
	}
	v := agentspec.Spec{Component: product(m), Version: version}
	ram := component(rc.Cluster.Spec, m).Resources.MinRAM
	if isES(m) {
		settings, secure, err := p.staticSettings(ctx, rc, s)
		if err != nil {
			return v, err
		}
		user, err := FlattenSettings(o.Elasticsearch.Settings)
		if err != nil {
			return v, err
		}
		for k, val := range user {
			settings[k] = val
		}
		v.HeapMB = ElasticsearchHeapMB(ram)
		v.Elasticsearch = &agentspec.ElasticsearchSpec{ClusterName: rc.Cluster.Name, NodeName: m.Name, Roles: roles(rc.Cluster.Spec, m), Settings: settings, SecureSettings: secure}
		v.RestartToken, err = NodeRestartToken(version, v.HeapMB, v.Elasticsearch.Roles, settings, nil)
		return v, err
	}
	hosts := []string{}
	for _, node := range s.members(kindNode) {
		if !node.Removing {
			hosts = append(hosts, "https://"+node.Name+":9200")
		}
	}
	get := func(key string) (string, error) { b, e := secret(ctx, rc, key); return string(b), e }
	switch m.Kind {
	case kindKibana:
		v.HeapMB = KibanaHeapMB(ram)
		k := &agentspec.KibanaSpec{ESHosts: hosts, Settings: o.Kibana.Settings}
		if k.ServiceAccountToken, err = get(secretKibanaToken); err != nil {
			return v, err
		}
		if k.SecurityKey, err = get(secretKibanaSecurity); err != nil {
			return v, err
		}
		if k.SavedObjectsKey, err = get(secretKibanaSavedObjects); err != nil {
			return v, err
		}
		if k.ReportingKey, err = get(secretKibanaReporting); err != nil {
			return v, err
		}
		if o.Kibana.Public {
			if sub, ok := rc.Backend.(infra.SubdomainAccess); ok {
				urls, e := sub.SubdomainURLs(ctx, m.Name)
				if e != nil {
					return v, e
				}
				if len(urls) > 0 {
					k.PublicBaseURL = urls[0]
				}
			}
		}
		v.Kibana = k
	case kindLogstash:
		v.HeapMB = LogstashHeapMB(ram)
		pw, e := get(secretLogstash)
		if e != nil {
			return v, e
		}
		v.Logstash = &agentspec.LogstashSpec{ESHosts: hosts, Writer: agentspec.Credentials{Username: LogstashUser, Password: pw}, Pipelines: o.Logstash.Pipelines, PersistentQueue: o.Logstash.PersistentQueue}
	case kindAPM:
		pw, e := get(secretAPM)
		if e != nil {
			return v, e
		}
		token, e := get(secretAPMToken)
		if e != nil {
			return v, e
		}
		v.APM = &agentspec.APMSpec{ESHosts: hosts, Writer: agentspec.Credentials{Username: APMUser, Password: pw}, SecretToken: token, APIKeys: o.APM.APIKeys != nil && *o.APM.APIKeys}
	}
	if m.HeapMB > 0 {
		v.HeapMB = m.HeapMB
	}
	static := v
	if v.Kibana != nil {
		k := *v.Kibana
		k.ESHosts = nil
		k.ServiceAccountToken = ""
		k.SecurityKey = ""
		k.SavedObjectsKey = ""
		k.ReportingKey = ""
		static.Kibana = &k
	}
	if v.Logstash != nil {
		l := *v.Logstash
		l.ESHosts = nil
		l.Writer = agentspec.Credentials{}
		static.Logstash = &l
	}
	if v.APM != nil {
		a := *v.APM
		a.ESHosts = nil
		a.Writer = agentspec.Credentials{}
		a.SecretToken = ""
		static.APM = &a
	}
	b, err := json.Marshal(static)
	if err != nil {
		return v, err
	}
	v.RestartToken = RestartToken(string(b))
	return v, nil
}

// publishConfig activates restart-relevant content for exactly one member. The
// encrypted payload keeps future desired changes out of other members' pulls.
func (p *Provider) publishConfig(ctx context.Context, rc *engine.Context, s *state, m *member, version, nonce string) error {
	intent := *m
	intent.HeapMB = 0
	v, err := p.desiredSpec(ctx, rc, s, &intent, version)
	if err != nil {
		return err
	}
	configHash := v.RestartToken
	if nonce != "" {
		v.RestartToken = RestartToken(v.RestartToken, nonce)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err = putSecret(ctx, rc, "members/"+m.Name+"/restart-spec", b); err != nil {
		return err
	}
	m.RestartToken = v.RestartToken
	m.ConfigHash = configHash
	m.HeapMB = v.HeapMB
	p.bumpConfig(s)
	return save(rc.Cluster, s)
}

// MemberConfig renders a member's currently activated secret-bearing payload.
func (p *Provider) MemberConfig(ctx context.Context, rc *engine.Context, name string) (agentapi.MemberConfig, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	m := s.find(name)
	if m == nil {
		return agentapi.MemberConfig{}, engine.ErrNotFound
	}
	v := agentapi.MemberConfig{Cluster: rc.Cluster.Name, Member: name, Engine: agentspec.Engine, Role: m.Kind, ConfigVersion: s.ConfigVersion, DesiredProcesses: []string{product(m)}, EngineVersion: m.Version}
	b, err := secret(ctx, rc, "members/"+name+"/restart-spec")
	if err != nil {
		return v, err
	}
	var spec agentspec.Spec
	if err = json.Unmarshal(b, &spec); err != nil {
		return v, err
	}
	v.EngineVersion = spec.Version
	get := func(key string) (string, error) { b, e := secret(ctx, rc, key); return string(b), e }
	if spec.CAPEM, err = get(secretCA); err != nil {
		return v, err
	}
	if v.AgentTLS.CertPEM, err = get("tls/agents/" + name + ".crt"); err != nil {
		return v, err
	}
	if v.AgentTLS.KeyPEM, err = get("tls/agents/" + name + ".key"); err != nil {
		return v, err
	}
	if isES(m) {
		v.Port = 9200
		e := spec.Elasticsearch
		e.SeedHosts = nil
		for _, seed := range s.esNodes() {
			if !seed.Removing && masterEligible(rc.Cluster.Spec, seed) {
				e.SeedHosts = append(e.SeedHosts, seed.Name+":9300")
			}
		}
		e.InitialMasterNodes = nil
		if s.ClusterUUID == "" {
			e.InitialMasterNodes = append([]string(nil), s.InitialMasterNodes...)
		}
		for _, cert := range []struct {
			kind string
			out  *agentspec.Certificate
		}{{"transport", &e.TransportCert}, {"http", &e.HTTPCert}} {
			base := "tls/nodes/" + name + "/" + cert.kind
			if cert.out.CertPEM, err = get(base + ".crt"); err != nil {
				return v, err
			}
			if cert.out.KeyPEM, err = get(base + ".key"); err != nil {
				return v, err
			}
		}
		admin, err := get(secretAdmin)
		if err != nil {
			return v, err
		}
		monitor, err := get(secretMonitor)
		if err != nil {
			return v, err
		}
		previous, err := get(secretFileRealm)
		if err != nil && !missing(err) {
			return v, err
		}
		e.FileUsers, e.FileUsersRoles, err = RenderFileRealm(map[string]string{AdminUser: admin, MonitorUser: monitor}, map[string][]string{"superuser": {AdminUser}, MonitorRole: {MonitorUser}}, previous)
		if err != nil {
			return v, err
		}
		if e.FileUsers != previous {
			if err = putSecret(ctx, rc, secretFileRealm, []byte(e.FileUsers)); err != nil {
				return v, err
			}
		}
		if e.FileRoles, err = RenderFileRoles(); err != nil {
			return v, err
		}
		e.Monitor = agentspec.Credentials{Username: MonitorUser, Password: monitor}
		_, secure, err := p.staticSettings(ctx, rc, s)
		if err != nil {
			return v, err
		}
		e.SecureSettings = secure
	} else {
		hosts := []string{}
		for _, node := range s.members(kindNode) {
			if !node.Removing {
				hosts = append(hosts, "https://"+node.Name+":9200")
			}
		}
		switch m.Kind {
		case kindKibana:
			k := spec.Kibana
			k.ESHosts = hosts
			if k.ServiceAccountToken, err = get(secretKibanaToken); err != nil {
				return v, err
			}
			if k.SecurityKey, err = get(secretKibanaSecurity); err != nil {
				return v, err
			}
			if k.SavedObjectsKey, err = get(secretKibanaSavedObjects); err != nil {
				return v, err
			}
			if k.ReportingKey, err = get(secretKibanaReporting); err != nil {
				return v, err
			}
		case kindLogstash:
			spec.Logstash.ESHosts = hosts
			if spec.Logstash.Writer.Password, err = get(secretLogstash); err != nil {
				return v, err
			}
		case kindAPM:
			spec.APM.ESHosts = hosts
			if spec.APM.Writer.Password, err = get(secretAPM); err != nil {
				return v, err
			}
			if spec.APM.SecretToken, err = get(secretAPMToken); err != nil {
				return v, err
			}
		}
		// Component credentials and discovery are restart-relevant, unlike the
		// Elasticsearch file realm and seed file. Include their current digest.
		dynamic, e := json.Marshal([]any{spec.Kibana, spec.Logstash, spec.APM})
		if e != nil {
			return v, e
		}
		spec.RestartToken = RestartToken(spec.RestartToken, string(dynamic))
		switch m.Kind {
		case kindKibana:
			v.Port = 5601
		case kindLogstash:
			v.Port = 9600
		case kindAPM:
			v.Port = 8200
		}
		if s.RestoreActive {
			v.DesiredProcesses = nil
		}
	}
	v.TLS = agentapi.TLSConfig{Mode: "requireTLS", CAPEM: spec.CAPEM}
	v.EngineSpec, err = json.Marshal(spec)
	return v, err
}

var _ engine.AgentConfigSource = (*Provider)(nil)
