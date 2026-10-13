//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (s *suite) rabbitSpec(version string) api.ClusterSpec {
	spec := api.ClusterSpec{Provider: "rabbitmq", Version: version, Topology: api.Topology{Type: api.TopologyTypeCluster}, Components: api.Components{Engine: api.Component{Replicas: 3, Resources: smallResources()}}, Tls: &api.TLS{Mode: api.TLSModeRequireTLS, Ca: api.TLSCaSelfSigned}, Exposure: &api.Exposure{Public: false}, Monitoring: &api.MonitoringSpec{Enabled: true}}
	if os.Getenv("KARAKORAM_E2E_ALLOW_CIDR") != "" {
		spec.Exposure = &api.Exposure{Public: true, IpType: ptr(api.ExposureIpTypeIPV6), Allowlist: ptr([]string{requireCIDR(s.t)})}
	}
	return spec
}

func (s *suite) assertRabbitMembers(name string, count int, version string) {
	s.t.Helper()
	c := s.ready(name)
	members := s.members(name)
	if len(members) != count || len(c.Status.Members) != count {
		s.t.Fatalf("RabbitMQ member count: list=%d status=%d want=%d", len(members), len(c.Status.Members), count)
	}
	seen := make(map[string]bool, count)
	for _, m := range members {
		if seen[m.Name] || !m.Health || m.Role != "broker" || m.State != "RUNNING" || m.Set != "rabbitmq" || m.Version == nil || *m.Version != version || m.Agent == nil || !m.Agent.Connected {
			s.t.Fatalf("RabbitMQ member %s lacks unique healthy RUNNING broker state at version %s", m.Name, version)
		}
		seen[m.Name] = true
		var individual api.Member
		s.expect(&individual)(s.client.GetClusterMember(s.ctx, name, m.Name, nil))
		if individual.Name != m.Name || !individual.Health {
			s.t.Fatalf("RabbitMQ member detail disagrees for %s", m.Name)
		}
	}
	for _, m := range c.Status.Members {
		if !seen[m.Name] || !m.Health {
			s.t.Fatal("RabbitMQ status and member inventory disagree")
		}
	}
	s.snapshot(name+"-rabbitmq-members", members)
	conn := s.connection(name)
	if conn.CaCertificate == nil || *conn.CaCertificate == "" || len(conn.Connection.Hosts) != count || conn.Connection.Endpoints == nil {
		s.t.Fatal("RabbitMQ connection missing CA, private hosts or endpoints")
	}
	_, password := credential(conn, "admin")
	if password == "" {
		s.t.Fatal("RabbitMQ connection missing admin credentials")
	}
	private, management, public := 0, 0, 0
	for _, ep := range *conn.Connection.Endpoints {
		u, err := url.Parse(ep.Url)
		if err != nil || u.Host == "" || u.User != nil {
			s.t.Fatal("RabbitMQ endpoint is malformed or contains credentials")
		}
		switch ep.Name {
		case "amqps":
			if u.Scheme != "amqps" {
				s.t.Fatal("RabbitMQ AMQPS endpoint uses wrong scheme")
			}
			if ep.Public {
				public++
			} else {
				private++
			}
		case "management":
			if u.Scheme != "https" {
				s.t.Fatal("RabbitMQ management endpoint is not HTTPS")
			}
			if !ep.Public {
				management++
			}
		}
	}
	wantPublic := 0
	if c.Spec.Exposure.Public {
		wantPublic = count
	}
	if private != count || management != count || public != wantPublic {
		s.t.Fatalf("RabbitMQ endpoints: private=%d management=%d public=%d want=%d/%d/%d", private, management, public, count, count, wantPublic)
	}
	for _, cred := range conn.Credentials {
		if cred.Username != "admin" || cred.Database != "/" || cred.Password == nil || *cred.Password == "" || cred.Uri == nil {
			s.t.Fatal("RabbitMQ connection returned incomplete credentials")
		}
		u, err := url.Parse(*cred.Uri)
		if err != nil || u.Scheme != "amqps" || u.User == nil || u.User.Username() != "admin" {
			s.t.Fatal("RabbitMQ credential URI is malformed")
		}
		p, present := u.User.Password()
		if !present || p != *cred.Password {
			s.t.Fatal("RabbitMQ credential URI password mismatch")
		}
	}
	// Credential-bearing connection responses are deliberately never snapshotted.
}

// rabbitData marks only the data-plane subtest skipped; API phases still run.
func (s *suite) rabbitData(phase string, fn func(*suite)) bool {
	return s.t.Run(phase, func(t *testing.T) {
		if os.Getenv("KARAKORAM_E2E_ALLOW_CIDR") == "" {
			t.Skip("AMQPS data-path checks require KARAKORAM_E2E_ALLOW_CIDR and public IPv6 connectivity; private broker DNS is not reachable externally")
		}
		child := *s
		child.t = t
		fn(&child)
	})
}

func (s *suite) rabbitChannel(name, vhost, user, password string, endpoint int) (*amqp.Connection, *amqp.Channel) {
	s.t.Helper()
	conn := s.connection(name)
	if password == "" {
		_, password = credential(conn, user)
	}
	if password == "" || conn.CaCertificate == nil {
		s.t.Fatal("AMQPS credentials or CA missing")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(*conn.CaCertificate)) {
		s.t.Fatal("AMQPS CA is invalid")
	}
	var endpoints []api.Endpoint
	if conn.Connection.Endpoints != nil {
		for _, ep := range *conn.Connection.Endpoints {
			if ep.Public && ep.Name == "amqps" {
				endpoints = append(endpoints, ep)
			}
		}
	}
	if endpoint >= len(endpoints) {
		s.t.Fatal("public AMQPS endpoint missing")
	}
	u, err := url.Parse(endpoints[endpoint].Url)
	if err != nil {
		s.t.Fatal("invalid public AMQPS URL")
	}
	u.User = url.UserPassword(user, password)
	var connection *amqp.Connection
	var socket net.Conn
	err = retryPublicProbe(s.ctx, func(ctx context.Context) error {
		var dialErr error
		connection, dialErr = amqp.DialConfig(u.String(), amqp.Config{
			Vhost: vhost, Heartbeat: 10 * time.Second,
			TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
			Dial: func(network, address string) (net.Conn, error) {
				c, e := (&net.Dialer{}).DialContext(ctx, network, address)
				if e != nil {
					return nil, e
				}
				socket = c
				deadline, _ := ctx.Deadline()
				if e = c.SetDeadline(deadline); e != nil {
					c.Close()
					return nil, e
				}
				return c, nil
			},
		})
		return dialErr
	})
	if err != nil {
		s.t.Fatalf("public AMQPS connection failed (%T); credentials withheld", err)
	}
	// DialConfig clears its handshake deadline; bound subsequent channel RPCs.
	deadline := time.Now().Add(time.Minute)
	if d, ok := s.ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := socket.SetDeadline(deadline); err != nil {
		connection.Close()
		s.t.Fatal("setting AMQPS deadline failed")
	}
	ch, err := connection.Channel()
	if err != nil {
		connection.Close()
		s.t.Fatal("opening AMQP channel failed")
	}
	return connection, ch
}

func (s *suite) rabbitPublish(name, vhost, user, password, queue, payload string) {
	s.t.Helper()
	conn, ch := s.rabbitChannel(name, vhost, user, password, 0)
	defer conn.Close()
	defer ch.Close()
	q, err := ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
	if err != nil || q.Name != queue {
		s.t.Fatal("declaring durable quorum queue failed")
	}
	if err := ch.Confirm(false); err != nil {
		s.t.Fatal("enabling publisher confirmations failed")
	}
	confirmation, err := ch.PublishWithDeferredConfirm("", queue, true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "text/plain", MessageId: queue, Body: []byte(payload)})
	if err != nil || confirmation == nil {
		s.t.Fatal("publishing persistent quorum message failed")
	}
	ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	ok, err := confirmation.WaitContext(ctx)
	if err != nil || !ok {
		s.t.Fatal("quorum message was not positively confirmed")
	}
	s.snapshot(name+"-quorum-publish", map[string]any{"queue": queue, "confirmed": true, "persistent": true})
}

// rabbitProof uses passive declaration, never recreates a lost queue, and
// requeues the one proof message so the same bytes survive every later phase.
func (s *suite) rabbitProof(name, vhost, user, password, queue, payload string) {
	s.t.Helper()
	for endpoint := range s.members(name) {
		func() {
			conn, ch := s.rabbitChannel(name, vhost, user, password, endpoint)
			defer conn.Close()
			defer ch.Close()
			q, err := ch.QueueDeclarePassive(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
			if err != nil || q.Name != queue {
				s.t.Fatal("durable quorum queue disappeared")
			}
			delivery, ok, err := ch.Get(queue, false)
			if err != nil || !ok || string(delivery.Body) != payload || delivery.DeliveryMode != amqp.Persistent || delivery.MessageId != queue {
				s.t.Fatal("persistent quorum message missing or changed")
			}
			if err := delivery.Nack(false, true); err != nil {
				s.t.Fatal("requeueing quorum proof failed")
			}
			// A synchronous passive declaration orders the nack before disconnect.
			if _, err := ch.QueueInspect(queue); err != nil {
				s.t.Fatal("quorum queue inspection after requeue failed")
			}
		}()
	}
	s.snapshot(name+"-quorum-proof", map[string]any{"queue": queue, "verifiedThroughAllMembers": true})
}

func (s *suite) assertRabbitMetrics(name string) {
	var baseline api.ClusterMetrics
	s.expect(&baseline)(s.client.GetClusterMetrics(s.ctx, name, &api.GetClusterMetricsParams{Range: ptr("15m")}))
	members := s.members(name)
	s.poll("fresh RabbitMQ per-member metrics", 5*time.Minute, func() bool {
		var metrics api.ClusterMetrics
		s.expect(&metrics)(s.client.GetClusterMetrics(s.ctx, name, &api.GetClusterMetricsParams{Range: ptr("15m")}))
		seen := freshMetricNames(metrics, baseline.End)
		for _, m := range members {
			for _, metric := range []string{"rabbitmq.up", "rabbitmq.connections", "rabbitmq.queues", "rabbitmq.messagesReady", "rabbitmq.clusterMembersRunning"} {
				if !seen[m.Name][metric] {
					return false
				}
			}
		}
		s.snapshot(name+"-rabbitmq-metrics", metrics)
		return true
	})
	s.assertPrometheus(name)
}

func (s *suite) rabbitVhost(name string) string {
	vhost := s.name()
	var created api.RabbitMQVhost
	s.expect(&created, 201)(s.client.CreateRabbitMQVhost(s.ctx, name, nil, api.RabbitMQVhost{Name: vhost, Tags: []string{"e2e"}, Description: ptr("RabbitMQ end-to-end test"), DefaultQueueType: ptr(api.Quorum)}))
	if created.Name != vhost {
		s.t.Fatal("created vhost name mismatch")
	}
	return vhost
}

func (s *suite) rabbitUpgrade(name string) {
	c := s.cluster(name)
	c.Spec.Version = "4.3.6"
	var changed api.Cluster
	s.expect(&changed)(s.client.ReplaceCluster(s.ctx, name, nil, api.ReplaceClusterRequest{Spec: c.Spec}))
	mixed := false
	s.poll("rolling RabbitMQ 4.3.5 to 4.3.6 upgrade", convergenceTimeout, func() bool {
		current := s.cluster(name)
		if current.Status.Phase == api.ClusterStatusPhaseError {
			s.snapshot(name+"-upgrade-error", current.Status)
			s.t.Fatal("RabbitMQ upgrade entered Error")
		}
		old, next, unhealthy := 0, 0, 0
		for _, member := range current.Status.Members {
			if !member.Health {
				unhealthy++
			}
			if member.Version != nil {
				switch strings.TrimSpace(*member.Version) {
				case "4.3.5":
					old++
				case "4.3.6":
					next++
				default:
					s.t.Fatal("unexpected observed RabbitMQ upgrade version")
				}
			}
		}
		if unhealthy > 1 || len(current.Status.Members) != 3 {
			s.t.Fatal("rolling RabbitMQ upgrade lost its healthy majority")
		}
		mixed = mixed || old > 0 && next > 0
		s.t.Logf("rolling upgrade old=%d new=%d unhealthy=%d", old, next, unhealthy)
		s.snapshot(name+"-upgrade-progress", current.Status)
		return current.Status.Phase == api.ClusterStatusPhaseReady && current.Status.ObservedGeneration >= changed.Generation && next == 3
	})
	if !mixed {
		s.t.Fatal("no mixed-version transition observed; rolling upgrade evidence is inconclusive")
	}
	s.assertRabbitMembers(name, 3, "4.3.6")
}

func rabbitPayload(queue string) string { return fmt.Sprintf("persistent-quorum-proof:%s", queue) }
