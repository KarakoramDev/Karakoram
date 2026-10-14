//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (s *suite) rabbitSpec(version string) api.ClusterSpec {
	spec := api.ClusterSpec{Provider: "rabbitmq", Version: version, Topology: api.Topology{Type: api.TopologyTypeCluster}, Components: api.Components{Engine: api.Component{Replicas: 3, Resources: smallResources()}}, Tls: &api.TLS{Mode: api.TLSModeRequireTLS, Ca: api.TLSCaSelfSigned}, Exposure: &api.Exposure{Public: false}, Monitoring: &api.MonitoringSpec{Enabled: true}}
	spec.EngineOptions = ptr(map[string]interface{}{"plugins": []string{"rabbitmq_stream"}})
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

func (s *suite) rabbitStreamPublish(name, vhost, user, password, stream, payload string) {
	s.t.Helper()
	conn, ch := s.rabbitChannel(name, vhost, user, password, 0)
	defer conn.Close()
	defer ch.Close()
	q, err := ch.QueueDeclare(stream, true, false, false, false, amqp.Table{"x-queue-type": "stream"})
	if err != nil || q.Name != stream {
		s.t.Fatal("declaring durable stream failed")
	}
	if err := ch.Confirm(false); err != nil {
		s.t.Fatal("enabling stream publisher confirmations failed")
	}
	confirmation, err := ch.PublishWithDeferredConfirm("", stream, true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "text/plain", MessageId: stream, Body: []byte(payload)})
	if err != nil || confirmation == nil {
		s.t.Fatal("publishing persistent stream message failed")
	}
	ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	ok, err := confirmation.WaitContext(ctx)
	if err != nil || !ok {
		s.t.Fatal("stream message was not positively confirmed")
	}
	s.snapshot(name+"-stream-publish", map[string]any{"stream": stream, "confirmed": true, "persistent": true})
}

// Each consumer starts at the first offset; acknowledgements do not remove
// stream messages, so later phases can prove the original bytes again.
func (s *suite) rabbitStreamProof(name, vhost, user, password, stream, payload string) {
	s.t.Helper()
	for endpoint := range s.members(name) {
		func() {
			conn, ch := s.rabbitChannel(name, vhost, user, password, endpoint)
			defer conn.Close()
			defer ch.Close()
			q, err := ch.QueueDeclarePassive(stream, true, false, false, false, amqp.Table{"x-queue-type": "stream"})
			if err != nil || q.Name != stream {
				s.t.Fatal("durable stream disappeared")
			}
			if err := ch.Qos(10, 0, false); err != nil {
				s.t.Fatal("setting stream consumer prefetch failed")
			}
			tag := s.name()
			deliveries, err := ch.Consume(stream, tag, false, false, false, false, amqp.Table{"x-stream-offset": "first"})
			if err != nil {
				s.t.Fatal("starting stream consumer failed")
			}
			ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
			defer cancel()
			for {
				select {
				case delivery, ok := <-deliveries:
					if !ok {
						s.t.Fatal("stream consumer closed before proof")
					}
					matched := string(delivery.Body) == payload
					if matched && (delivery.DeliveryMode != amqp.Persistent || delivery.MessageId != stream) {
						s.t.Fatal("persistent stream message metadata changed")
					}
					if err := delivery.Ack(false); err != nil {
						s.t.Fatal("acknowledging stream message failed")
					}
					if matched {
						if err := ch.Cancel(tag, false); err != nil {
							s.t.Fatal("cancelling stream consumer failed")
						}
						return
					}
				case <-ctx.Done():
					s.t.Fatal("persistent stream payload not observed within deadline")
				}
			}
		}()
	}
	s.snapshot(name+"-stream-proof", map[string]any{"stream": stream, "verifiedThroughAllMembers": true})
}

func (s *suite) assertRabbitTLS(name string) {
	s.t.Helper()
	connection := s.connection(name)
	roots := x509.NewCertPool()
	if connection.CaCertificate == nil || !roots.AppendCertsFromPEM([]byte(*connection.CaCertificate)) {
		s.t.Fatal("RabbitMQ cluster CA missing or invalid")
	}
	if len(connection.Connection.Hosts) == 0 {
		s.t.Fatal("RabbitMQ private hosts missing")
	}
	for _, published := range connection.Connection.Hosts {
		host, _, err := net.SplitHostPort(published)
		if err != nil || host == "" {
			s.t.Fatal("RabbitMQ published private host is not host:port")
		}
		for _, port := range []string{"5671", "5551"} {
			ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
			dialer := tls.Dialer{Config: &tls.Config{RootCAs: roots, ServerName: host, MinVersion: tls.VersionTLS12}}
			conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
			cancel()
			if err != nil {
				s.t.Fatalf("private RabbitMQ port %s failed CA and hostname verified TLS (%T)", port, err)
			}
			conn.Close()
		}
		s.rabbitPlaintextRejected(host, "5672", []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}, false)
		// Minimal stream peer-properties request (version 1, empty properties).
		s.rabbitPlaintextRejected(host, "5552", []byte{0, 0, 0, 8, 0, 17, 0, 1, 0, 0, 0, 0}, false)
		verified := false
		config := &tls.Config{
			MinVersion: tls.VersionTLS12,
			// Distribution certificates identify peers, not a public endpoint.
			// VerifyConnection still requires a chain signed by the cluster CA.
			InsecureSkipVerify: true,
			VerifyConnection: func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 {
					return errors.New("distribution server certificate missing")
				}
				intermediates := x509.NewCertPool()
				for _, cert := range state.PeerCertificates[1:] {
					intermediates.AddCert(cert)
				}
				_, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
				verified = err == nil
				return err
			},
		}
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		dialer := tls.Dialer{Config: config}
		conn, _ := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, "25672"))
		cancel()
		if conn != nil {
			conn.Close()
		}
		// A missing client certificate may abort the handshake after the
		// server chain is verified, but cannot excuse an unobserved chain.
		if !verified {
			s.t.Fatal("distribution TLS did not present a cluster-CA-signed server certificate")
		}
		// Modern Erlang send_name: tag, flags, creation, name length, name.
		node := []byte("e2e@localhost")
		packet := make([]byte, 17+len(node))
		binary.BigEndian.PutUint16(packet[:2], uint16(len(packet)-2))
		packet[2] = 'N'
		binary.BigEndian.PutUint64(packet[3:11], 0x1074f9c)
		binary.BigEndian.PutUint16(packet[15:17], uint16(len(node)))
		copy(packet[17:], node)
		s.rabbitPlaintextRejected(host, "25672", packet, true)
	}
	s.snapshot(name+"-private-tls", map[string]bool{"amqpsCAVerified": true, "amqpsHostnameVerified": true, "amqpPlaintextRejected": true, "distributionCAVerified": true, "distributionPlaintextRejected": true, "streamCAVerified": true, "streamHostnameVerified": true, "streamPlaintextRejected": true})
}

func (s *suite) rabbitPlaintextRejected(host, port string, probe []byte, distribution bool) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return
		}
		s.t.Fatalf("private plaintext port %s probe inconclusive (%T)", port, err)
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		s.t.Fatal("setting private plaintext probe deadline failed")
	}
	if _, err := conn.Write(probe); err != nil {
		if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
			return
		}
		s.t.Fatalf("writing private plaintext port %s probe failed (%T)", port, err)
	}
	if distribution {
		var header [2]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return // Close or no valid reply within the bounded deadline.
		}
		size := binary.BigEndian.Uint16(header[:])
		if size == 0 {
			return
		}
		reply := make([]byte, int(size))
		if _, err := io.ReadFull(conn, reply); err == nil && ((reply[0] == 's' && size > 1) || (reply[0] == 'N' && size >= 19) || (reply[0] == 'n' && size >= 11)) {
			s.t.Fatal("plaintext Erlang distribution handshake reply observed")
		}
		return
	}
	var reply [1]byte
	n, err := conn.Read(reply[:])
	if n > 0 {
		s.t.Fatalf("private plaintext port %s accepted a protocol probe", port)
	}
	if err == nil || !(errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)) {
		s.t.Fatalf("private plaintext port %s did not refuse or close (%T)", port, err)
	}
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
	s.expect(&changed)(s.client.ReplaceClusterWithBody(s.ctx, name, &api.ReplaceClusterParams{Namespace: s.namespace}, "application/json", s.clusterBody(clusterWriteEnvelope{Spec: c.Spec})))
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
