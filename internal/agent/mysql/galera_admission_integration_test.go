//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func nativeUntrustedTrafficPeer(t *testing.T, s myspec.Spec, kind string) myspec.Certificate {
	t.Helper()
	if kind == "SQL-CA" {
		return *s.ServerCert
	}
	ca, leaf := coreTLS(t)
	// coreTLS uses one test-only key for its CA and first leaf. The self-signed
	// CA certificate below therefore has the matching private key, not a mismatch.
	candidate := myspec.Certificate{CertPEM: ca, KeyPEM: leaf.KeyPEM}
	if _, err := tls.X509KeyPair([]byte(candidate.CertPEM), []byte(candidate.KeyPEM)); err != nil {
		t.Fatal(err)
	}
	return candidate
}
func nativeTrafficPeerPolicy(t *testing.T, s myspec.Spec, candidate myspec.Certificate) *tls.Config {
	t.Helper()
	pair, err := tls.X509KeyPair([]byte(candidate.CertPEM), []byte(candidate.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    x509Pool(s.Galera.TrafficCAPEM),
		ServerName: nativeCertificateName(t, s.Galera.TrafficCert.CertPEM),
		// Do not let automatic issuer filtering omit this deliberately
		// unauthorized candidate. The receiver must reject the actual leaf.
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &pair, nil
		},
	}
}

// Await only a not-yet-open listener. Once TCP connects, an explicit certificate
// alert is mandatory; EOF/timeouts/application errors are not rejection proof.
func nativeTrafficPeerRejected(ctx context.Context, address string, policy *tls.Config, awaitListener bool) error {
	for {
		raw, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", address)
		if err != nil {
			if awaitListener && errors.Is(err, syscall.ECONNREFUSED) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Millisecond):
					continue
				}
			}
			return fmt.Errorf("peer TCP connection: %w", err)
		}
		conn := tls.Client(raw, policy)
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		err = conn.HandshakeContext(ctx)
		if err == nil {
			_, err = conn.Read(make([]byte, 1))
		}
		conn.Close()
		if !nativeRemoteCertificateRejection(err) {
			return fmt.Errorf("peer did not receive an explicit certificate rejection: %v", err)
		}
		return nil
	}
}

// Closing done publishes the immutable result to both the normal waiter and
// failure diagnostics. Observation never consumes or resubmits a probe result.
type nativeISTAdmissionResult struct {
	done chan struct{}
	err  error
}

func (r *nativeISTAdmissionResult) wait() error {
	<-r.done
	return r.err
}

func nativeISTAdmissionProbe(t *testing.T, ctx context.Context, s myspec.Spec, kind string) *nativeISTAdmissionResult {
	t.Helper()
	candidate := nativeUntrustedTrafficPeer(t, s, kind)
	policy := nativeTrafficPeerPolicy(t, s, candidate)
	local := galeraLocal(s)
	address := galeraAddress(local.Host, s.Galera.ISTPort)
	result := &nativeISTAdmissionResult{done: make(chan struct{})}
	t.Cleanup(func() {
		if t.Failed() {
			select {
			case <-result.done:
				t.Logf("%s IST %s admission probe finished: %s", s.Member, kind, nativeGaleraRedact(s, "native-galera-token-"+s.Member, fmt.Sprint(result.err)))
			default:
				t.Logf("%s IST %s admission probe still pending at failure", s.Member, kind)
			}
		}
	})
	go func() {
		// A retained unsynced start marker delays the listener by the hold-off.
		probe, cancel := context.WithTimeout(ctx, galeraTransferHoldoff+90*time.Second)
		defer cancel()
		result.err = nativeTrafficPeerRejected(probe, address, policy, true)
		close(result.done)
	}()
	return result
}

// Run the pinned, unmodified upstream SST joiner script against an isolated
// scratch datadir with the real exported traffic policy. No donor races this
// listener, so unauthorized transfer rejection is deterministic and cannot
// damage a formed member. Successful SST joins are separately proven via Runner.
func nativeSSTAdmissionProof(t *testing.T, ctx context.Context, s myspec.Spec) {
	t.Helper()
	for _, kind := range []string{"SQL-CA", "self-signed"} {
		nativeSSTRejectPeer(t, ctx, s, kind)
	}
}
func nativeSSTRejectPeer(t *testing.T, ctx context.Context, s myspec.Spec, kind string) {
	t.Helper()
	s = nativeSpec(s)
	p := dataPaths(nativeTestDir(t))
	for _, dir := range []string{p.root, p.data, p.run} {
		if err := privateDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	for file, value := range map[string]string{"ca.pem": s.CAPEM, "server-cert.pem": s.ServerCert.CertPEM, "server-key.pem": s.ServerCert.KeyPEM, "galera-ca.pem": s.Galera.TrafficCAPEM, "galera-cert.pem": s.Galera.TrafficCert.CertPEM, "galera-key.pem": s.Galera.TrafficCert.KeyPEM, "startup.sql": ""} {
		if err := os.WriteFile(filepath.Join(p.root, file), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(p.config, []byte(renderConfig(s, p)), 0600); err != nil {
		t.Fatal(err)
	}
	run, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	port := nativeTestPort(t)
	address := galeraAddress("127.0.0.1", port)
	cmd := exec.CommandContext(run, filepath.Join(s.ServerDir, "bin", "wsrep_sst_xtrabackup-v2"), "--role", "joiner", "--address", address+"/xtrabackup_sst//1", "--datadir", p.data, "--defaults-file", p.config, "--parent", strconv.Itoa(os.Getpid()), "--mysqld-version", s.Version, "--basedir", s.ServerDir, "--plugindir", filepath.Join(s.ServerDir, "lib", "plugin"), "--socket", p.socket, "--binlog", filepath.Join(p.root, "binlog"))
	cmd.Env = galeraEnv(s)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) }
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.OpenFile(filepath.Join(p.root, "sst-admission.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{}, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "ready ") {
				select {
				case ready <- struct{}{}:
				default:
				}
			}
		}
	}()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); close(done) }()
	defer func() {
		select {
		case <-done:
			return
		default:
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
	}()
	select {
	case <-ready:
	case err := <-done:
		raw, _ := os.ReadFile(log.Name())
		t.Fatalf("upstream SST %s receiver exited before ready: %v log=%s", kind, err, raw)
	case <-run.Done():
		t.Fatal("upstream SST receiver readiness", run.Err())
	}
	policy := nativeTrafficPeerPolicy(t, s, nativeUntrustedTrafficPeer(t, s, kind))
	if err = nativeTrafficPeerRejected(run, address, policy, false); err != nil {
		t.Fatalf("SST %s peer: %v", kind, err)
	}
	if _, err = os.Stat(filepath.Join(p.data, "mysql")); !os.IsNotExist(err) {
		t.Fatal("unauthorized SST populated a MySQL datadir before receiver cleanup")
	}
	// The unchanged script adds retry=30 to socat and can remain waiting for
	// an authorized donor after rejecting this peer. Process exit is not the
	// rejection contract: the strict remote certificate alert above is proof,
	// established before any cancellation of our isolated receiver.
	t.Logf("upstream SST explicitly rejected presented %s certificate; no MySQL data admitted before cleanup", kind)
	select {
	case <-done:
		// A natural receiver exit is also allowed; its status is not used as
		// evidence that the unauthorized certificate was rejected.
	default:
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
	}
	if _, err = os.Stat(filepath.Join(p.data, "mysql")); !os.IsNotExist(err) {
		t.Fatal("unauthorized SST populated a MySQL datadir")
	}
	t.Logf("isolated upstream SST receiver cleaned after independently verified %s TLS rejection", kind)
}

func TestNativeTrafficPeerPolicyForcesUnauthorizedLeaf(t *testing.T) {
	s := testGaleraSpec()
	ca, traffic := coreTLS(t)
	s.Galera.TrafficCAPEM = ca
	s.Galera.TrafficCert = &traffic
	_, candidate := coreTLS(t)
	policy := nativeTrafficPeerPolicy(t, s, candidate)
	if policy.GetClientCertificate == nil {
		t.Fatal("unauthorized candidate can be omitted by automatic issuer filtering")
	}
	actual, err := policy.GetClientCertificate(&tls.CertificateRequestInfo{
		AcceptableCAs: [][]byte{[]byte("unrelated issuer")},
	})
	if err != nil {
		t.Fatal(err)
	}
	expected, err := tls.X509KeyPair([]byte(candidate.CertPEM), []byte(candidate.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	if actual == nil || len(actual.Certificate) == 0 || !bytes.Equal(actual.Certificate[0], expected.Certificate[0]) {
		t.Fatal("peer policy did not present the exact unauthorized candidate")
	}
}
