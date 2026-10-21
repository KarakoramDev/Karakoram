// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build native

package postgres

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"gopkg.in/yaml.v3"
)

// TestNativeCitusRenderedFormation uses isolated loopback addresses, native
// binaries and temporary data. No root access or platform deployment is needed.
func TestNativeCitusRenderedFormation(t *testing.T) {
	bin := os.Getenv("KARAKORAM_CITUS_PG_BIN")
	patroni := os.Getenv("KARAKORAM_CITUS_PATRONI")
	etcd := os.Getenv("KARAKORAM_CITUS_ETCD")
	if bin == "" || patroni == "" || etcd == "" {
		t.Skip("set KARAKORAM_CITUS_PG_BIN, KARAKORAM_CITUS_PATRONI and KARAKORAM_CITUS_ETCD to run native Citus acceptance")
	}
	for _, path := range []string{filepath.Join(bin, "postgres"), patroni, etcd} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	ca, cert := nativeCitusTLS(t)
	var specs [3]pgspec.Spec
	var dirs [3]paths
	var configs [3]string
	backrest := os.Getenv("KARAKORAM_CITUS_PGBACKREST")
	for i := range specs {
		s := testSpec()
		s.Scope = fmt.Sprintf("native-citus-%d", time.Now().UnixNano())
		if i > 0 {
			s.Scope = specs[0].Scope
		}
		s.Member = fmt.Sprintf("member%d", i)
		s.Topology = pgspec.TopologyDistributed
		s.Citus = &pgspec.Citus{Group: 0, Database: "postgres"}
		if i > 0 {
			s.Citus.Group = 1
		}
		s.BinDir = bin
		s.ListenHost = fmt.Sprintf("127.0.0.%d", 81+i)
		s.CAPEM = ca
		s.ServerCert = cert
		s.Postgres.ClientCIDRs = []string{"127.0.0.0/8"}
		s.Postgres.Synchronous = pgspec.SyncOff
		s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30", "max_worker_processes": "16", "citus.max_shared_pool_size": "10", "citus.max_adaptive_executor_pool_size": "4"}
		s.Postgres.Parameters["checkpoint_completion_target"] = "0.0"
		if i == 0 {
			s.Peers = []pgspec.Peer{{Name: s.Member, Host: s.ListenHost, Etcd: "voter"}}
			s.Etcd = &pgspec.Etcd{InitialCluster: s.Member + "=https://" + s.ListenHost + ":2380", State: "new", Token: s.Scope}
		} else {
			s.Peers = []pgspec.Peer{{Name: "member1", Host: "127.0.0.82"}, {Name: "member2", Host: "127.0.0.83"}}
			s.Etcd = &pgspec.Etcd{Endpoints: []string{"127.0.0.81:2379"}}
		}
		p := dataPaths(filepath.Join(root, s.Member))
		for _, dir := range []string{p.root, p.socket, p.etcd} {
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
		}
		files, err := render(s, p, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			mode := os.FileMode(0600)
			if file.executable {
				mode = 0700
			}
			if err = os.WriteFile(filepath.Join(p.root, file.name), []byte(file.content), mode); err != nil {
				t.Fatal(err)
			}
		}
		// The renderer uses production S3 repositories. Native acceptance substitutes
		// only the archive command with a local pgBackRest repository for each group.
		if backrest != "" {
			config := filepath.Join(p.root, "pgbackrest.conf")
			configs[i] = config
			content := fmt.Sprintf("[global]\nrepo1-path=%s\nlog-level-console=warn\narchive-async=n\n[%s]\npg1-path=%s\npg1-socket-path=%s\npg1-user=%s\n", filepath.Join(root, "repository"), fmt.Sprintf("group%d", s.Citus.Group), p.pgdata, p.socket, s.Superuser.Username)
			if err = os.WriteFile(config, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(p.root, "patroni.yml"))
			if err != nil {
				t.Fatal(err)
			}
			var configMap map[string]any
			if err = yaml.Unmarshal(raw, &configMap); err != nil {
				t.Fatal(err)
			}
			command := shellQuote(backrest) + " --config=" + shellQuote(config) + " --stanza=group" + fmt.Sprint(s.Citus.Group) + " archive-push %p"
			configMap["postgresql"].(map[string]any)["parameters"].(map[string]any)["archive_command"] = command
			configMap["bootstrap"].(map[string]any)["dcs"].(map[string]any)["postgresql"].(map[string]any)["parameters"].(map[string]any)["archive_command"] = command
			raw, err = yaml.Marshal(configMap)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(p.root, "patroni.yml"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			s.Backup = &pgspec.Backup{Repository: pgspec.Repository{Stanza: fmt.Sprintf("group%d", s.Citus.Group)}}
		}
		specs[i] = s
		dirs[i] = p
	}
	exits := make(chan error, len(specs)+1)
	start := func(name, binary string, args ...string) {
		t.Helper()
		log, err := os.Create(filepath.Join(root, name+".log"))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(binary, args...)
		cmd.Stdout = log
		cmd.Stderr = log
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err = cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			err := cmd.Wait()
			done <- err
			exits <- fmt.Errorf("%s exited before acceptance completed: %v", name, err)
		}()
		t.Cleanup(func() {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				_ = cmd.Process.Kill()
				<-done
			}
			_ = log.Close()
			if t.Failed() {
				data, _ := os.ReadFile(log.Name())
				t.Logf("%s diagnostics:\n%s", name, data)
			}
		})
	}
	wait := func(name string, fn func() error) {
		t.Helper()
		var err error
		for ctx.Err() == nil {
			select {
			case exited := <-exits:
				t.Fatal(exited)
			default:
			}
			if err = fn(); err == nil {
				return
			}
			time.Sleep(300 * time.Millisecond)
		}
		t.Fatalf("%s: %v (deadline %v)", name, err, ctx.Err())
	}
	ep := etcdProcess(specs[0], dirs[0])
	start("etcd", etcd, ep.Args...)
	wait("DCS quorum", func() error { return etcdRequest(ctx, specs[0], "/v3/kv/range", map[string]any{"key": "a2s="}, nil) })
	for i := range specs {
		index := i
		t.Cleanup(func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer stopCancel()
			_ = exec.CommandContext(stopCtx, filepath.Join(bin, "pg_ctl"), "-D", dirs[index].pgdata, "-m", "immediate", "stop").Run()
		})
		start(specs[i].Member, patroni, filepath.Join(dirs[i].root, "patroni.yml"))
		if i == 0 {
			wait("coordinator primary", func() error {
				value, err := runCitusAction(ctx, specs[0], dirs[0], pgspec.ActionCitusStatus, &struct{}{})
				if err != nil {
					return err
				}
				for _, node := range value.(pgspec.CitusStatusResponse).Nodes {
					if node.GroupID == 0 && node.Active {
						return nil
					}
				}
				return fmt.Errorf("coordinator metadata registration is not ready")
			})
			c, err := localConnect(ctx, specs[0], dirs[0])
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Exec(ctx, `SELECT pg_catalog.citus_set_node_property('127.0.0.81',5432,'shouldhaveshards',true); SET citus.shard_count=8; CREATE TABLE public.native_items(id bigint PRIMARY KEY); SELECT pg_catalog.create_distributed_table('public.native_items','id'); INSERT INTO public.native_items SELECT generate_series(1,1000)`)
			c.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	var primary int
	wait("worker primary and replica", func() error {
		count, primaries := 0, 0
		primary = -1
		for i := 1; i < 3; i++ {
			c, err := localConnect(ctx, specs[i], dirs[i])
			if err != nil {
				return err
			}
			var recovery bool
			err = c.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&recovery)
			c.Close(ctx)
			if err != nil {
				return err
			}
			count++
			if !recovery {
				primaries++
				primary = i
			}
		}
		if count != 2 || primaries != 1 {
			return fmt.Errorf("worker group not ready")
		}
		return nil
	})
	wait("worker registration", func() error {
		out, err := runCitusAction(ctx, specs[0], dirs[0], pgspec.ActionCitusStatus, &struct{}{})
		if err != nil {
			return err
		}
		if len(out.(pgspec.CitusStatusResponse).Nodes) < 2 {
			return fmt.Errorf("worker not registered")
		}
		return nil
	})
	input := &pgspec.CitusRebalanceStartRequest{Receipt: "native-rebalance", Strategy: "by_shard_count"}
	value, err := runCitusAction(ctx, specs[0], dirs[0], pgspec.ActionCitusRebalanceStart, input)
	if err != nil {
		t.Fatal(err)
	}
	job := value.(pgspec.CitusRebalanceResponse)
	if job.JobID <= 0 {
		t.Fatalf("expected a native rebalance job, got %+v", job)
	}
	again, err := runCitusAction(ctx, specs[0], dirs[0], pgspec.ActionCitusRebalanceStart, input)
	if err != nil {
		t.Fatal(err)
	}
	if again.(pgspec.CitusRebalanceResponse).JobID != job.JobID {
		t.Fatal("receipt started a second rebalance")
	}
	wait("rebalance finished", func() error {
		value, err := runCitusAction(ctx, specs[0], dirs[0], pgspec.ActionCitusRebalanceStatus, &pgspec.CitusRebalanceRequest{JobID: job.JobID})
		if err != nil {
			return err
		}
		job = value.(pgspec.CitusRebalanceResponse)
		if job.State != "finished" {
			return fmt.Errorf("job state %s", job.State)
		}
		return nil
	})
	t.Logf("rebalance receipt replay returned job %d; state %s", job.JobID, job.State)
	value, err = runCitusAction(ctx, specs[0], dirs[0], pgspec.ActionCitusRestorePoint, &pgspec.CitusRestorePointRequest{Name: "native-consistent-point"})
	if err != nil {
		t.Fatal(err)
	}
	points := value.(pgspec.CitusRestorePointResponse)
	if len(points.Nodes) != 2 {
		t.Fatalf("restore point node positions: %+v", points)
	}
	t.Logf("restore point per-primary WAL upper bounds: %+v", points.Nodes)
	replayed, err := runCitusAction(ctx, specs[0], dirs[0], pgspec.ActionCitusRestorePoint, &pgspec.CitusRestorePointRequest{Name: "native-consistent-point"})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(replayed) != fmt.Sprint(points) {
		t.Fatalf("restore-point receipt changed on replay: %v != %v", replayed, points)
	}
	for _, i := range []int{0, primary} {
		if backrest != "" {
			cmd := exec.CommandContext(ctx, backrest, "--config="+configs[i], fmt.Sprintf("--stanza=group%d", specs[i].Citus.Group), "stanza-create")
			cmd.Env = append(os.Environ(), "PGPASSWORD="+specs[i].Superuser.Password)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("local pgBackRest stanza-create: %v: %s", err, out)
			}
		}
		value, err = runCitusAction(ctx, specs[i], dirs[i], pgspec.ActionWALSwitch, &struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		lsn := value.(pgspec.WALSwitchResponse).LSN
		if backrest != "" {
			c, err := localConnect(ctx, specs[i], dirs[i])
			if err != nil {
				t.Fatal(err)
			}
			nativeArchiveCommand := func(probeCtx context.Context, _ pgspec.Spec, _ paths, args ...string) ([]byte, error) {
				return exec.CommandContext(probeCtx, backrest, args...).Output()
			}
			wait("archive covers switched WAL", func() error {
				value, err := archiveCovers(ctx, c, specs[i], dirs[i], lsn, nativeArchiveCommand)
				if err != nil {
					return err
				}
				if !value.Covers {
					return fmt.Errorf("archive does not cover %s", lsn)
				}
				return nil
			})
			t.Logf("group %d switched WAL at %s; local pgBackRest archive covers it", specs[i].Citus.Group, lsn)
			var segment string
			if err := c.QueryRow(ctx, "SELECT pg_catalog.pg_walfile_name($1::pg_lsn)", lsn).Scan(&segment); err != nil {
				t.Fatal(err)
			}
			// Simulate PostgreSQL's receipt cleanup without deleting WAL needed
			// by the marker inspection below. Coverage must survive via a real
			// read-only fetch from this group's local repository.
			if err := os.Remove(filepath.Join(dirs[i].pgdata, "pg_wal", "archive_status", segment+".done")); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			covered, err := archiveCovers(ctx, c, specs[i], dirs[i], lsn, nativeArchiveCommand)
			c.Close(ctx)
			if err != nil || !covered.Covers {
				t.Fatalf("archive coverage lost after receipt cleanup: covers=%v err=%v", covered.Covers, err)
			}
			t.Logf("group %d repository covers exact segment %s after local receipt cleanup", specs[i].Citus.Group, segment)
		} else {
			t.Logf("group %d switched WAL at %s; archive coverage not exercised (set KARAKORAM_CITUS_PGBACKREST)", specs[i].Citus.Group, lsn)
		}
	}
	// Verify the coordinated point reached every physical member, including the
	// streamed standby, using pg_waldump before the temporary data is removed.
	for i := range specs {
		wait(fmt.Sprintf("member %d restore-point WAL", i), func() error {
			walDir := filepath.Join(dirs[i].pgdata, "pg_wal")
			entries, err := os.ReadDir(walDir)
			if err != nil {
				return err
			}
			// A base backup may omit initdb WAL, and a positional pg_waldump
			// argument scans one segment only. Inspect every retained segment.
			for _, entry := range entries {
				if len(entry.Name()) != 24 || strings.Trim(entry.Name(), "0123456789ABCDEF") != "" {
					continue
				}
				out, err := exec.CommandContext(ctx, filepath.Join(bin, "pg_waldump"), "-r", "XLOG", "-p", walDir, entry.Name()).CombinedOutput()
				if strings.Contains(string(out), "RESTORE_POINT native-consistent-point") {
					return nil
				}
				if ctx.Err() != nil {
					return err
				}
			}
			return fmt.Errorf("coordinated restore point is not present in retained WAL")
		})
		t.Logf("member %d WAL contains coordinated RESTORE_POINT", i)
	}
}

func nativeCitusTLS(t *testing.T) (string, *pgspec.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "native-citus-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	// Use the parsed CA so its generated SubjectKeyId becomes the leaf's
	// AuthorityKeyId, as required by Python/OpenSSL strict verification.
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "native-citus"}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.81"), net.ParseIP("127.0.0.82"), net.ParseIP("127.0.0.83")}}
	der, err = x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(caPEM), &pgspec.Certificate{CertPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), KeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: private}))}
}
