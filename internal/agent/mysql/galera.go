// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func galeraLocal(s myspec.Spec) myspec.Peer {
	for _, p := range s.Galera.Peers {
		if p.Name == s.Member {
			return p
		}
	}
	return myspec.Peer{}
}
func galeraAddress(host string, port int) string { return net.JoinHostPort(host, strconv.Itoa(port)) }
func RenderGaleraOptions(s myspec.Spec, root string) map[string]string {
	g := s.Galera
	local := galeraLocal(s)
	listenHost := g.ListenHost
	if listenHost == "" {
		listenHost = local.Host
		if net.ParseIP(listenHost) == nil {
			// Older configurations do not carry a bind policy. Never pass a
			// DNS identity to Galera's numeric outgoing source-address parser.
			listenHost = "0.0.0.0"
		}
	}
	seeds := make([]string, 0, len(g.Peers))
	for _, p := range g.Peers {
		seeds = append(seeds, galeraAddress(p.Host, p.Port))
	}
	sqlPort := myspec.PortMySQL
	for _, p := range s.Peers {
		if p.Name == s.Member {
			sqlPort = p.Port
		}
	}
	return map[string]string{
		"wsrep_on": "ON", "wsrep_provider": filepath.Join(s.ServerDir, "lib", "libgalera_smm.so"),
		"wsrep_cluster_name": g.ClusterName, "wsrep_cluster_address": "gcomm://" + strings.Join(seeds, ","),
		"wsrep_node_name": s.Member, "wsrep_node_address": galeraAddress(local.Host, g.GaleraPort),
		"wsrep_node_incoming_address": galeraAddress(local.Host, sqlPort),
		"wsrep_sst_method":            "xtrabackup-v2", "wsrep_sst_receive_address": galeraAddress(local.Host, g.SSTPort),
		"wsrep_provider_options": "gmcast.listen_addr=tcp://" + galeraAddress(listenHost, g.GaleraPort) + ";ist.recv_addr=" + galeraAddress(local.Host, g.ISTPort) + ";gcache.size=" + g.GCacheSize + ";gcache.recover=yes;socket.ssl=true;socket.ssl_ca=" + filepath.Join(root, "galera-ca.pem") + ";socket.ssl_cert=" + filepath.Join(root, "galera-cert.pem") + ";socket.ssl_key=" + filepath.Join(root, "galera-key.pem"),
		// Disable only upstream's auto-copy of SQL TLS paths: all replication,
		// IST and SST channels are explicitly encrypted with the shared leaf.
		"pxc_encrypt_cluster_traffic": "OFF", "pxc_strict_mode": "ENFORCING", "binlog_format": "ROW", "log_replica_updates": "ON",
	}
}

// The pinned wsrep_sst_xtrabackup-v2 donor retries a joiner's SST address up
// to galeraSSTDonorAttempts times, each bounded by [sst] donor-timeout and
// followed by a one-second sleep. Joiners listen before a donor is chosen, so
// a short private-network connect timeout bounds that loop for legitimate
// transfers too.
const (
	galeraSSTDonorAttempts = 30
	galeraSSTDonorTimeout  = 3 * time.Second
)

func renderGaleraSST(root string) string {
	return "[sst]\ntransferfmt=socat\nencrypt=4\ndonor-timeout=" + strconv.Itoa(int(galeraSSTDonorTimeout/time.Second)) + "\nssl-ca=" + optionQuote(filepath.Join(root, "galera-ca.pem")) + "\nssl-cert=" + optionQuote(filepath.Join(root, "galera-cert.pem")) + "\nssl-key=" + optionQuote(filepath.Join(root, "galera-key.pem")) + "\n[xtrabackup]\nparallel=1\n"
}
func galeraEnv(s myspec.Spec) []string {
	env := nativeEnv()
	major := "8.4"
	if strings.HasPrefix(s.Version, "8.0.") {
		major = "8.0"
	}
	path := filepath.Join(s.ServerDir, "bin", "pxc_extra", "pxb-"+major, "bin") + ":" + filepath.Join(s.ServerDir, "bin") + ":/usr/bin:/bin"
	for i, v := range env {
		if strings.HasPrefix(v, "PATH=") {
			env[i] = "PATH=" + path
			return env
		}
	}
	return append(env, "PATH="+path)
}
func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

// galeraTransferHoldoff outlasts that donor loop after an interrupted IST/SST.
// A joiner relaunched inside it can consume the stale stream of the aborted
// request and then wait forever for an IST that no donor will send.
var galeraTransferHoldoff = galeraSSTDonorAttempts*(galeraSSTDonorTimeout+time.Second) + 10*time.Second

// galeraUnsyncedMarker is created by every ordinary supervised launch and
// removed once that launch is observed Synced in its pinned history. If it
// survives into the next launch, the previous one never finished joining.
const galeraUnsyncedMarker = "galera-launch-unsynced"

// The supervised executable consumes its bootstrap claim before exec, including
// after an agent crash. Supervisor retries always take the ordinary join path.
// File/directory fsync makes claims durable without requiring syncfs permission.
func galeraLauncher(s myspec.Spec, p paths) string {
	base := shellQuote(filepath.Join(s.ServerDir, "bin", "mysqld")) + " " + shellQuote("--defaults-file="+p.config)
	var b strings.Builder
	b.WriteString("#!/bin/sh\nset -eu\n")
	// A shell waiting for recovery can otherwise resume and exec a new server
	// after TERM. Wait in the background so the shell handles the signal
	// immediately; reap its recovery child before exiting, never launching.
	b.WriteString("recover_pid=''\ntrap 'trap \"\" TERM INT; if [ -n \"$recover_pid\" ]; then kill -KILL \"$recover_pid\" 2>/dev/null || :; wait \"$recover_pid\" 2>/dev/null || :; fi; exit 143' TERM INT\n")
	fmt.Fprintf(&b, "state=%s\nrecovery=%s\n", shellQuote(filepath.Join(p.root, "galera-state-uuid")), shellQuote(filepath.Join(p.root, "galera-supervisor-recover.log")))
	// Node.BeforeStart also runs on live idempotent starts and before restart
	// stops the old process. Only this actual supervised launch may recover redo.
	b.WriteString("recover_position() {\n read -r expected < \"$state\"\n : > \"$recovery\"\n")
	b.WriteString(" " + base + " --wsrep-recover --skip-networking --init-file=/dev/null \"--log-error=$recovery\" &\n recover_pid=$!\n wait \"$recover_pid\"\n recover_pid=''\n position=''\n")
	b.WriteString(" while IFS= read -r line; do\n  case \"$line\" in\n   *'WSREP: Recovered position: '*|*'[WSREP] Recovered position: '*)\n    [ -z \"$position\" ] || exit 1\n    position=${line##*Recovered position: }\n    ;;\n  esac\n done < \"$recovery\"\n")
	b.WriteString(" case \"$position\" in\n  \"$expected\":*) seqno=${position#*:} ;;\n  *) exit 1 ;;\n esac\n case \"$seqno\" in ''|*[!0-9]*) exit 1 ;; esac\n}\n")
	if s.Galera.Bootstrap {
		receipt := filepath.Join(p.root, fmt.Sprintf("galera-bootstrap-%d", s.Galera.FormationEpoch))
		fmt.Fprintf(&b, "if [ ! -d %s ]; then\n", shellQuote(receipt))
		if s.Galera.StartPosition != "" {
			fmt.Fprintf(&b, " recover_position\n [ \"$position\" = %s ] || exit 1\n", shellQuote(s.Galera.StartPosition))
		}
		fmt.Fprintf(&b, " mkdir -- %s\n sync -- %s\n sync -- %s\n exec %s --wsrep-new-cluster", shellQuote(receipt), shellQuote(receipt), shellQuote(p.root), base)
		if s.Galera.StartPosition != "" {
			b.WriteString(" " + shellQuote("--wsrep-start-position="+s.Galera.StartPosition))
		}
		b.WriteString("\nfi\n")
	}
	// A surviving marker means the previous launch never reached Synced: its
	// state transfer was interrupted. Without it, upstream's zero-UUID state is
	// an inconsistency eviction of a finished member, which supervised restarts
	// must never turn into an automatic rejoin. Check before re-marking.
	unsynced := shellQuote(filepath.Join(p.root, galeraUnsyncedMarker))
	fmt.Fprintf(&b, "if [ ! -e %s ] && [ -f %s ]; then\n while IFS= read -r line || [ -n \"$line\" ]; do\n  case \"$line\" in uuid:*%s*) echo 'galera state reset outside an unfinished join; explicit recovery required' >&2; exit 1 ;; esac\n done < %s\nfi\n", unsynced, shellQuote(filepath.Join(p.data, "grastate.dat")), galeraTransferRequiredUUID, shellQuote(filepath.Join(p.data, "grastate.dat")))
	fmt.Fprintf(&b, "if [ -e %s ]; then\n sleep %d &\n recover_pid=$!\n wait \"$recover_pid\"\n recover_pid=''\nfi\n", unsynced, int(galeraTransferHoldoff/time.Second))
	fmt.Fprintf(&b, ": > %s\nsync -- %s\nsync -- %s\n", unsynced, unsynced, shellQuote(p.root))
	b.WriteString("if [ -s \"$state\" ]; then\n recover_position\n")
	fmt.Fprintf(&b, " mkdir -p -- %s\n sync -- %s\n sync -- %s\n exec %s \"--wsrep-start-position=$position\"\nfi\n", shellQuote(filepath.Join(p.root, "galera-sst-start")), shellQuote(filepath.Join(p.root, "galera-sst-start")), shellQuote(p.root), base)
	b.WriteString("exec " + base + "\n")
	return b.String()
}
func configureMySQL(n *agent.Node, s myspec.Spec, p paths) error {
	spec := agent.ProcessSpec{Binary: filepath.Join(s.ServerDir, "bin", "mysqld"), Args: []string{"--defaults-file=" + p.config}, Dir: p.root, Env: nativeEnv()}
	if s.Galera != nil {
		if err := n.WriteFile("mysql/galera-start", []byte(galeraLauncher(s, p)), 0700); err != nil {
			return err
		}
		spec.Binary = filepath.Join(p.root, "galera-start")
		spec.Args = nil
		spec.Env = galeraEnv(s)
	}
	return n.Configure("mysqld", spec)
}
func galeraStatusDB(ctx context.Context, db *sql.DB) (myspec.GaleraStatusResponse, error) {
	var out myspec.GaleraStatusResponse
	rows, err := db.QueryContext(ctx, "SHOW GLOBAL STATUS WHERE Variable_name IN ('wsrep_cluster_status','wsrep_local_state_comment','wsrep_ready','wsrep_connected','wsrep_cluster_size','wsrep_cluster_state_uuid','wsrep_last_committed')")
	if err != nil {
		return out, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var k, v string
		if err = rows.Scan(&k, &v); err != nil {
			return out, err
		}
		count++
		switch strings.ToLower(k) {
		case "wsrep_cluster_status":
			out.ClusterStatus = v
		case "wsrep_local_state_comment":
			out.LocalStateComment = v
		case "wsrep_ready":
			out.Ready = v == "ON"
		case "wsrep_connected":
			out.Connected = v == "ON"
		case "wsrep_cluster_size":
			out.ClusterSize, err = strconv.Atoi(v)
		case "wsrep_cluster_state_uuid":
			out.StateUUID = v
		case "wsrep_last_committed":
			out.LastCommitted, err = strconv.ParseInt(v, 10, 64)
		}
		if err != nil {
			return out, err
		}
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if count != 7 {
		return out, errors.New("galera status incomplete")
	}
	if err = db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&out.GTIDExecuted); err != nil {
		return out, err
	}
	return out, nil
}
func (e *Runner) galeraStatus(ctx context.Context) (myspec.GaleraStatusResponse, error) {
	s, _ := e.state()
	if s.Galera == nil {
		return myspec.GaleraStatusResponse{}, errors.New("galera member required")
	}
	db, err := e.LocalAccessDB(ctx)
	if err != nil {
		return myspec.GaleraStatusResponse{}, err
	}
	status, err := galeraStatusDB(ctx, db)
	if err == nil && status.Eligible() {
		_, p := e.state()
		err = refreshGaleraIdentity(ctx, db, s, p, status)
	}
	return status, err
}
func galeraSSTGuard(s myspec.Spec, p paths, id myspec.DataIdentity) (bool, error) {
	// A durable CP-authorized SST intent admits copied data after an interrupted
	// join. It never admits a foreign datadir merely because Galera is enabled.
	var receipt galeraIdentity
	raw, err := os.ReadFile(filepath.Join(p.root, "sst-authorized.json"))
	if err != nil {
		return false, err
	}
	if err = json.Unmarshal(raw, &receipt); err != nil {
		return false, err
	}
	if receipt.ClusterName != s.Galera.ClusterName || receipt.Member != s.Member || receipt.FormationEpoch > s.Galera.FormationEpoch || !receipt.Pending || id.Initialized {
		return false, errors.New("SST identity authorization mismatch")
	}
	return true, nil
}

// PXC loads replication/IST TLS at process startup. Publishing different
// credentials while live would mix active socket trust with new SST trust.
// The control plane must first stop every data member, then publish and recover.
func guardGaleraTrafficRotation(s myspec.Spec, p paths, running bool) error {
	if !running {
		return nil
	}
	for name, want := range map[string]string{
		"galera-ca.pem":   s.Galera.TrafficCAPEM,
		"galera-cert.pem": s.Galera.TrafficCert.CertPEM,
		"galera-key.pem":  s.Galera.TrafficCert.KeyPEM,
	} {
		raw, err := os.ReadFile(filepath.Join(p.root, name))
		if err != nil || string(raw) != want {
			return errors.New("galera traffic TLS rotation requires all data members stopped")
		}
	}
	return nil
}
