// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sys/unix"
)

func localPeerIdentity() (string, string, error) {
	// Children inherit the effective kernel UID, never an environment username.
	owner, err := user.LookupId(strconv.Itoa(os.Geteuid()))
	if err != nil || owner.Username == "" || strings.ContainsAny(owner.Username, "\r\n\x00") || strings.HasPrefix(owner.Username, "/") {
		return "", "", errors.New("cannot resolve runtime owner for PostgreSQL peer authentication")
	}
	identity := `"` + strings.ReplaceAll(owner.Username, `"`, `""`) + `"`
	return owner.Username, "karakoram-local " + identity + " postgres\n", nil
}

type localPeerObservation struct {
	IdentFile string    `json:"identFile"`
	Owner     string    `json:"owner"`
	LoadedAt  time.Time `json:"loadedAt,omitempty"`
}

type peerAuthSession interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// The view parses the current file, while the reload epoch proves PostgreSQL
// processed a reload after publication. Never infer activation from disk alone.
const localPeerMappingObservedSQL = `SELECT current_setting('ident_file')=$1 AND pg_conf_load_time()>$2
 AND EXISTS (SELECT FROM pg_catalog.pg_ident_file_mappings WHERE map_name='karakoram-local' AND sys_name=$3 AND pg_username='postgres' AND error IS NULL)
 AND NOT EXISTS (SELECT FROM pg_catalog.pg_ident_file_mappings WHERE error IS NOT NULL OR (map_name='karakoram-local' AND (sys_name<>$3 OR pg_username<>'postgres')))`

func stageLocalPeerMap(name, mapping string) error {
	if !filepath.IsAbs(name) {
		return errors.New("PostgreSQL active identity file is not absolute")
	}
	d, err := openDurableDirectory(filepath.Dir(name))
	if err != nil {
		return fmt.Errorf("cannot open PostgreSQL identity directory: %w", err)
	}
	defer d.Close()
	previous, err := readLocalPeerMapAt(d, filepath.Base(name))
	if err != nil {
		return fmt.Errorf("cannot read PostgreSQL active identity file: %w", err)
	}
	var b strings.Builder
	// Preserve native mappings owned by other callers. Only this reserved map is
	// replaced, avoiding duplicate grants when activation is interrupted/replayed.
	for line := range strings.Lines(string(previous)) {
		first := ""
		for field := range strings.FieldsSeq(line) {
			first = field
			break
		}
		if first == "karakoram-local" || first == `"karakoram-local"` {
			continue
		}
		b.WriteString(line)
	}
	if content := b.String(); len(content) > 0 && content[len(content)-1] != '\n' {
		b.WriteByte('\n')
	}
	b.WriteString(mapping)
	if string(previous) == b.String() {
		return nil
	}
	if err := durableFileAt(d, filepath.Base(name), []byte(b.String())); err != nil {
		return fmt.Errorf("cannot stage PostgreSQL active identity map: %w", err)
	}
	return nil
}

func readLocalPeerMapAt(d *os.File, name string) ([]byte, error) {
	// O_NONBLOCK also prevents an untrusted FIFO from blocking before fstat.
	fd, err := unix.Openat(int(d.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || !postgresFileOwned(info) || info.Mode().Perm()&0022 != 0 {
		return nil, errors.New("identity map must be regular, agent-owned and not writable by other users")
	}
	return io.ReadAll(f)
}

func activateLocalPeerMap(ctx context.Context, c peerAuthSession, p *paths, owner, mapping string) error {
	var active string
	var before time.Time
	if err := c.QueryRow(ctx, "SELECT current_setting('ident_file'), pg_conf_load_time()").Scan(&active, &before); err != nil {
		return errors.New("cannot observe PostgreSQL active identity configuration")
	}
	if err := stageLocalPeerMap(active, mapping); err != nil {
		return err
	}
	target := filepath.Join(p.root, "pg_ident.conf")
	if err := stageLocalPeerMap(target, mapping); err != nil {
		return err
	}
	var signaled bool
	if err := c.QueryRow(ctx, "SELECT pg_catalog.pg_reload_conf()").Scan(&signaled); err != nil || !signaled {
		return errors.New("cannot reload PostgreSQL identity map before peer activation")
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var observed bool
		if err := c.QueryRow(ctx, localPeerMappingObservedSQL, active, before, owner).Scan(&observed); err != nil {
			return errors.New("cannot observe PostgreSQL identity map activation")
		}
		if observed {
			if err := durableJSON(filepath.Join(p.root, "peer-auth.json"), localPeerObservation{IdentFile: active, Owner: owner, LoadedAt: time.Now()}); err != nil {
				return err
			}
			p.ident = target
			p.identStaged = true
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("PostgreSQL identity map activation interrupted; peer HBA remains unpublished")
		case <-deadline.C:
			return errors.New("waiting for PostgreSQL identity map activation before publishing peer HBA")
		case <-ticker.C:
		}
	}
}

// Called before rendering either the single-server HBA or Patroni's pg_hba.
// A live server must first load its CURRENT identity file: ident_file's path is
// postmaster-only, but HBA rules take effect immediately on SIGHUP.
func prepareLocalPeerAuthentication(ctx context.Context, s pgspec.Spec, p *paths) error {
	if !s.LocalPeerAuth || s.Role != pgspec.RoleData {
		return nil
	}
	owner, mapping, err := localPeerIdentity()
	if err != nil {
		return err
	}
	// Future starts must never depend on PGDATA's backed-up identity file:
	// Patroni can materialize it without another agent Apply in between.
	p.ident = filepath.Join(p.root, "pg_ident.conf")
	if err := stageLocalPeerMap(p.ident, mapping); err != nil {
		return err
	}
	p.identStaged = true
	if peer, err := localPeerConnect(ctx, s, *p); err == nil {
		defer peer.Close(ctx)
		var active string
		if err := peer.QueryRow(ctx, "SELECT current_setting('ident_file')").Scan(&active); err != nil {
			return errors.New("cannot observe PostgreSQL active identity configuration")
		}
		// A successful passwordless connection is the stronger live observation.
		// Reuse a durable observation, otherwise establish its identity-file owner.
		data, readErr := os.ReadFile(filepath.Join(p.root, "peer-auth.json"))
		var prior localPeerObservation
		if readErr == nil && json.Unmarshal(data, &prior) == nil && prior.Owner == owner && prior.IdentFile == active {
			return nil
		}
		return activateLocalPeerMap(ctx, peer, p, owner, mapping)
	}
	legacy := s
	legacy.LocalPeerAuth = false
	if c, err := localConnect(ctx, legacy, *p); err == nil {
		defer c.Close(ctx)
		return activateLocalPeerMap(ctx, c, p, owner, mapping)
	}
	// A stale PID file must not block cold startup, but an unobservable live
	// server is not permission to publish new authentication rules.
	data, pidErr := os.ReadFile(filepath.Join(p.pgdata, "postmaster.pid"))
	if pidErr == nil {
		fields := strings.Fields(string(data))
		if len(fields) == 0 {
			return errors.New("cannot establish PostgreSQL stopped state before peer activation")
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid <= 0 {
			return errors.New("cannot establish PostgreSQL stopped state before peer activation")
		}
		if err := syscall.Kill(pid, 0); err == nil || !errors.Is(err, syscall.ESRCH) {
			recovered, repairErr := repairLocalPeerMapWith(ctx, *p, owner, mapping, func(ctx context.Context) error {
				_, err := command(ctx, filepath.Join(s.BinDir, "pg_ctl"), "reload", "-D", p.pgdata)
				return err
			}, func(ctx context.Context) (*pgx.Conn, error) {
				return localPeerConnect(ctx, s, *p)
			})
			if repairErr != nil {
				return repairErr
			}
			defer recovered.Close(ctx)
			return activateLocalPeerMap(ctx, recovered, p, owner, mapping)
		}
	} else if !os.IsNotExist(pidErr) {
		return errors.New("cannot inspect PostgreSQL stopped state before peer activation")
	}
	if _, err := os.Stat(filepath.Join(p.pgdata, "PG_VERSION")); err == nil {
		// Keep the existing configured identity path, including when a running
		// Patroni parent restarts PostgreSQL with its previous configuration.
		output, err := command(ctx, filepath.Join(s.BinDir, "postgres"), "-D", p.pgdata, "-c", "config_file="+filepath.Join(p.root, "postgresql.conf"), "-C", "ident_file")
		if err != nil {
			return errors.New("cannot inspect stopped PostgreSQL identity configuration")
		}
		active := strings.TrimSpace(string(output))
		if err := stageLocalPeerMap(active, mapping); err != nil {
			return err
		}
		p.identStaged = true
	} else if !os.IsNotExist(err) {
		return errors.New("cannot inspect PostgreSQL initialization before peer activation")
	}
	// A fresh/stopped server consumes the fully staged matching ident_file/HBA
	// together at startup; no existing backend can observe half the transition.
	return nil
}

// Repair already-published peer authentication without first needing SQL.
// Stage both the last observed live file and the stable future-start file:
// a restart may have switched between them since the durable observation.
func repairLocalPeerMapWith(ctx context.Context, p paths, owner, mapping string, reload func(context.Context) error, connect func(context.Context) (*pgx.Conn, error)) (*pgx.Conn, error) {
	raw, err := os.ReadFile(filepath.Join(p.root, "peer-auth.json"))
	var prior localPeerObservation
	if err != nil || json.Unmarshal(raw, &prior) != nil || prior.Owner != owner || !filepath.IsAbs(prior.IdentFile) {
		return nil, errors.New("running PostgreSQL requires an observed identity file before SQL-independent peer repair")
	}
	if err := stageLocalPeerMap(filepath.Join(p.root, "pg_ident.conf"), mapping); err != nil {
		return nil, err
	}
	// A vanished directory may belong to the pre-restart configuration. Other
	// write failures remain errors; only a passwordless probe proves recovery.
	if prior.IdentFile != filepath.Join(p.root, "pg_ident.conf") {
		if err := stageLocalPeerMap(prior.IdentFile, mapping); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	if ctx.Err() != nil {
		return nil, errors.New("PostgreSQL peer authentication repair interrupted before reload")
	}
	if err := reload(ctx); err != nil {
		return nil, errors.New("cannot signal PostgreSQL identity-map repair")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if c, err := connect(waitCtx); err == nil {
			return c, nil
		}
		select {
		case <-waitCtx.Done():
			return nil, errors.New("waiting for PostgreSQL password-independent authentication after identity-map repair")
		case <-ticker.C:
		}
	}
}
