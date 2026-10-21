// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type paths struct{ root, data, run, socket, config string }

func dataPaths(dir string) paths {
	root := filepath.Join(dir, "karakoram", "mysql")
	run := filepath.Join(root, "run")
	return paths{root: root, data: filepath.Join(dir, "mysql"), run: run, socket: filepath.Join(run, "mysql.sock"), config: filepath.Join(root, "my.cnf")}
}
func privateDir(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	i, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !i.IsDir() || i.Mode()&os.ModeSymlink != 0 {
		return errors.New("MySQL directory must be a real directory")
	}
	return os.Chmod(dir, 0700)
}
func durableJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".receipt-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func readIdentity(p paths) (myspec.DataIdentity, error) {
	var id myspec.DataIdentity
	b, err := os.ReadFile(filepath.Join(p.root, "initialized.json"))
	if os.IsNotExist(err) {
		return id, nil
	}
	if err != nil {
		return id, err
	}
	err = json.Unmarshal(b, &id)
	return id, err
}
func guardInitialization(s myspec.Spec, p paths) (bool, error) {
	id, err := readIdentity(p)
	if err != nil {
		return false, err
	}
	info, err := os.Lstat(filepath.Join(p.data, "mysql"))
	if err == nil {
		if s.Galera != nil && !id.Initialized && info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			_, err := galeraSSTGuard(s, p, id)
			return false, err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !id.Initialized {
			return false, errors.New("unrecognized MySQL data identity; explicit recovery required")
		}
		pending := false
		if s.Galera != nil {
			r, err := guardGaleraIdentity(s, p)
			if err != nil {
				return false, err
			}
			pending = r.Pending
		}
		if id.ServerUUID != "" {
			auto := filepath.Join(p.data, "auto.cnf")
			info, err := os.Lstat(auto)
			if os.IsNotExist(err) && pending {
				return false, nil // upstream SST removes auto.cnf before mysqld regenerates it
			}
			if err != nil || !info.Mode().IsRegular() || info.Size() > 4096 {
				return false, errors.New("MySQL server identity is missing or invalid")
			}
			raw, err := os.ReadFile(auto)
			if err != nil {
				return false, err
			}
			found := ""
			for _, line := range strings.Split(string(raw), "\n") {
				key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
				if ok && strings.TrimSpace(key) == "server-uuid" {
					found = strings.TrimSpace(value)
				}
			}
			if found != id.ServerUUID && !pending {
				return false, errors.New("MySQL server identity does not match its durable receipt")
			}
		}
		return false, nil
	}
	if !os.IsNotExist(err) {
		return false, err
	}
	entries, err := os.ReadDir(p.data)
	if err != nil {
		return false, err
	}
	if !id.Initialized && s.Galera != nil && !s.Galera.Bootstrap {
		if r, err := readGaleraIdentity(s, p); err == nil && r.Pending {
			return false, nil // includes partial data from an interrupted authorized SST
		}
	}
	if len(entries) != 0 || id.Initialized || !s.AllowInitialize {
		return false, errors.New("MySQL data is missing or foreign; automatic initialization refused")
	}
	if s.Galera != nil && !s.Galera.Bootstrap {
		// Only an empty, explicitly authorized new member may receive SST.
		// Never run --initialize or account SQL on a joining member.
		return false, durableJSON(filepath.Join(p.root, "sst-authorized.json"), galeraIdentity{
			ClusterName: s.Galera.ClusterName, Member: s.Member,
			FormationEpoch: s.Galera.FormationEpoch, Pending: true,
		})
	}
	// Consume authorization before running mysqld. Interrupted initialization is never retried.
	f, err := os.OpenFile(filepath.Join(p.root, "initialize-attempt.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return false, errors.New("MySQL initialization already attempted; explicit recovery required")
	}
	_, err = f.WriteString("true\n")
	if err == nil {
		err = f.Sync()
	}
	_ = f.Close()
	if err != nil {
		return false, err
	}
	d, err := os.Open(p.root)
	if err != nil {
		return false, err
	}
	defer d.Close()
	if err = d.Sync(); err != nil {
		return false, err
	}
	return true, nil
}
func refusePersistedVariables(p paths) error {
	_, err := os.Lstat(filepath.Join(p.data, "mysqld-auto.cnf"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	// Persisted variables travel with clone/restore and must never override agent policy.
	return os.Remove(filepath.Join(p.data, "mysqld-auto.cnf"))
}
