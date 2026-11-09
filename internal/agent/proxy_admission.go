// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// This is deliberately outside DataDir: /tmp belongs to the managed Dokploy
// container's writable layer and is never mounted from an engine data volume.
// Provenance also prevents reuse if that layer is copied or the container restarts.
type proxyContainerAdmission struct {
	path     string
	identity string
}

func proxyAdmissionPath(dir string) string {
	private := fmt.Sprintf("/tmp/.karakoram-proxy-admission-%d-%x", os.Geteuid(), sha256.Sum256([]byte(filepath.Clean(dir))))
	return filepath.Join(private, "admitted")
}

func proxyAdmissionForContainer(dir string) (proxyContainerAdmission, error) {
	init, _, err := processIdentity(1)
	if err != nil {
		return proxyContainerAdmission{}, err
	}
	namespace, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		return proxyContainerAdmission{}, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return proxyContainerAdmission{}, err
	}
	// PID1 survives an agent child restart/re-exec. All components are stable
	// within that container lifetime and change for a fresh task/PID namespace.
	identity := fmt.Sprintf("v1:%x\n", sha256.Sum256([]byte(init.Boot+"\x00"+init.Start+"\x00"+namespace+"\x00"+hostname)))
	return proxyContainerAdmission{path: proxyAdmissionPath(dir), identity: identity}, nil
}

func proxyAdmissionOwned(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func validProxyAdmissionDirectory(info os.FileInfo) bool {
	return info.Mode() == os.ModeDir|0700 && proxyAdmissionOwned(info)
}

func openProxyAdmissionDirectory(path string) (*os.File, error) {
	if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !validProxyAdmissionDirectory(before) {
		return nil, errors.New("proxy admission directory must be real, agent-owned and mode 0700")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	directory := os.NewFile(uintptr(fd), path)
	opened, err := directory.Stat()
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	if !validProxyAdmissionDirectory(opened) || !os.SameFile(before, opened) {
		_ = directory.Close()
		return nil, errors.New("proxy admission directory changed while opening")
	}
	return directory, nil
}

func (p proxyContainerAdmission) admitted() (bool, error) {
	data, err := p.readRecord(len(p.identity))
	return string(data) == p.identity, err
}

func (p proxyContainerAdmission) readRecord(maxBytes int) ([]byte, error) {
	directory, err := openProxyAdmissionDirectory(filepath.Dir(p.path))
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	// Open relative to the validated directory fd. Never follow a record symlink
	// or block opening a FIFO, and validate the opened inode rather than a path.
	fd, err := unix.Openat(int(directory.Fd()), filepath.Base(p.path), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "proxy admission record")
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || !proxyAdmissionOwned(info) || info.Mode().Perm()&0022 != 0 {
		return nil, errors.New("proxy admission record must be regular, agent-owned and not writable by other users")
	}
	if info.Size() > int64(maxBytes) {
		return nil, nil
	}
	return io.ReadAll(io.LimitReader(file, int64(maxBytes+1)))
}

func (p proxyContainerAdmission) record() error {
	directory, err := openProxyAdmissionDirectory(filepath.Dir(p.path))
	if err != nil {
		return err
	}
	defer directory.Close()
	dirfd := int(directory.Fd())
	name := "admission-" + rand.Text()
	fd, err := unix.Openat(dirfd, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	defer unix.Unlinkat(dirfd, name, 0)
	file := os.NewFile(uintptr(fd), name)
	if _, err = file.WriteString(p.identity); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	// Both publication paths are relative to the same validated directory inode.
	// No process credentials are stored here.
	return unix.Renameat(dirfd, name, dirfd, filepath.Base(p.path))
}
