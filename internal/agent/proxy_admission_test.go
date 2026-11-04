// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestProxyAdmissionRejectsUnsafeRecord(t *testing.T) {
	for _, condition := range []string{"symlink-directory", "wrong-directory-mode", "symlink-record", "fifo-record", "oversize-record", "writable-record"} {
		t.Run(condition, func(t *testing.T) {
			root := t.TempDir()
			directory := filepath.Join(root, "private")
			if err := os.Mkdir(directory, 0700); err != nil {
				t.Fatal(err)
			}
			admission := proxyContainerAdmission{path: filepath.Join(directory, "admitted"), identity: "test-container\n"}
			target := filepath.Join(root, "target")
			if err := os.WriteFile(target, []byte(admission.identity), 0600); err != nil {
				t.Fatal(err)
			}
			switch condition {
			case "symlink-directory":
				linked := filepath.Join(root, "linked")
				if err := os.Symlink(directory, linked); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(admission.path, []byte(admission.identity), 0600); err != nil {
					t.Fatal(err)
				}
				admission.path = filepath.Join(linked, "admitted")
			case "wrong-directory-mode":
				if err := os.Chmod(directory, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(admission.path, []byte(admission.identity), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink-record":
				if err := os.Symlink(target, admission.path); err != nil {
					t.Fatal(err)
				}
			case "fifo-record":
				if err := unix.Mkfifo(admission.path, 0600); err != nil {
					t.Fatal(err)
				}
			case "oversize-record":
				if err := os.WriteFile(admission.path, []byte(admission.identity+"extra"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(admission.path, 1<<30); err != nil {
					t.Fatal(err)
				}
			case "writable-record":
				if err := os.WriteFile(admission.path, []byte(admission.identity), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(admission.path, 0666); err != nil {
					t.Fatal(err)
				}
			}
			result := make(chan bool, 1)
			go func() { admitted, _ := admission.admitted(); result <- admitted }()
			select {
			case admitted := <-result:
				if admitted {
					t.Fatal("unsafe filesystem object forged container admission")
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatal("unsafe filesystem object blocked admission startup")
			}
		})
	}
}

type proxyAdmissionForeignInfo struct{ os.FileInfo }

func (f proxyAdmissionForeignInfo) Sys() any {
	stat := *f.FileInfo.Sys().(*syscall.Stat_t)
	stat.Uid = uint32(os.Geteuid()) ^ 1
	return &stat
}

func TestProxyAdmissionRejectsForeignOwnership(t *testing.T) {
	// Exercise the exact ownership validation used after Lstat and fstat
	// without requiring root privileges to create another UID's filesystem node.
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if !validProxyAdmissionDirectory(info) {
		t.Fatal("owned mode-0700 directory rejected")
	}
	foreign := proxyAdmissionForeignInfo{info}
	if validProxyAdmissionDirectory(foreign) || proxyAdmissionOwned(foreign) {
		t.Fatal("foreign UID can forge admission directory/record ownership")
	}
}

func TestProxyAdmissionCreatesPrivateAtomicRecord(t *testing.T) {
	admission := proxyContainerAdmission{path: filepath.Join(t.TempDir(), "private", "admitted"), identity: "test-container\n"}
	if err := admission.record(); err != nil {
		t.Fatal(err)
	}
	admitted, err := admission.admitted()
	if err != nil || !admitted {
		t.Fatalf("private record rejected: %v %v", admitted, err)
	}
	directory, err := os.Lstat(filepath.Dir(admission.path))
	if err != nil || !validProxyAdmissionDirectory(directory) {
		t.Fatal("admission directory is not owned mode 0700")
	}
	record, err := os.Lstat(admission.path)
	if err != nil || !record.Mode().IsRegular() || record.Mode().Perm() != 0600 || !proxyAdmissionOwned(record) {
		t.Fatal("admission record is not private and owned")
	}
	entries, err := os.ReadDir(filepath.Dir(admission.path))
	if err != nil || len(entries) != 1 {
		t.Fatal("atomic record publication retained temporary files")
	}
}
