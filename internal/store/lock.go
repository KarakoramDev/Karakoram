// SPDX-License-Identifier: Apache-2.0
package store

import (
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"os"
	"syscall"
)

// Open holds a shared lifetime lock so offline restore cannot replace live state.
func Open(path string, sealer *secrets.Sealer) (*Store, error) {
	if path == ":memory:" {
		return open(path, sealer)
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, err
	}
	st, err := open(path, sealer)
	if err != nil {
		lock.Close()
		return nil, err
	}
	st.lock = lock
	return st, nil
}
