// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package relay

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/xtaci/smux"
)

func muxConfig(c *smux.Config) *smux.Config {
	if c == nil {
		c = smux.DefaultConfig()
		c.KeepAliveInterval = 15 * time.Second
		c.KeepAliveTimeout = 45 * time.Second
	}
	copy := *c
	copy.Version = 2
	return &copy
}
func pingLoop(ctx context.Context, c *websocket.Conn, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pingCtx, cancel := context.WithTimeout(ctx, min(5*time.Second, interval))
			err := c.Ping(pingCtx)
			cancel()
			if err != nil {
				c.CloseNow()
				return
			}
		}
	}
}

// streamConn reports the logical target and deliberately exposes only net.Conn semantics.
type streamConn struct {
	net.Conn
	remote net.Addr
}

func (c *streamConn) RemoteAddr() net.Addr { return c.remote }

type relayAddr string

func (a relayAddr) Network() string { return "tcp" }
func (a relayAddr) String() string  { return string(a) }

func pipe(ctx context.Context, a, b net.Conn) {
	stop := context.AfterFunc(ctx, func() { a.Close(); b.Close() })
	defer stop()
	defer a.Close()
	defer b.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	copySide := func(dst, src net.Conn) {
		defer wg.Done()
		_, _ = io.Copy(dst, src)
		a.Close()
		b.Close()
	}
	go copySide(a, b)
	go copySide(b, a)
	wg.Wait()
}

// boundedStreamOperation handles cancellation even when ctx has no deadline.
func boundedStreamOperation(ctx context.Context, s *smux.Stream, timeout time.Duration, fn func() error) error {
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	s.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { s.Close() })
	err := fn()
	if !stop() {
		s.Close()
		return ctx.Err()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil {
		s.SetDeadline(time.Time{})
	}
	return err
}
