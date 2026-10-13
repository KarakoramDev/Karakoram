// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package relay

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/xtaci/smux"
)

const MaxWebSocketMessageSize = 1 << 20
const MaxActiveStreams = 256

var ErrStreamCapacity = errors.New("relay active stream capacity reached")

type streamLimit struct{ active atomic.Int32 }

func (l *streamLimit) acquire() bool {
	for {
		n := l.active.Load()
		if n >= MaxActiveStreams {
			return false
		}
		if l.active.CompareAndSwap(n, n+1) {
			return true
		}
	}
}
func (l *streamLimit) release() { l.active.Add(-1) }

func websocketConn(ctx context.Context, ws *websocket.Conn) net.Conn {
	conn := websocket.NetConn(ctx, ws, websocket.MessageBinary)
	// NetConn disables the library default limit; restore a TLS-record-sized bound.
	ws.SetReadLimit(MaxWebSocketMessageSize)
	return conn
}

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
	remote  net.Addr
	release func()
	once    sync.Once
}

func (c *streamConn) RemoteAddr() net.Addr { return c.remote }

func (c *streamConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		if c.release != nil {
			c.release()
		}
	})
	return err
}

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
