// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package relay

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// MaxHeaderSize bounds stream introductions and replies.
const MaxHeaderSize = 4096

// MaxControlSize bounds policy and hello frames on the reserved control stream.
const MaxControlSize = 1 << 20

// MaxPolicyTargets limits entries in a complete namespace allowlist.
const MaxPolicyTargets = 4096

// Dial requests an outbound TCP connection; a Reply precedes raw tunnel bytes.
type Dial struct {
	Type string `json:"type"`
	Addr string `json:"addr"`
}

// Agent introduces an inbound, opaque connection for the hub's internal listener.
type Agent struct {
	Type string `json:"type"`
}

// Policy replaces the complete target allowlist for a monotonically increasing generation.
type Policy struct {
	Type       string   `json:"type"`
	Targets    []string `json:"targets"`
	Generation uint64   `json:"generation"`
}

// Hello identifies the relay executable and namespace on the first relay-opened stream.
type Hello struct {
	Type             string `json:"type"`
	Namespace        string `json:"namespace"`
	Version          string `json:"version"`
	Commit           string `json:"commit"`
	ExecutableSHA256 string `json:"executableSHA256"`
	Hostname         string `json:"hostname"`
}

// Reply acknowledges a dial or policy. Error contains a non-secret refusal reason.
type Reply struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// WriteHeader writes one bounded JSON frame. It never writes a partial oversized frame.
func WriteHeader(w io.Writer, v any) error {
	return writeHeader(w, v, MaxHeaderSize)
}

// WriteControlHeader writes a validated policy or hello frame, bounded to one MiB.
// It must only be used on the reserved control stream; replies retain the 4 KiB limit.
func WriteControlHeader(w io.Writer, v any) error {
	if err := validateControl(v); err != nil {
		return err
	}
	return writeHeader(w, v, MaxControlSize)
}

func writeHeader(w io.Writer, v any, limit int) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) == 0 || len(b) > limit {
		return errors.New("relay header exceeds size limit")
	}
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(b)))
	if err = writeAll(w, size[:]); err != nil {
		return err
	}
	return writeAll(w, b)
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func readFrame(r io.Reader, limit uint32) ([]byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(size[:])
	if n == 0 || n > limit {
		return nil, errors.New("invalid relay header size")
	}
	b := make([]byte, int(n))
	_, err := io.ReadFull(r, b)
	return b, err
}
func strictJSON(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON in relay header")
	}
	return nil
}

// ReadHeader reads a typed Dial, Agent, Policy or Hello; unknown types and fields are rejected.
func ReadHeader(r io.Reader) (any, error) {
	return readHeader(r, MaxHeaderSize, false)
}

// ReadControlHeader reads only validated policy or hello frames from the reserved
// control stream, rejecting oversized frames before allocating their payload.
func ReadControlHeader(r io.Reader) (any, error) {
	return readHeader(r, MaxControlSize, true)
}

func readHeader(r io.Reader, limit uint32, controlOnly bool) (any, error) {
	b, err := readFrame(r, limit)
	if err != nil {
		return nil, err
	}
	var kind struct {
		Type string `json:"type"`
	}
	if err = json.Unmarshal(b, &kind); err != nil {
		return nil, err
	}
	var v any
	switch kind.Type {
	case "dial":
		v = &Dial{}
	case "agent":
		v = &Agent{}
	case "policy":
		v = &Policy{}
	case "hello":
		v = &Hello{}
	default:
		return nil, fmt.Errorf("unknown relay header type %q", kind.Type)
	}
	if err = strictJSON(b, v); err != nil {
		return nil, err
	}
	if controlOnly || kind.Type == "policy" || kind.Type == "hello" {
		if err := validateControl(v); err != nil {
			return nil, err
		}
	}
	return v, nil
}

// ReadReply reads a strict bounded reply frame.
func ReadReply(r io.Reader) (Reply, error) {
	var v Reply
	b, err := readFrame(r, MaxHeaderSize)
	if err != nil {
		return v, err
	}
	err = strictJSON(b, &v)
	if err == nil && ((v.OK && v.Error != "") || (!v.OK && v.Error == "")) {
		err = errors.New("invalid relay reply")
	}
	return v, err
}

func validateControl(v any) error {
	switch message := v.(type) {
	case Hello:
		if message.Type != "hello" {
			return errors.New("invalid hello type")
		}
		return nil
	case *Hello:
		if message == nil {
			return errors.New("nil hello")
		}
		return validateControl(*message)
	case Policy:
		if message.Type != "policy" {
			return errors.New("invalid policy type")
		}
		return validateTargets(message.Targets)
	case *Policy:
		if message == nil {
			return errors.New("nil policy")
		}
		if message.Type != "policy" {
			return errors.New("invalid policy type")
		}
		return validateTargets(message.Targets)
	default:
		return errors.New("control stream requires policy or hello")
	}
}

func validateTargets(targets []string) error {
	if len(targets) > MaxPolicyTargets {
		return errors.New("relay policy exceeds target limit")
	}
	for _, target := range targets {
		host, port, err := net.SplitHostPort(target)
		if err != nil || host == "" || strings.ContainsAny(host, " \t\r\n/\\") {
			return errors.New("invalid relay policy target")
		}
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 || strconv.Itoa(number) != port {
			return errors.New("invalid relay policy target port")
		}
	}
	return nil
}
