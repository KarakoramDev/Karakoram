// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

func receiptPath(n *agent.Node) string {
	return filepath.Join(n.DataDir(), "karakoram/openbao/init-receipt.enc")
}
func receipt(n *agent.Node) (baospec.Envelope, error) {
	var env baospec.Envelope
	f, e := os.Open(receiptPath(n))
	if e != nil {
		return env, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 128<<10))
	if e != nil {
		return env, e
	}
	if e = json.Unmarshal(b, &env); e != nil {
		return env, errors.New("invalid initialization receipt")
	}
	if env.Scheme != baospec.ReceiptScheme || !baospec.ValidReceiptID(env.ReceiptID) {
		return env, errors.New("invalid initialization receipt")
	}
	return env, nil
}

type routeError struct {
	code    int
	message string
}

func (e routeError) Error() string { return e.message }
func invalidInit(v baospec.InitRequest) bool {
	if !baospec.ValidReceiptID(v.ReceiptID) || len(v.RecipientPublicKey) != 32 {
		return true
	}
	pub, e := ecdh.X25519().NewPublicKey(v.RecipientPublicKey)
	if e != nil {
		return true
	}
	priv, e := ecdh.X25519().GenerateKey(rand.Reader)
	if e != nil {
		return true
	}
	if _, e = priv.ECDH(pub); e != nil {
		return true
	}
	valid := func(n, t int) bool { return n >= 1 && n <= 255 && t >= 1 && t <= n }
	return !(valid(v.SecretShares, v.SecretThreshold) && v.RecoveryShares == 0 && v.RecoveryThreshold == 0 || valid(v.RecoveryShares, v.RecoveryThreshold) && v.SecretShares == 0 && v.SecretThreshold == 0)
}
func initialize(ctx context.Context, n *agent.Node, v baospec.InitRequest) (baospec.InitResponse, error) {
	var out baospec.InitResponse
	err := n.WithLock(ctx, func(n *agent.Node) error {
		env, e := receipt(n)
		if e == nil {
			out.Envelope = env
			return nil
		}
		if !os.IsNotExist(e) {
			return e
		}
		client, e := localClient(n.Config(), false)
		if e != nil {
			return e
		}
		state, e := client.SealStatus(ctx)
		if e != nil {
			return errors.New("openbao local API unavailable")
		}
		if state.Initialized {
			return routeError{409, baospec.ErrorInitializedWithoutReceipt}
		}
		if invalidInit(v) {
			return routeError{400, "invalid initialization request"}
		}
		s, e := spec(n.Config())
		if e != nil {
			return e
		}
		auto := s.Seal.Type != "" && s.Seal.Type != baospec.SealShamir
		if auto != (v.RecoveryShares > 0) {
			return routeError{400, "initialization shares do not match seal type"}
		}
		raw, e := client.Init(ctx, baoapi.InitRequest{SecretShares: v.SecretShares, SecretThreshold: v.SecretThreshold, RecoveryShares: v.RecoveryShares, RecoveryThreshold: v.RecoveryThreshold})
		if e != nil {
			return errors.New("openbao initialization failed")
		}
		defer clear(raw)
		env, e = baospec.SealReceipt(rand.Reader, v.RecipientPublicKey, n.Config().Cluster, n.Config().Member, v.ReceiptID, raw)
		if e != nil {
			return errors.New("initialization receipt encryption failed")
		}
		b, e := json.Marshal(env)
		if e != nil {
			return e
		}
		if e = n.WriteFile("openbao/init-receipt.enc", b, 0600); e != nil {
			return errors.New("initialization receipt persistence failed")
		}
		out.Envelope = env
		return nil
	})
	return out, err
}
func acknowledge(ctx context.Context, n *agent.Node, v baospec.InitAckRequest) error {
	return n.WithLock(ctx, func(n *agent.Node) error {
		env, e := receipt(n)
		if os.IsNotExist(e) {
			return nil
		}
		if e != nil {
			return e
		}
		if env.ReceiptID != v.ReceiptID {
			return routeError{409, "initialization receipt identity mismatch"}
		}
		root, e := os.OpenRoot(filepath.Join(n.DataDir(), "karakoram/openbao"))
		if e != nil {
			return e
		}
		defer root.Close()
		if e = root.Remove("init-receipt.enc"); e != nil {
			return e
		}
		dir, e := root.Open(".")
		if e != nil {
			return e
		}
		defer dir.Close()
		return dir.Sync()
	})
}
func reply(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		code := 500
		message := "openbao action failed"
		var re routeError
		if errors.As(err, &re) {
			code = re.code
			message = re.message
		}
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(agentapi.ErrorResponse{Error: message})
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		reply(w, nil, routeError{400, "invalid request"})
		return false
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		reply(w, nil, routeError{400, "invalid request"})
		return false
	}
	return true
}
func (*Runner) Routes(mux *http.ServeMux, n *agent.Node) {
	mux.HandleFunc("POST /v1/openbao/status", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		reply(w, status(ctx, n), nil)
	})
	mux.HandleFunc("POST /v1/openbao/init", func(w http.ResponseWriter, r *http.Request) {
		var v baospec.InitRequest
		if !decode(w, r, &v) {
			return
		}
		defer clear(v.RecipientPublicKey)
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		out, e := initialize(ctx, n, v)
		reply(w, out, e)
	})
	mux.HandleFunc("POST /v1/openbao/init-ack", func(w http.ResponseWriter, r *http.Request) {
		var v baospec.InitAckRequest
		if !decode(w, r, &v) {
			return
		}
		reply(w, agentapi.ActionResponse{Success: true}, acknowledge(r.Context(), n, v))
	})
}
