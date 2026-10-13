// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package ui serves the Karakoram single-page web application.
//
// The compiled SPA (built by `npm run build` in ./ui, which writes to
// internal/ui/dist) is embedded into the binary when Go is invoked with the
// `ui` build tag. Without the tag the package still compiles and the handler
// serves a minimal "UI not built" page, so server builds and tests never
// depend on a Node toolchain.
package ui

import (
	"errors"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// Assets returns the embedded SPA files (index.html at the root) or nil when
// the binary was built without the `ui` tag.
func Assets() fs.FS { return assets() }

// Handler returns an http.Handler serving the SPA with history-API fallback:
// unknown paths without a file extension return index.html so client-side
// routes deep-link correctly. Hashed build assets under /assets/ are served as
// immutable; everything else (notably index.html) is revalidated on every
// request so deploys take effect immediately.
func Handler() http.Handler {
	files := Assets()
	if files == nil {
		return http.HandlerFunc(serveNotBuilt)
	}
	return &spaHandler{files: files, fileServer: http.FileServer(http.FS(files))}
}

type spaHandler struct {
	files      fs.FS
	fileServer http.Handler
}

func (h *spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	clean := path.Clean("/" + r.URL.Path)
	name := strings.TrimPrefix(clean, "/")
	if name == "" {
		name = "index.html"
	}

	if name != "index.html" {
		if st, err := fs.Stat(h.files, name); err == nil && !st.IsDir() {
			setCacheHeaders(w, clean)
			r.URL.Path = clean
			h.fileServer.ServeHTTP(w, r)
			return
		} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		// A missing path that looks like a file (has an extension) is a real
		// 404; routes never carry extensions.
		if path.Ext(name) != "" {
			http.NotFound(w, r)
			return
		}
	}

	index, err := fs.ReadFile(h.files, "index.html")
	if err != nil {
		http.Error(w, "index.html missing from UI bundle", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(index)
	}
}

func setCacheHeaders(w http.ResponseWriter, clean string) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if strings.HasPrefix(clean, "/assets/") {
		// Vite emits content-hashed filenames under assets/.
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
}

const notBuiltPage = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Karakoram</title>
<style>body{font:16px/1.5 system-ui,sans-serif;margin:0;min-height:100vh;display:grid;place-items:center;background:#0f1a2b;color:#e8eef7}
main{max-width:34rem;padding:2rem}code{background:#1c2a40;padding:.1em .4em;border-radius:4px}</style></head>
<body><main><h1>Karakoram</h1>
<p>The web UI was not included in this build. The REST API is available under <code>/api/v1</code>.</p>
<p>To include the UI, run <code>npm ci &amp;&amp; npm run build</code> in <code>ui/</code> and rebuild the server with <code>-tags ui</code>.</p>
</main></body></html>
`

func serveNotBuilt(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusServiceUnavailable)
	if r.Method == http.MethodGet {
		_, _ = w.Write([]byte(notBuiltPage))
	}
}
