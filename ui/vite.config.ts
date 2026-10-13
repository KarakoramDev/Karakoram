// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { fileURLToPath, URL } from 'node:url';

const apiTarget = process.env.KARAKORAM_API_URL ?? 'http://localhost:8080';

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: apiTarget,
        changeOrigin: false,
        // SSE streams must not be buffered by the proxy.
        configure: (proxy) => {
          proxy.on('proxyRes', (res) => {
            if (res.headers['content-type']?.startsWith('text/event-stream')) {
              res.headers['cache-control'] = 'no-cache';
              res.headers['x-accel-buffering'] = 'no';
            }
          });
        },
      },
    },
  },
  build: {
    // The Go package internal/ui embeds this directory (build tag `ui`).
    outDir: '../internal/ui/dist',
    emptyOutDir: true,
    sourcemap: false,
    target: 'es2022',
  },
});
