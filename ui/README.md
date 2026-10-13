# Karakoram web UI

React 18 + TypeScript + Vite single-page app. Talks to the control plane at `/api/v1` with the
cookie session (`X-Karakoram-CSRF` on unsafe methods) and keeps views live from the
`/api/v1/events/stream` server-sent events.

## Commands

```sh
npm ci              # install (lockfile committed)
npm run gen:api     # regenerate src/api/schema.d.ts from ../api/openapi.yaml (never hand-edit)
npm run dev         # Vite on :5173, proxies /api -> http://localhost:8080 (override KARAKORAM_API_URL)
npm run typecheck
npm run lint
npm test            # unit tests via node --test; needs Node 22.6+ (TypeScript type stripping)
npm run build       # writes ../internal/ui/dist
```

## Embedding in the Go binary

`internal/ui` serves the SPA with history-API fallback and cache headers. The build output is
embedded only when Go is compiled with `-tags ui`; without the tag the package still builds and
serves a "UI not built" page, so Go builds and tests never need Node:

```sh
(cd ui && npm ci && npm run build) && go build -tags ui ./cmd/karakoram
```

## Layout

- `src/api` – generated OpenAPI types, typed fetch client (`openapi-fetch`), query keys, SSE client.
- `src/auth` – session provider (`/auth/me`, `/permissions`), client-side policy evaluation that
  mirrors the server (deny overrides allow, doublestar objects `<namespace>/<cluster>`), route guard.
- `src/namespaces` – the namespace scope of collection views: `useNamespace()` reads `?namespace=`
  (authoritative) with a non-secret localStorage fallback so the choice survives navigation;
  `undefined` means every readable namespace, which is also the API default.
  The switcher, namespace settings and creation/restore selectors fetch every authorized namespace
  page before exposing the collection; choices are not capped at the API page size.
- `src/components/ui` – dependency-free component kit (buttons, fields, native `<dialog>`, toasts,
  SVG line chart).
- `src/layout` – app shell with the sidebar namespace switcher.
- `src/pages` – clusters list, creation wizard, cluster detail tabs (`/clusters/<namespace>/<name>`;
  pre-namespace `/clusters/<name>` links redirect to `default`), global events/operations, settings
  (including Settings → Namespaces).
- `src/styles` – design tokens and component styles (no CSS framework).

Credentials are only fetched on explicit "Reveal credentials" (`GET /clusters/{c}/connection`), which
the server gates on the `cluster-credentials:read` permission; they are never cached in the query
store. Namespace tokens are write-only: the add and rotate dialogs clear the field on submit and
nothing token-like is kept in the query cache or localStorage.
