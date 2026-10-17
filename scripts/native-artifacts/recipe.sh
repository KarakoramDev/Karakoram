#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
set -euo pipefail
name=$1 source=$2 node_version=$3
stage=/stage/$name
rm -rf "$stage"
if [ "$name" = supabase-pg17-extensions ]; then
    mkdir -p "$stage/usr/share/postgresql/17/karakoram-native-artifacts/LICENSES"
    license_dir="$stage/usr/share/postgresql/17/karakoram-native-artifacts/LICENSES"
else
    mkdir -p "$stage/bin" "$stage/LICENSES"
    license_dir="$stage/LICENSES"
fi
cd "$source"
case "$name" in
    supabase-studio|supabase-supavisor|supabase-logflare)
        git apply "/root/$name.patch"
        mkdir -p "$stage/native-patches"
        cp "/root/$name.patch" "$stage/native-patches/"
        (cd /root && sha256sum "$name.patch") > "$stage/native-patches/SHA256SUMS"
        ;;
esac
find . -maxdepth 1 -iname 'license*' -exec cp -a '{}' "$license_dir/" \;
case "$name" in
    supabase-storage)
        test "$(npm --version)" = 11.12.1
        npm ci --ignore-scripts
        npm rebuild
        npm run build
        npm prune --omit=dev --ignore-scripts
        mkdir -p "$stage/app"
        cp -a dist node_modules migrations package.json watt.json watt.storage.json "$stage/app/"
        entry=dist/start/server.js
        ;;
    supabase-postgres-meta)
        npm clean-install
        npm run build
        npm prune --omit=dev
        mkdir -p "$stage/app"
        cp -a dist node_modules package.json LICENSE "$stage/app/"
        entry=dist/server/server.js
        ;;
    supabase-studio)
        test "$(pnpm --version)" = 11.13.1
        test "$(turbo --version)" = 2.9.14
        turbo prune studio --docker
        work=/build/studio-native
        rm -rf "$work"; mkdir -p "$work"
        cp -a out/json/. "$work/"
        cp out/pnpm-lock.yaml "$work/"
        cp -a patches "$work/"
        cd "$work"
        pnpm install --frozen-lockfile
        cp -a "$source/out/full/." .
        mkdir -p apps/studio/public/deno
        cp /root/studio-lib.deno.d.ts apps/studio/public/deno/lib.deno.d.ts
        cp /root/studio-edge-runtime.d.ts apps/studio/public/deno/edge-runtime.d.ts
        test -s apps/studio/public/deno/lib.deno.d.ts
        test -s apps/studio/public/deno/edge-runtime.d.ts
        printf '%s\n' v1.45.0 > apps/studio/public/deno/deno-version.txt
        printf '%s\n' 2.4.4 > apps/studio/public/deno/supabase-functions-js-version.txt
        cp /root/studio-deno-LICENSE /root/studio-functions-js-LICENSE "$license_dir/"
        NEXT_TELEMETRY_DISABLED=1 RAYON_NUM_THREADS=2 pnpm --filter studio exec next build --webpack
        mkdir -p "$stage/app"
        cp -a apps/studio/.next/standalone/. "$stage/app/"
        mkdir -p "$stage/app/apps/studio/.next"
        cp -a apps/studio/.next/static "$stage/app/apps/studio/.next/"
        cp -a apps/studio/public "$stage/app/apps/studio/"
        # Next's file tracer omits this dynamically loaded parser WASM.
        test -n "$(find node_modules -path '*/libpg-query/wasm/libpg-query.wasm' -print -quit)"
        find node_modules -path '*/libpg-query/wasm/libpg-query.wasm' -exec cp --parents '{}' "$stage/app/" \;
        cp node_modules/.pnpm/libpg-query@17.6.0/node_modules/libpg-query/LICENSE "$license_dir/libpg-query-LICENSE"
        entry=apps/studio/server.js
        ;;
    supabase-imgproxy)
        CGO_ENABLED=1 go build -trimpath -ldflags='-s -w' -o "$stage/bin/imgproxy" .
        ln -s imgproxy "$stage/bin/start"
        ;;
    supabase-pg17-extensions)
        python3 /root/extensions.py
        ;;
    supabase-edge-runtime)
        GIT_V_TAG=v1.77.4 cargo build --profile release --features ""
        cp target/release/edge-runtime "$stage/bin/"
        mkdir -p "$stage/app/main"
        curl -fsSL https://raw.githubusercontent.com/supabase/supabase/bc27d2661edd09a5d0b8d76ce33da6d4071a756e/docker/volumes/functions/main/index.ts -o "$stage/app/main/upstream-index.ts"
        python3 /root/worker.py "$stage/app/main/upstream-index.ts" "$stage/app/main/index.ts"
        cat > "$stage/bin/start" <<'EOF'
#!/bin/sh
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
exec "$root/bin/edge-runtime" start --user-worker-request-idle-timeout 150000 --main-service "$root/app/main"
EOF
        chmod 0755 "$stage/bin/start"
        ;;
    supabase-realtime|supabase-supavisor|supabase-logflare)
        export MIX_ENV=prod
        export HEX_CACERTS_PATH=/etc/ssl/certs/ca-certificates.crt
        mix archive.install /root/hex-2.5.3.ez --force
        mix local.rebar rebar3 /root/rebar3-3.24.0 --force
        mix deps.get --only prod
        mix deps.compile
        app=${name#supabase-}
        if [ "$app" = realtime ]; then
            (cd assets && npm ci --omit=dev)
            mix assets.deploy
        fi
        if [ "$app" = logflare ]; then
            npm --prefix assets ci
            export CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
        fi
        mix compile
        mix release "$app"
        if [ "$app" = logflare ]; then
            npm run --prefix assets deploy
            mix phx.digest
            cp -a priv/static/. "_build/prod/rel/logflare/lib/logflare-$(cat VERSION)/priv/static/"
        fi
        cp -a "_build/prod/rel/$app" "$stage/app"
        if [ "$app" = realtime ]; then
            # The native pgdelta helper is part of Realtime's startup contract.
            version=$(python3 -c 'import json; print(json.load(open("/root/native-manifest.json"))["version"])')
            helper_root="/opt/karakoram/$name-$version/app/pg-toolbelt"
            mkdir -p "$helper_root"
            git -C "$helper_root" init
            git -C "$helper_root" fetch --depth=1 https://github.com/supabase/pg-toolbelt.git 107ac3df4b889c527215d1f6a37df64b33154c16
            git -C "$helper_root" checkout --detach FETCH_HEAD
            cd "$helper_root"
            bun install --frozen-lockfile --ignore-scripts
            (cd packages/pg-delta && bun build --compile src/cli/main.ts --outfile "$stage/bin/pgdelta")
            mkdir -p "$stage/app/pg-toolbelt"
            test -n "$(find . -path '*/@libpg-query/parser/wasm/libpg-query.wasm' -print -quit)"
            find . -path '*/@libpg-query/parser/wasm/libpg-query.wasm' -exec cp --parents '{}' "$stage/app/pg-toolbelt/" \;
        fi
        cat > "$stage/bin/start" <<EOF
#!/bin/sh
root=\$(CDPATH= cd -- "\$(dirname "\$0")/.." && pwd)
export PATH="\$root/bin:\$PATH"
cd "\$root/app"
exec "\$root/app/bin/$app" start
EOF
        chmod 0755 "$stage/bin/start"
        ;;
    *) printf 'Unknown recipe: %s\n' "$name" >&2; exit 2 ;;
esac
if [ -n "${entry:-}" ]; then
    cp -a "/opt/node-$node_version" "$stage/node"
    cat > "$stage/bin/start" <<EOF
#!/bin/sh
root=\$(CDPATH= cd -- "\$(dirname "\$0")/.." && pwd)
cd "\$root/app"
export NODE_ENV=production
exec "\$root/node/bin/node" "$entry"
EOF
    chmod 0755 "$stage/bin/start"
fi
