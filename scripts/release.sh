#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
#
# Maintainer release tool. See "Releasing" in CONTRIBUTING.md.
#
#   scripts/release.sh prepare VERSION          commit the Zerops release pin and create the tag
#   scripts/release.sh build   VERSION          build signed release assets from the tag into dist/VERSION
#   scripts/release.sh publish VERSION NOTES    build, upload a draft, verify it, publish, push main
set -euo pipefail

readonly REPO=KarakoramDev/Karakoram
readonly SIGNER=release@karakoram.dev
readonly NAMESPACE=karakoram-release
readonly PIN=deploy/zerops/RELEASE
readonly SIGNERS=deploy/allowed_signers
readonly PKG=github.com/KarakoramDev/Karakoram/internal/version
readonly SERVER_PLATFORMS="linux/amd64 linux/arm64"
readonly CLI_PLATFORMS="linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64"
KEY=${KARAKORAM_RELEASE_SIGNING_KEY:-$HOME/.config/karakoram/release-signing-ed25519}

die() { printf 'release: %s\n' "$*" >&2; exit 1; }
log() { printf '==> %s\n' "$*" >&2; }

usage() {
    sed -n '7,9p' "$0" | sed 's/^# //' >&2
    exit 2
}

root=$(git rev-parse --show-toplevel 2>/dev/null) || die 'run inside the Karakoram repository'
cd "$root"

check_version() {
    printf '%s\n' "$1" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$' ||
        die "VERSION must be a semantic version tag such as v1.2.3, got '$1'"
}

check_clean() {
    git update-index -q --refresh
    [ -z "$(git status --porcelain --untracked-files=normal)" ] || die 'working tree is not clean'
}

remote_tag() { git ls-remote --tags origin "refs/tags/$1^{}" | cut -f1; }

tmpdirs=()
cleanup() { [ "${#tmpdirs[@]}" -eq 0 ] || rm -rf "${tmpdirs[@]}"; }
trap cleanup EXIT
# newtmp VAR PREFIX: create a temporary directory removed on exit.
newtmp() {
    local dir
    dir=$(mktemp -d "${TMPDIR:-/tmp}/$2.XXXXXX")
    tmpdirs+=("$dir")
    printf -v "$1" '%s' "$dir"
}

prepare() {
    local version=$1 current
    check_clean
    [ "$(git symbolic-ref --short -q HEAD)" = main ] || die 'check out main first'
    git fetch --quiet origin main
    [ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] || die 'main must equal origin/main'
    git rev-parse -q --verify "refs/tags/$version" >/dev/null && die "tag $version already exists locally"
    [ -z "$(git ls-remote --tags origin "refs/tags/$version")" ] || die "tag $version already exists on origin"
    current=$(cat "$PIN" 2>/dev/null || true)
    [ "$current" != "$version" ] || die "$PIN already pins $version"
    [ -z "$current" ] || [ "$(printf '%s\n' "${current%%-*}" "${version%%-*}" | sort -V | tail -n 1)" = "${version%%-*}" ] ||
        die "$version is older than the pinned $current"
    # Commit on a detached HEAD: local main must not carry a pin before the release is published.
    git checkout --quiet --detach
    printf '%s\n' "$version" >"$PIN"
    git add "$PIN"
    git commit --quiet --signoff -m "Release $version"
    git tag --annotate --message "Karakoram $version" "$version"
    log "created commit $(git rev-parse --short HEAD) and tag $version on a detached HEAD (main is unchanged)"
    log "review it, then run: make release VERSION=$version NOTES=<file>"
}

build() {
    local version=$1 out=$root/dist/$1
    git rev-parse -q --verify "refs/tags/$version^{commit}" >/dev/null || die "tag $version does not exist; run make release-prepare first"
    [ "$(git show "$version:$PIN" 2>/dev/null)" = "$version" ] || die "$PIN at tag $version does not pin $version"
    [ -r "$KEY" ] || die "signing key $KEY not found (set KARAKORAM_RELEASE_SIGNING_KEY)"
    for tool in go npm gzip sha256sum ssh-keygen python3; do command -v "$tool" >/dev/null || die "$tool is required"; done
    tar --version 2>/dev/null | grep -q 'GNU tar' || die 'GNU tar is required for reproducible archives'

    local commit epoch build_date work
    commit=$(git rev-parse "$version^{commit}")
    epoch=$(git log -1 --format=%ct "$commit")
    build_date=$(date -u -d "@$epoch" +%Y-%m-%dT%H:%M:%SZ)
    newtmp work karakoram-release

    log "exporting $version ($commit) to a clean tree"
    git archive --format=tar "$commit" | tar -x -C "$work"

    # Fail before the build if the key is not authorized by the tag's allowed_signers.
    printf '%s\n' "$version" >"$work/key-check"
    ssh-keygen -q -Y sign -f "$KEY" -n "$NAMESPACE" "$work/key-check"
    ssh-keygen -Y verify -f "$work/$SIGNERS" -I "$SIGNER" -n "$NAMESPACE" -s "$work/key-check.sig" \
        <"$work/key-check" >/dev/null 2>&1 || die "$KEY is not authorized in $SIGNERS at $version"

    log 'building UI'
    (cd "$work/ui" && npm ci --no-audit --no-fund --loglevel=error && npm run --silent build) >&2
    [ -f "$work/internal/ui/dist/index.html" ] || die 'UI build produced no index.html'

    rm -rf "$out"
    mkdir -p "$out" "$work/stage"
    local ldflags="-s -w -X $PKG.Version=$version -X $PKG.Commit=$commit -X $PKG.BuildDate=$build_date"
    gobuild() { # GOOS GOARCH OUTPUT PACKAGE
        log "go build $4 for $1/$2"
        (cd "$work" && CGO_ENABLED=0 GOOS=$1 GOARCH=$2 GOFLAGS=-mod=readonly \
            go build -tags ui -trimpath -buildvcs=false -ldflags "$ldflags" -o "$3" "$4")
    }

    local platform os arch name
    for platform in $SERVER_PLATFORMS; do
        os=${platform%/*} arch=${platform#*/}
        name=karakoram-$version-$os-$arch
        rm -rf "$work/stage/$name" && mkdir -p "$work/stage/$name"
        gobuild "$os" "$arch" "$work/stage/$name/karakoram" ./cmd/karakoram
        cp "$work/LICENSE" "$work/NOTICE" "$work/stage/$name/"
        chmod 0755 "$work/stage/$name/karakoram"
        chmod 0644 "$work/stage/$name/LICENSE" "$work/stage/$name/NOTICE"
        tar --sort=name --mtime="@$epoch" --owner=0 --group=0 --numeric-owner --format=gnu \
            -C "$work/stage/$name" -cf - karakoram LICENSE NOTICE | gzip -9 -n >"$out/$name.tar.gz"
    done
    for platform in $CLI_PLATFORMS; do
        os=${platform%/*} arch=${platform#*/}
        name=karactl-$os-$arch
        [ "$os" = windows ] && name=$name.exe
        gobuild "$os" "$arch" "$out/$name" ./cmd/karactl
        chmod 0755 "$out/$name"
    done

    local host_server
    host_server=$work/stage/karakoram-$version-$(go env GOHOSTOS)-$(go env GOHOSTARCH)/karakoram
    if [ -x "$host_server" ]; then
        "$host_server" version | grep -Fq "$version (commit $commit, built $build_date)" ||
            die "server binary reports unexpected version: $("$host_server" version)"
    fi

    log 'staging verified native artifacts from the tagged lock'
    python3 "$work/scripts/native-artifacts/release.py" "$work/internal/engine/nativeartifacts/lock.json" \
        "$root/dist/native-artifacts" "$out" || die 'native artifact staging failed'

    (cd "$out" && sha256sum -- * >checksums.txt)
    log "signing checksums.txt with $KEY"
    ssh-keygen -q -Y sign -f "$KEY" -n "$NAMESPACE" "$out/checksums.txt"
    verify_dir "$out" "$work/$SIGNERS"
    log "assets in $out (built with $(go env GOVERSION), BuildDate $build_date):"
    (cd "$out" && ls -1) >&2
}

# verify_dir DIR ALLOWED_SIGNERS: signature over checksums.txt and every listed digest.
verify_dir() {
    ssh-keygen -Y verify -f "$2" -I "$SIGNER" -n "$NAMESPACE" -s "$1/checksums.txt.sig" <"$1/checksums.txt" >&2 ||
        die "signature verification failed in $1"
    (cd "$1" && sha256sum --quiet --strict -c checksums.txt) || die "checksum verification failed in $1"
}

# verify_release VERSION [DIST]: download the GitHub release assets, verify them and compare them with DIST.
verify_release() {
    local check f
    newtmp check karakoram-verify
    gh release download "$1" -R "$REPO" -D "$check"
    if [ -n "${2:-}" ]; then
        [ "$(cd "$2" && ls -1)" = "$(cd "$check" && ls -1)" ] || die 'uploaded asset list differs from dist'
        for f in "$2"/*; do cmp -s "$f" "$check/${f##*/}" || die "uploaded ${f##*/} differs from dist"; done
    fi
    verify_dir "$check" "$root/$SIGNERS"
}

publish() {
    local version=$1 notes=$2 out=$root/dist/$1 commit state
    local -a kind=(--latest)
    case $version in *-*) kind=(--prerelease --latest=false) ;; esac
    [ -n "$notes" ] && [ -r "$notes" ] || die 'NOTES must name a readable release-notes file'
    command -v gh >/dev/null || die 'gh is required'
    gh auth status --hostname github.com >/dev/null 2>&1 || die 'gh is not logged in to github.com'
    check_clean
    commit=$(git rev-parse -q --verify "refs/tags/$version^{commit}") || die "tag $version does not exist"
    [ "$(git rev-parse HEAD)" = "$commit" ] || die "HEAD must be the tagged commit $version"
    git fetch --quiet origin main
    # Reruns resume: an unfinished draft is replaced, a published release is only re-verified.
    state=$(gh release view "$version" -R "$REPO" --json isDraft -q 'if .isDraft then "draft" else "published" end' 2>/dev/null || true)
    if ! git merge-base --is-ancestor origin/main "$commit"; then
        [ "$state" = published ] && die "origin/main moved after $version was published; merge $commit into main and push it"
        die 'origin/main moved; rebase and re-tag before publishing'
    fi
    case $(remote_tag "$version") in
        '') ;;
        "$commit") log "tag $version already on origin" ;;
        *) die "origin has a different $version tag" ;;
    esac

    if [ "$state" = draft ]; then
        log "deleting the unfinished draft release $version (the tag stays)"
        gh release delete "$version" -R "$REPO" --yes
    fi
    if [ "$state" = published ]; then
        log "release $version is already published; verifying its assets"
        verify_release "$version" "$([ -d "$out" ] && printf '%s' "$out")"
    else
        build "$version"
        log "pushing tag $version"
        git push --quiet origin "refs/tags/$version"
        log 'creating draft release'
        gh release create "$version" -R "$REPO" --draft --verify-tag --title "Karakoram $version" \
            --notes-file "$notes" "$out"/*
        log 'verifying uploaded draft assets'
        verify_release "$version" "$out"
        log 'publishing'
        gh release edit "$version" -R "$REPO" --draft=false "${kind[@]}"
    fi

    log 'fast-forwarding origin/main to the release commit'
    git push --quiet origin "$commit:refs/heads/main"
    log "published https://github.com/$REPO/releases/tag/$version"
    log 'update your checkout: git checkout main && git merge --ff-only origin/main'
}

[ "$#" -ge 2 ] || usage
command=$1 version=$2
check_version "$version"
case $command in
    prepare) [ "$#" -eq 2 ] || usage; prepare "$version" ;;
    build) [ "$#" -eq 2 ] || usage; build "$version" ;;
    publish) [ "$#" -eq 3 ] || usage; publish "$version" "$3" ;;
    *) usage ;;
esac
