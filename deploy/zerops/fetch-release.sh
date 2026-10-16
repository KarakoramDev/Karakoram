#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
#
# Zerops build step of the prebuilt `karakoram` setup: downloads the server
# release pinned in deploy/zerops/RELEASE, verifies the signed checksums and
# writes ./karakoram. Run from the repository root; needs curl, ssh-keygen,
# sha256sum and tar. Forks without their own signed releases use the
# `karakoram-source` setup instead.
set -eu

repo=https://github.com/KarakoramDev/Karakoram
signer=release@karakoram.dev
namespace=karakoram-release

fail() { printf 'fetch-release: %s\n' "$*" >&2; exit 1; }
for tool in curl ssh-keygen sha256sum tar awk; do
    command -v "$tool" >/dev/null 2>&1 || fail "$tool is required in the build container"
done
version=$(tr -d ' \t\r\n' <deploy/zerops/RELEASE) || fail 'deploy/zerops/RELEASE is missing'
printf '%s\n' "$version" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$' ||
    fail "invalid release '$version' in deploy/zerops/RELEASE"
case $(uname -m) in
    x86_64 | amd64) arch=amd64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *) fail "unsupported architecture $(uname -m)" ;;
esac
archive=karakoram-$version-linux-$arch.tar.gz

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

for asset in checksums.txt checksums.txt.sig "$archive"; do
    curl --proto '=https' --proto-redir '=https' --tlsv1.2 --fail --silent --show-error --location \
        --retry 3 --connect-timeout 20 --max-time 600 \
        --output "$tmp/$asset" "$repo/releases/download/$version/$asset" ||
        fail "cannot download $asset of $version"
done

ssh-keygen -Y verify -f deploy/allowed_signers -I "$signer" -n "$namespace" \
    -s "$tmp/checksums.txt.sig" <"$tmp/checksums.txt" >/dev/null ||
    fail "checksums.txt of $version is not signed by a key in deploy/allowed_signers"
expected=$(awk -v f="$archive" '$2 == f { print $1; n++ } END { exit n != 1 }' "$tmp/checksums.txt") ||
    fail "checksums.txt has no single entry for $archive"
printf '%s\n' "$expected" | grep -Eq '^[0-9a-f]{64}$' || fail "malformed checksum for $archive"
actual=$(sha256sum "$tmp/$archive" | cut -d ' ' -f 1)
[ "$actual" = "$expected" ] || fail "sha256 mismatch for $archive"

mkdir "$tmp/x"
tar -xzf "$tmp/$archive" -C "$tmp/x" karakoram
install -m 0755 "$tmp/x/karakoram" ./karakoram
./karakoram version
./karakoram version | grep -Fq "$version (commit " || fail "downloaded binary does not report $version"
