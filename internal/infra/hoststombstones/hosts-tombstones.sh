#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
set -eu
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
LC_ALL=C
export LC_ALL
umask 077
fail() { printf '%s\n' "karakoram hosts tombstones: $*" >&2; exit 1; }
[ "$#" -eq 0 ] || fail 'arguments are not accepted'
hosts=/etc/hosts
lock=/etc/.karakoram-hosts-tombstones.lock
[ -f "$hosts" ] && [ ! -L "$hosts" ] || fail 'hosts target is not a regular file'
exec 9> "$lock"
flock -x 9 || fail 'cannot lock hosts file'
tmp=$(mktemp -d /tmp/karakoram-hosts.XXXXXXXXXX) || fail 'cannot create temporary directory'
writing=0
restore() {
    [ "$writing" -eq 0 ] && return 0
    printf '%s' "$original" > "$hosts" && cmp -s "$tmp/backup" "$hosts"
}
trap 'restore || printf "%s\n" "karakoram hosts tombstones: rollback failed" >&2; rm -rf "$tmp"' EXIT
trap 'exit 1' HUP INT TERM
own=$(uname -n) || fail 'cannot determine own hostname'
awk -v own="$own" '
 NR > 64 || length($0) > 63 || $0 !~ /^[a-z][a-z0-9-]*$/ || $0 == own { bad=1; exit 1 }
 { print }
 END { if (bad) exit 1 }
' > "$tmp/input" || fail 'invalid hostname list (maximum 64; own hostname forbidden)'
sort -u "$tmp/input" > "$tmp/names" || fail 'cannot sort hostnames'
attempt=0
while [ "$attempt" -lt 3 ]; do
attempt=$((attempt + 1))
[ -f "$hosts" ] && [ ! -L "$hosts" ] || fail 'hosts target is not a regular file'
cat "$hosts" > "$tmp/backup" || fail 'cannot back up hosts file'
awk '
 FILENAME == ARGV[1] { retired[$0]=1; next }
 $0 == "# BEGIN karakoram resolution tombstones" { if (inside || blocks++) { bad=1; exit 1 }; inside=1; next }
 $0 == "# END karakoram resolution tombstones" { if (!inside) { bad=1; exit 1 }; inside=0; next }
 inside { next }
 {
   row=$0; comment=""; at=index(row, "#")
   if (at) { comment=substr(row, at); row=substr(row, 1, at-1) }
   n=split(row, fields, /[[:space:]]+/); address=""; aliases=""; removed=0
   for (i=1; i<=n; i++) {
     if (fields[i] == "") continue
     if (address == "") { address=fields[i]; continue }
     if (fields[i] in retired) { removed=1; continue }
     aliases=aliases " " fields[i]
   }
   if (!removed) print
   else if (aliases != "") print address aliases (comment == "" ? "" : " " comment)
 }
 END { if (inside || bad) exit 1 }
' "$tmp/names" "$tmp/backup" > "$tmp/hosts" || fail 'cannot transform hosts file (check managed markers)'
if [ -s "$tmp/names" ]; then
    printf '%s\n' '# BEGIN karakoram resolution tombstones' >> "$tmp/hosts" || fail 'cannot write managed block'
    while IFS= read -r name; do
        printf '239.192.0.1 %s\nff18::4b4b:1 %s\n' "$name" "$name" >> "$tmp/hosts" || fail 'cannot write sentinels'
    done < "$tmp/names"
    printf '%s\n' '# END karakoram resolution tombstones' >> "$tmp/hosts" || fail 'cannot finish managed block'
fi
# Shell builtin printf avoids a streaming truncate-and-copy, using one write
# where the shell/size permits. Preserve all trailing newlines in both buffers.
original=$(cat "$tmp/backup" || exit 1; printf '.') || fail 'cannot read backup'
original=${original%.}
replacement=$(cat "$tmp/hosts" || exit 1; printf '.') || fail 'cannot read replacement'
replacement=${replacement%.}
# Docker does not share our lock. Retry if it changed the snapshot before write.
[ -f "$hosts" ] && [ ! -L "$hosts" ] || fail 'hosts target is not a regular file'
cmp -s "$tmp/backup" "$hosts" || continue
# Docker bind-mounts this inode: rename would fail or replace the wrong file.
writing=1
if ! printf '%s' "$replacement" > "$hosts" || ! cmp -s "$tmp/hosts" "$hosts"; then
    restore || fail 'hosts update and rollback failed'
    writing=0
    fail 'hosts update failed; original restored'
fi
writing=0
exit 0
done
fail 'hosts file kept changing before installation'
