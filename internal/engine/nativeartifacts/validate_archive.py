# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Validate tar headers, not locale-dependent human-readable tar listings."""
from collections import deque
import posixpath
import sys
import tarfile


def validate(filename, kind, major=None):
    if kind not in ("application", "pg-extension"):
        raise ValueError("invalid archive kind")
    extensions = kind == "pg-extension"
    if extensions and major not in ("16", "17", "18"):
        raise ValueError("missing or invalid PostgreSQL major")
    if not extensions and major is not None:
        raise ValueError("unexpected PostgreSQL major")
    with tarfile.open(filename, "r:*") as archive:
        members = archive.getmembers()
    entries = {}
    symlinks = {}
    roots = (f"usr/lib/postgresql/{major}", f"usr/share/postgresql/{major}")
    ancestors = {".", "usr", "usr/lib", "usr/share", "usr/lib/postgresql", "usr/share/postgresql"}
    for member in members:
        name = member.name
        if not name or name.startswith("/") or ".." in name.split("/") or any(ord(c) < 32 or ord(c) == 127 for c in name):
            raise ValueError("invalid path: " + repr(name))
        name = posixpath.normpath(name)
        if name in entries and not (member.isdir() and entries[name].isdir()):
            raise ValueError("duplicate path: " + repr(name))
        entries[name] = member
        if not (member.isfile() or member.isdir() or member.issym() or member.islnk()):
            raise ValueError("unsupported file type: " + repr(name))
        if extensions:
            if not (member.isfile() or member.isdir()):
                raise ValueError("unexpected extension archive type: " + repr(name))
            if not (any(name == root or name.startswith(root + "/") for root in roots) or (name in ancestors and member.isdir())):
                raise ValueError("unexpected extension archive member: " + repr(name))
        if member.issym() or member.islnk():
            if not member.linkname or member.linkname.startswith("/") or any(ord(c) < 32 or ord(c) == 127 for c in member.linkname):
                raise ValueError("invalid link target: " + repr(name))
        if member.issym():
            symlinks[name] = member.linkname

    def resolve(path, follow_final=True):
        pending = deque(path.split("/"))
        resolved = []
        followed = 0
        while pending:
            part = pending.popleft()
            if part in ("", "."):
                continue
            if part == "..":
                if not resolved:
                    raise ValueError("link escapes destination: " + repr(path))
                resolved.pop()
                continue
            candidate = "/".join([*resolved, part])
            if candidate in symlinks and (pending or follow_final):
                followed += 1
                if followed > 40:
                    raise ValueError("too many symbolic links: " + repr(path))
                pending.extendleft(reversed(symlinks[candidate].split("/")))
            else:
                resolved.append(part)
        return "/".join(resolved) or "."

    for name, member in entries.items():
        parent = posixpath.dirname(name)
        while parent not in ("", "."):
            if parent in symlinks:
                raise ValueError("member traverses a symbolic link: " + repr(name))
            parent = posixpath.dirname(parent)
        resolve(name)
        if member.islnk():
            # Hardlink targets are archive-root-relative, unlike symlinks.
            target = member.linkname
            seen = {name}
            while True:
                target = resolve(target, follow_final=False)
                if target in seen or target not in entries:
                    raise ValueError("invalid hardlink: " + repr(name))
                seen.add(target)
                linked = entries[target]
                if linked.isfile():
                    break
                if not linked.islnk():
                    raise ValueError("hardlink must reference a regular file: " + repr(name))
                target = linked.linkname


if __name__ == "__main__":
    try:
        if len(sys.argv) not in (3, 4):
            raise ValueError("expected archive, kind and optional PostgreSQL major")
        validate(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) == 4 else None)
    except (OSError, ValueError, tarfile.TarError) as error:
        sys.exit("unsafe archive member: " + str(error))
