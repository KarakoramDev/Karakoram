#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Throwaway v0.4.9 control plane for Glama's stdio MCP inspection, not deployment.

Inspection only: no provider is reachable. The control plane requires a default backend,
so Nomad points to a closed loopback port with an obviously fake credential.
Tools that need a provider will report it as unavailable. No cluster is created.
"""

import http.cookies
import json
import os
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


def main():
    server = None
    bridge = None

    def stop(_signum, _frame):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    # State, credentials and SQLite files are private and removed on exit.
    os.umask(0o077)
    with tempfile.TemporaryDirectory(prefix="glama-", dir=os.getcwd()) as state:
        # Glama's mcp-proxy uses 8080; keep the control plane on a separate port.
        origin = "http://127.0.0.1:18080"
        password = secrets.token_urlsafe(32)
        environment = os.environ.copy()
        environment.update(
            KARAKORAM_DATA_DIR=state,
            KARAKORAM_MASTER_KEY=secrets.token_hex(32),
            KARAKORAM_ADMIN_PASSWORD=password,
            KARAKORAM_LISTEN="127.0.0.1:18080",
            KARAKORAM_INTERNAL_LISTEN="127.0.0.1:9443",
            KARAKORAM_PUBLIC_URL=origin,
            KARAKORAM_ADVERTISE_URL="https://127.0.0.1:9443",
            KARAKORAM_BACKEND="nomad",
            KARAKORAM_NOMAD_ADDR="http://127.0.0.1:1",
            KARAKORAM_NOMAD_NAMESPACE="glama-inspection",
            KARAKORAM_NOMAD_TOKEN="glama-inspection-placeholder",
            KARAKORAM_MCP="readonly",
            # Improves tool completeness for registry inspection; read-only token unchanged.
            KARAKORAM_MCP_OUTPUT_SCHEMAS="true",
            KARAKORAM_OAUTH="off",
            KARAKORAM_OAUTH_DCR="off",
        )
        # Never use an external proxy for the local bootstrap or stdio bridge.
        environment["NO_PROXY"] = environment["no_proxy"] = "127.0.0.1,localhost"
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        cookies = http.cookies.SimpleCookie()
        csrf = ""

        def request(path, body):
            nonlocal csrf
            headers = {"Content-Type": "application/json", "Origin": origin}
            if cookies:
                headers["Cookie"] = "; ".join(f"{k}={v.value}" for k, v in cookies.items())
            if csrf:
                headers["X-Karakoram-CSRF"] = csrf
            req = urllib.request.Request(
                origin + "/api/v1" + path,
                data=json.dumps(body).encode() if body is not None else None,
                headers=headers, method="POST",
            )
            with opener.open(req, timeout=10) as response:
                for cookie in response.headers.get_all("Set-Cookie", []):
                    cookies.load(cookie)
                data = response.read()
                result = json.loads(data) if data else {}
                csrf = result.get("csrfToken", csrf)
                return result

        try:
            # stdout belongs exclusively to MCP JSON-RPC; server logs go to stderr.
            server = subprocess.Popen(
                ["karakoram", "server"], env=environment,
                stdin=subprocess.DEVNULL, stdout=sys.stderr, stderr=sys.stderr,
            )
            for _ in range(100):
                if server.poll() is not None:
                    raise RuntimeError("local control plane exited before readiness")
                try:
                    with opener.open(origin + "/healthz", timeout=1):
                        break
                except (urllib.error.URLError, TimeoutError):
                    time.sleep(0.1)
            else:
                raise RuntimeError("local control plane readiness timed out")

            request("/auth/login", {"username": "admin", "password": password})
            changed_password = secrets.token_urlsafe(32)
            request("/auth/change-password", {
                "currentPassword": password, "newPassword": changed_password,
            })
            cookies.clear()
            csrf = ""
            request("/auth/login", {"username": "admin", "password": changed_password})
            token = request("/api-tokens", {
                "name": "glama-inspection", "restrictions": {
                    "readOnly": True, "allowCredentialReveal": False,
                },
            })["secret"]
            request("/auth/logout", None)
            environment.pop("KARAKORAM_MASTER_KEY")
            environment.pop("KARAKORAM_ADMIN_PASSWORD")
            environment.update(KARAKORAM_SERVER=origin, KARAKORAM_TOKEN=token)
            bridge = subprocess.Popen(["karactl", "mcp"], env=environment)
            return bridge.wait()
        finally:
            for child in (bridge, server):
                if child is not None and child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except urllib.error.HTTPError as error:
        # Response bodies can carry credentials; report only status and API path.
        print(f"Glama bootstrap failed: HTTP {error.code} at {error.url}", file=sys.stderr)
        sys.exit(1)
    except (OSError, RuntimeError, KeyError, ValueError) as error:
        print(f"Glama bootstrap failed: {error}", file=sys.stderr)
        sys.exit(1)
