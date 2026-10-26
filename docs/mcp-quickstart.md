<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# MCP quick start

Connect an AI agent to a Karakoram installation in a few minutes. The management endpoint `https://YOUR-CONTROL-PLANE/mcp` exposes the REST API as MCP tools; each cluster can additionally expose the data inside it at `/mcp/clusters/{namespace}/{cluster}`. This page covers the common path; the [MCP server](mcp.md), [cluster data MCP](data-mcp.md) and [agent access security model](security.md#agent-access-mcp-security-model) pages are the reference.

## 1. Check the server mode

`/mcp` is enabled after installation in **read-only** mode: `KARAKORAM_MCP=readonly` is the default, so agents can inspect clusters, backups, operations and events but cannot change anything. Set `readwrite` to also expose mutations and destructive tools (destructive tools still require a human confirmation), or `off` to make `/mcp` return 404. On Zerops, add the variable to the Karakoram service's environment and restart; in an import YAML the setting is:

```yaml
services:
  - hostname: karakoram
    envVariables:
      KARAKORAM_MCP: readwrite
```

Check the effective mode without credentials: an enabled endpoint answers `401` with a `WWW-Authenticate: Bearer` challenge; a disabled one answers `404`.

```sh
curl -s -o /dev/null -w '%{http_code}\n' https://YOUR-CONTROL-PLANE/mcp
```

The mode applies to the whole installation. Whether a particular agent may write is additionally decided by its token and its owner's RBAC, so the server can stay in `readwrite` while most agents hold read-only tokens.

## 2. Mint a restricted token

Sign in with `karactl` once ([login and contexts](cli.md#login-and-contexts)), then let `karactl mcp config` create a dedicated API token and print the configuration for your client:

```sh
karactl --context production mcp config --client claude-code --namespace production
```

| Flag | Effect |
| --- | --- |
| `--client claude-code\|cursor\|vscode\|codex\|json` | Output format; `json` (default) is a generic `mcpServers` snippet |
| `-n/--namespace NAME` | Restrict the token to a namespace; repeatable. Without it the token sees every namespace its owner can see |
| `--write` | Remove the token's read-only restriction. It never exceeds the owner's RBAC or the server mode |
| `--expires-in 90d` | Lifetime as whole days or a Go duration; 90 days by default |
| `--token-name NAME` | Override the default `mcp-CLIENT-DATE` |
| `--output FILE` | Write the snippet to a file with mode 0600 instead of stdout |
| `--stdio` | Print a `karactl mcp` stdio command for the selected saved context instead of embedding a token; the new token is printed separately and is not saved anywhere |

The token secret is shown **once**. Keep it in the client's configuration or secret store, never in a repository or ticket. Revoke it with `karactl tokens delete ID --yes` or in **Settings → API tokens** when the agent no longer needs it. The same page creates equivalent tokens by hand under **Restrictions** (read-only, namespaces, clusters, permissions); `karactl tokens create --name mcp-agent --read-only --scope-namespace NAME` does the same from the shell ([restricted API tokens](access-control.md#restricted-api-tokens)).

## 3. Connect your client

All remote configurations use the endpoint URL and an `Authorization: Bearer` header over Streamable HTTP. Clients that only speak stdio run the `karactl mcp` bridge instead; it forwards everything, including confirmations, to the server.

### Claude Code

Run the printed command, substituting your values:

```sh
claude mcp add --transport http karakoram https://YOUR-CONTROL-PLANE/mcp --header 'Authorization: Bearer YOUR_API_TOKEN'
```

`--client claude-code --stdio` prints `claude mcp add --transport stdio karakoram -- karactl --context production mcp` instead, which keeps the token in `karactl`'s saved context rather than in Claude Code's settings. `mcp config` never updates a saved context, so that command uses the context's existing credential: first save the newly printed restricted token into a dedicated context (`karactl config set-context mcp-agent --server https://YOUR-CONTROL-PLANE --token NEW_TOKEN`) and use `--context mcp-agent` in the command. Use `/mcp` inside Claude Code to check the connection.

### Claude Desktop

Claude Desktop (and claude.ai) connect to remote servers through **custom connectors**, which are reached from Anthropic's cloud, so the control plane's HTTPS origin must be reachable from the internet. Custom connectors authenticate with OAuth: set `KARAKORAM_OAUTH=on` on the control plane ([connect with OAuth](mcp.md#connect-with-oauth)), then open **Customize → Connectors → Add custom connector**, enter `https://YOUR-CONTROL-PLANE/mcp` and choose **Use Claude's published identity** as the OAuth client. That option uses a client ID metadata document, which Karakoram accepts without dynamic registration; **Register automatically** additionally requires `KARAKORAM_OAUTH_DCR=on`. Each user signs in with their own Karakoram account and approves the scopes and namespace limit on the consent page; the connection appears under **Settings → Connected apps**. Fixed bearer headers for custom connectors are currently a beta feature in Claude that is not available to every organization.

Without OAuth, run the stdio bridge locally. Choose **Settings → Developer → Edit Config** in Claude Desktop and add the server to `claude_desktop_config.json` (this is the output of `--client json --stdio`):

```json
{
  "mcpServers": {
    "karakoram": {
      "command": "karactl",
      "args": ["--context", "production", "mcp"]
    }
  }
}
```

If the desktop app cannot find `karactl`, use its absolute path. As with Claude Code, point `--context` at a dedicated context holding the restricted token, not your administrator context. Restart Claude Desktop after editing the file.

### Cursor

`--client cursor` prints an `mcpServers` entry with `url` and `headers`. Put it in `.cursor/mcp.json` in the project or `~/.cursor/mcp.json` for all projects, for example with `--output ~/.cursor/mcp.json` when the file does not exist yet. Cursor stores the header value in that file, so keep project files out of version control or use `--client cursor --stdio`, which references the saved context instead of a token.

### VS Code

`--client vscode` prints a `servers` entry with `type: http`. Put it in `.vscode/mcp.json` (workspace) or your user profile's `mcp.json`. To keep the token out of the file, replace the header value with a prompted input as shown in the [MCP server guide](mcp.md#client-configuration); VS Code asks for the secret once and stores it securely.

### Codex

`--client codex` prints a TOML table for `~/.codex/config.toml` (or a trusted project's `.codex/config.toml`):

```toml
[mcp_servers.karakoram]
url = "https://YOUR-CONTROL-PLANE/mcp"
http_headers = { Authorization = "Bearer YOUR_API_TOKEN" }
```

To avoid writing the token into the file, replace `http_headers` with `bearer_token_env_var = "KARAKORAM_MCP_TOKEN"` and export that variable where Codex runs. `--client codex --stdio` prints a `command`/`args` table for the bridge instead. With `KARAKORAM_OAUTH=on`, `codex mcp login karakoram` starts the OAuth flow.

### Other clients

Any spec-compliant client works. Use its remote Streamable HTTP option with the endpoint URL and the bearer header (`--client json` prints `url` and `headers`), or OAuth when enabled. The server speaks MCP `2026-07-28` and the legacy initialize-based revisions `2025-11-25`, `2025-06-18` and `2025-03-26`; it does not serve the deprecated HTTP+SSE transport. Stdio-only clients run `karactl --context production mcp` as the server command.

To inspect the endpoint before wiring it into an agent, the MCP Inspector lists the tools your token can see:

```sh
npx @modelcontextprotocol/inspector --cli --transport http --server-url https://YOUR-CONTROL-PLANE/mcp \
  --header 'Authorization: Bearer YOUR_API_TOKEN' --method tools/list
```

## 4. Try it

Start with questions, not changes; everything below works in read-only mode:

- "Which clusters can you see, and what phase is each in?" (`clusters_list`, `cluster_status_get`)
- "Diagnose cluster `appdb` in namespace `production`." (`cluster_diagnose` gathers status, members, recent operations, warning events and backups)
- "When was the last successful backup of `appdb`?" (`backups_list`)
- "What would change if `appdb` had three members? Preview only." (`cluster_update_dry_run` returns the changed paths, effects and warnings without applying anything)

In `readwrite` mode with a `--write` token, "Take a backup of `appdb` and wait for it" calls `backup_create` and then `wait`; mutations are asynchronous and `wait` observes the operation. A destructive request such as deleting a backup pauses in your client for a confirmation written by the server; some confirmations require typing the resource's actual name. If your client does not support elicitation, the tool returns an error naming the equivalent `karactl` command and nothing executes.

Clients with tool limits can narrow the list on the server: append `?toolsets=Clusters,Backups,Workflows` to the endpoint URL (OpenAPI tags; `Workflows` keeps `wait` and `cluster_diagnose`).

## 5. Data MCP quick start

[Cluster data MCP](data-mcp.md) gives an agent tools for the data inside one cluster through a separate per-cluster endpoint. It is off per cluster until you enable it, and it needs the `cluster-data` permission, which the built-in viewer and operator roles do not have.

1. Enable it on the cluster with a merge patch (read-only, one MongoDB database in this example):

   ```yaml
   spec:
     dataMcp:
       enabled: true
       access: read
       scope:
         databases: [app]
   ```

   ```sh
   karactl --context production -n production clusters patch appdb -f datamcp.yaml
   ```

   MongoDB, PostgreSQL and Supabase clusters start a small companion service; Elasticsearch and RabbitMQ use built-in tools. Wait until `status.dataMcp.phase` is `Ready` (`karactl -n production clusters get appdb -o json`); the web UI shows the same on the cluster's **Data MCP** card.

2. Make sure your account holds `cluster-data:read` on `production/appdb`. Administrators already do; otherwise add `p, user:NAME, cluster-data, read, production/appdb` to the [policy](access-control.md#policy-format).

3. Mint a cluster-scoped token and connect:

   ```sh
   karactl --context production mcp config --cluster production/appdb --client claude-code
   ```

   `--cluster` accepts `claude-code`, `cursor`, `vscode` and `json`; `--namespace`, `--stdio` and `--output` do not apply. The printed configuration targets `https://YOUR-CONTROL-PLANE/mcp/clusters/production/appdb`.

4. Ask: "Which collections are in database `app`, and what does a typical document look like?" Read tools run through a dedicated read-only database identity. Writes require [all four switches](data-mcp.md#four-switches-for-writes): cluster `access: readWrite`, a `--write` token, owner RBAC `cluster-data:write` and `KARAKORAM_MCP=readwrite`; destructive tools always ask for confirmation.

Treat everything an agent reads from a database as untrusted data; see [risks and operating practice](data-mcp.md#risks-and-operating-practice).
