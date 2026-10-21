<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Agent skills

[karakoram-operator](karakoram-operator/SKILL.md) teaches an AI agent to operate an
installation through MCP or `karactl`, with least privilege, previews, human
confirmation and durable waits. It is an [Agent Skills](https://agentskills.io/specification)
directory, not a server plugin or a grant of access.

Copy the entire `karakoram-operator/` directory into your agent's supported skills
folder, for example `~/.claude/skills/karakoram-operator/` for personal use or
`.claude/skills/karakoram-operator/` for a project. Keep `SKILL.md` inside that
subdirectory and use a version matching your installation. Other agents may use a
different skills folder; follow their installation instructions.

If the repository is published and supported by your skill installer, you can
alternatively use `npx skills add <repo>` with its published repository identifier.
This is an installation option, not a claim that a registry package is published.

Configure the MCP endpoint and restricted token separately in the client's
protected configuration. Never put installation credentials into the skill itself.
For contributing code instead of operating clusters, read [AGENTS.md](../AGENTS.md).
