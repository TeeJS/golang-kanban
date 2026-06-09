# Project Charter: Kanban MCP Server

**Date:** 2026-06-09
**Status:** Awaiting sign-off

## 1. What is the one thing this must do?

Let AI assistants (Claude Code, Claude Desktop, and other MCP clients) fully manage the
kanban board — list, create, update, move, and delete cards — through a standard MCP
server reachable over the network, with no manual steps per session.

## 2. What would be wrong if we shipped "working" software without it?

- An MCP server that is read-only, or that can create cards but not move/update/delete
  them, is not "managing the board."
- Changes made by an AI must appear live in the browser (the existing SSE refresh must
  fire) — silent DB writes that require a manual page reload don't count.
- Errors must be surfaced to the AI as real error messages, not swallowed 200s.

## 3. What is explicitly off-limits as a workaround?

- Telling the user (or the AI) to "just use curl" — that's option 1, which was rejected.
- stdio-only transport that requires installing a runtime/binary on every client machine.
- Manual edits to the database or config files as part of normal operation.
- Parsing HTML fragments as the machine interface — proper JSON endpoints are part of
  this work.

## 4. Deployment target and backup location

- **Deployment:** Unraid, as a service in the existing `docker-compose.yml` stack,
  built with `docker build` on Unraid like the kanban app itself (Go is NOT installed
  on the Windows dev machine).
- **Backups:** date-time-stamped copies in the project root before editing any existing
  file (e.g., `main.go.backup-YYYYMMDD-HHMMSS`), per existing convention.

## 5. How will we verify it is done?

1. `GET /api/cards` etc. still work; new `POST /api/cards`, `PATCH /api/cards/{id}`,
   `DELETE /api/cards/{id}` return JSON with correct status codes, authenticated by
   `X-API-Key`.
2. The MCP server container starts in the stack and responds on its HTTP endpoint.
3. Claude Code on the Windows machine, configured with the server URL, can:
   create a test card → see it on the board without a manual reload → move it to
   another status → update its title/description → delete it.
4. Requests without a valid token/key are rejected.

## Design summary (agreed 2026-06-09)

- **Location:** this repo, under `cmd/kanban-mcp/` (same `go.mod`).
- **Language:** Go, using the official `modelcontextprotocol/go-sdk`.
- **Transport:** Streamable HTTP, hosted on Unraid in the docker-compose stack.
- **Auth:** MCP endpoint protected by a bearer token (env var); the server talks to the
  kanban API with the user's `X-API-Key` (env var). Claude Code / Claude Desktop connect
  with custom headers. (claude.ai web connector auth requirements to be verified before
  promising support there.)
- **MCP tools:** `list_cards`, `create_card`, `update_card`, `move_card`, `delete_card`,
  `list_categories`, `list_statuses`.
- **Prerequisite work in `main.go`:** JSON write endpoints (`POST/PATCH/DELETE` under
  `/api/cards`) that reuse existing handler logic and trigger the existing SSE broadcast.
