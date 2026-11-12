# Project Charter: Kanban MCP Server

**Date:** 2026-06-09
**Status:** COMPLETE — deployed and verified 2026-06-09. MCP server live at
http://192.168.1.25:17809/ (Unraid container `kanban-mcp`, image
ghcr.io/teejs/golang-kanban-mcp:latest). All §5 criteria passed: JSON endpoints
authenticated by X-API-Key; create/move/update/delete verified through MCP tools
with live SSE board updates and 401 on bad/missing token. Registered in Claude
Code user config (`claude mcp list` → kanban ✓ Connected). Status slugs are
unhyphenated (e.g. `inprogress`, `needsfeedback`).

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

---

# Project Charter: Freshservice 429 fix (Unassigned view)

**Date:** 2026-11-12
**Status:** APPROVED 2026-11-12. Code written in `main.go`, not yet built by CI or deployed.
Differences from the proposal: a `Retry-After` longer than 15 s is not waited out (the refresh
stops and last-known tasks are shown), cards are now listed in ticket order instead of
completion order, and a failed refresh is retried at most once per 60 s.

**Problem:** `/unassigned/fragment` ([main.go](main.go) `unassignedFragmentHandler`) makes
2 ticket searches plus one `/tickets/{id}/tasks` call per open ticket (about 70) on every
browser load, every refresh interval (default 300 s) and every row-open, with no cache. The
Freshservice limit is per minute for the whole account, so bursts get HTTP 429. Each 429
silently drops that ticket's tasks from the view and writes one log line (about 70 per burst).

## 1. What is the one thing this must do?

The Unassigned view keeps showing every unassigned Freshservice task, even when Freshservice
returns 429.

## 2. What would be wrong if we shipped "working" software without it?

The log gets quiet but tasks still vanish from the kiosk during bursts, or errors are hidden
instead of the cause being fixed.

## 3. What is explicitly off-limits as a workaround?

- Muting or removing the log lines.
- Only lowering concurrency or adding sleeps (the limit is calls per minute, not call rate).
- Telling the user to raise the refresh-interval setting by hand.
- Removing or shrinking the task lookup.

## 4. Deployment target and backup location

- **Deployment:** GitHub Actions builds the image, Unraid runs the container on `:latest`.
  No local Go build, no remote access to the Unraid host without explicit permission.
- **Backups:** git (branch + PR) covers it; no separate file backups.

## 5. How will we verify it is done?

1. CI build is green.
2. After deploy, load the board and the kiosk in two browsers at once: logs show one
   Freshservice summary line and zero 429 lines, and the unassigned tasks are present.
3. The summary line shows Freshservice's remaining/total rate-limit numbers (reveals the real limit).
4. Reload again within 60 s: no new Freshservice calls. After 60 s: exactly one refresh.

## Design

- Cache the finished card list for 60 s; concurrent requests wait on one in-flight refresh
  (stdlib mutex, no new dependency).
- On 429: wait `Retry-After`, retry that call once; if it 429s again, stop calling Freshservice
  for this refresh.
- For any ticket not refreshed, keep its last-known tasks.
- Replace per-ticket error lines with one summary line per refresh.
- Not touched: helpdesk column, refresh-interval setting, what the view displays.
