# Jira MCP Server

A minimal [MCP](https://modelcontextprotocol.io/) server exposing Jira Cloud
operations as tools, so Claude Code (or any MCP client) can read and create
tasks on a Jira board.

## Tools

| Tool | What it does |
|---|---|
| `get_board` | Board info: name, type, linked project |
| `list_board_issues` | Issues on a board, optional JQL filter (`status = "In Progress"` etc.) |
| `get_issue` | Full details of one issue, including description |
| `create_issue` | Create a Task/Bug/Story in the board's project |
| `transition_issue` | Move an issue to another status (lists valid transitions on mismatch) |
| `add_comment` | Add a plain-text comment to an issue |

All board-scoped tools accept an explicit `board_id` and fall back to the
`JIRA_BOARD_ID` environment variable.

## Prerequisites

- Python 3.10+
- A Jira Cloud API token: create one at
  <https://id.atlassian.com/manage-profile/security/api-tokens>
- Your board id — visible in the board URL:
  `https://your-domain.atlassian.net/jira/software/projects/XXX/boards/<BOARD_ID>`

## Setup

```bash
cd jira_mcp
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Register in Claude Code

```bash
claude mcp add jira \
  --env JIRA_URL=https://your-domain.atlassian.net \
  --env JIRA_EMAIL=you@example.com \
  --env JIRA_API_TOKEN=<token> \
  --env JIRA_BOARD_ID=<board id> \
  -- /absolute/path/to/jira_mcp/.venv/bin/python /absolute/path/to/jira_mcp/server.py
```

By default this registers the server for you in the current project
(`local` scope). Add `--scope user` to make it available in every project,
or `--scope project` to share it via a checked-in `.mcp.json` (do **not**
use `project` scope here — the API token would land in git).

Verify with `/mcp` inside Claude Code, then just ask, e.g.
*"list open tasks on my Jira board"* or
*"create a task: fix silver_sales dedup"*.

## Local testing (MCP Inspector)

```bash
export $(grep -v '^#' .env | xargs)   # after copying .env.example to .env
.venv/bin/mcp dev server.py
```

This opens the MCP Inspector in a browser, where you can call each tool by
hand before wiring it into Claude Code.

## Notes

- Auth is HTTP Basic (email + API token) — Jira Cloud only. Jira
  Server/Data Center would need a bearer token instead.
- Descriptions and comments are sent as plain text wrapped in ADF
  (Atlassian Document Format), which API v3 requires.
- The server is stateless; every tool call is one or two REST requests.
