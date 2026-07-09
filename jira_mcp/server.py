"""Jira MCP server.

Exposes Jira Cloud operations (board issues, issue creation, transitions,
comments) as MCP tools over stdio, for use from Claude Code or any other
MCP client.

Configuration (environment variables):
    JIRA_URL        e.g. https://your-domain.atlassian.net
    JIRA_EMAIL      Atlassian account email
    JIRA_API_TOKEN  API token from https://id.atlassian.com/manage-profile/security/api-tokens
    JIRA_BOARD_ID   optional default board id (tools accept an explicit one too)
"""

import os
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("jira")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _config() -> tuple[str, str, str]:
    url = os.environ.get("JIRA_URL", "").rstrip("/")
    email = os.environ.get("JIRA_EMAIL", "")
    token = os.environ.get("JIRA_API_TOKEN", "")
    missing = [
        name
        for name, value in (
            ("JIRA_URL", url),
            ("JIRA_EMAIL", email),
            ("JIRA_API_TOKEN", token),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing environment variables: {', '.join(missing)}. "
            "Set them when registering the server (claude mcp add --env ...)."
        )
    return url, email, token


def _request(method: str, path: str, **kwargs: Any) -> Any:
    url, email, token = _config()
    response = httpx.request(
        method,
        f"{url}{path}",
        auth=(email, token),
        headers={"Accept": "application/json"},
        timeout=30.0,
        **kwargs,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"Jira API error {response.status_code} for {method} {path}: "
            f"{response.text[:500]}"
        )
    if response.status_code == 204 or not response.content:
        return {}
    return response.json()


def _resolve_board_id(board_id: int | None) -> int:
    if board_id is not None:
        return board_id
    env_value = os.environ.get("JIRA_BOARD_ID", "")
    if not env_value:
        raise RuntimeError(
            "No board_id given and JIRA_BOARD_ID is not set. "
            "Pass board_id explicitly or configure the env variable."
        )
    return int(env_value)


def _adf(text: str) -> dict[str, Any]:
    """Wrap plain text in Atlassian Document Format (required by API v3)."""
    paragraphs = [
        {"type": "paragraph", "content": [{"type": "text", "text": line}]}
        if line
        else {"type": "paragraph", "content": []}
        for line in text.split("\n")
    ]
    return {"type": "doc", "version": 1, "content": paragraphs}


def _adf_to_text(node: Any) -> str:
    """Best-effort extraction of plain text from an ADF document."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "".join(_adf_to_text(item) for item in node)
    if isinstance(node, dict):
        if node.get("type") == "text":
            return node.get("text", "")
        text = _adf_to_text(node.get("content", []))
        if node.get("type") in ("paragraph", "heading", "listItem"):
            text += "\n"
        return text
    return ""


def _issue_summary(issue: dict[str, Any]) -> dict[str, Any]:
    fields = issue.get("fields", {})
    assignee = fields.get("assignee") or {}
    return {
        "key": issue.get("key"),
        "summary": fields.get("summary"),
        "status": (fields.get("status") or {}).get("name"),
        "type": (fields.get("issuetype") or {}).get("name"),
        "priority": (fields.get("priority") or {}).get("name"),
        "assignee": assignee.get("displayName"),
    }


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def get_board(board_id: int | None = None) -> dict[str, Any]:
    """Get basic info about a Jira board (name, type, linked project).

    Args:
        board_id: Board id. Falls back to the JIRA_BOARD_ID env variable.
    """
    resolved = _resolve_board_id(board_id)
    board = _request("GET", f"/rest/agile/1.0/board/{resolved}")
    location = board.get("location") or {}
    return {
        "id": board.get("id"),
        "name": board.get("name"),
        "type": board.get("type"),
        "project_key": location.get("projectKey"),
        "project_name": location.get("projectName"),
    }


@mcp.tool()
def list_board_issues(
    board_id: int | None = None,
    jql: str = "",
    max_results: int = 25,
) -> list[dict[str, Any]]:
    """List issues on a Jira board, optionally filtered by JQL.

    Args:
        board_id: Board id. Falls back to the JIRA_BOARD_ID env variable.
        jql: Extra JQL filter, e.g. 'status = "In Progress"' or
            'assignee = currentUser() AND status != Done'.
        max_results: Maximum number of issues to return (default 25).
    """
    resolved = _resolve_board_id(board_id)
    params: dict[str, Any] = {
        "maxResults": max_results,
        "fields": "summary,status,issuetype,priority,assignee",
    }
    if jql:
        params["jql"] = jql
    data = _request("GET", f"/rest/agile/1.0/board/{resolved}/issue", params=params)
    return [_issue_summary(issue) for issue in data.get("issues", [])]


@mcp.tool()
def get_issue(issue_key: str) -> dict[str, Any]:
    """Get full details of a single issue, including its description.

    Args:
        issue_key: Issue key, e.g. 'PROJ-123'.
    """
    issue = _request(
        "GET",
        f"/rest/api/3/issue/{issue_key}",
        params={
            "fields": "summary,status,issuetype,priority,assignee,reporter,"
            "description,labels,created,updated,duedate",
        },
    )
    fields = issue.get("fields", {})
    result = _issue_summary(issue)
    result.update(
        {
            "reporter": (fields.get("reporter") or {}).get("displayName"),
            "labels": fields.get("labels", []),
            "created": fields.get("created"),
            "updated": fields.get("updated"),
            "due_date": fields.get("duedate"),
            "description": _adf_to_text(fields.get("description")).strip(),
        }
    )
    return result


@mcp.tool()
def create_issue(
    summary: str,
    description: str = "",
    issue_type: str = "Task",
    board_id: int | None = None,
) -> dict[str, Any]:
    """Create a new issue in the project linked to a Jira board.

    Args:
        summary: Issue title.
        description: Plain-text issue description (optional).
        issue_type: Issue type name, e.g. 'Task', 'Bug', 'Story' (default 'Task').
        board_id: Board whose project the issue goes to. Falls back to JIRA_BOARD_ID.
    """
    board = get_board(board_id)
    project_key = board.get("project_key")
    if not project_key:
        raise RuntimeError(
            f"Board {board.get('id')} has no linked project; "
            "cannot determine where to create the issue."
        )
    fields: dict[str, Any] = {
        "project": {"key": project_key},
        "summary": summary,
        "issuetype": {"name": issue_type},
    }
    if description:
        fields["description"] = _adf(description)
    created = _request("POST", "/rest/api/3/issue", json={"fields": fields})
    url = os.environ.get("JIRA_URL", "").rstrip("/")
    return {
        "key": created.get("key"),
        "id": created.get("id"),
        "url": f"{url}/browse/{created.get('key')}",
    }


@mcp.tool()
def transition_issue(issue_key: str, transition: str) -> dict[str, Any]:
    """Move an issue to another status (e.g. 'In Progress', 'Done').

    If the given transition name does not match, the available transitions
    are returned instead so you can pick the right one.

    Args:
        issue_key: Issue key, e.g. 'PROJ-123'.
        transition: Target transition name (case-insensitive).
    """
    data = _request("GET", f"/rest/api/3/issue/{issue_key}/transitions")
    transitions = data.get("transitions", [])
    match = next(
        (t for t in transitions if t.get("name", "").lower() == transition.lower()),
        None,
    )
    if match is None:
        return {
            "error": f"No transition named '{transition}' for {issue_key}.",
            "available_transitions": [t.get("name") for t in transitions],
        }
    _request(
        "POST",
        f"/rest/api/3/issue/{issue_key}/transitions",
        json={"transition": {"id": match["id"]}},
    )
    return {"key": issue_key, "transitioned_to": match.get("name")}


@mcp.tool()
def add_comment(issue_key: str, body: str) -> dict[str, Any]:
    """Add a plain-text comment to an issue.

    Args:
        issue_key: Issue key, e.g. 'PROJ-123'.
        body: Comment text.
    """
    created = _request(
        "POST",
        f"/rest/api/3/issue/{issue_key}/comment",
        json={"body": _adf(body)},
    )
    return {"key": issue_key, "comment_id": created.get("id")}


if __name__ == "__main__":
    mcp.run(transport="stdio")
