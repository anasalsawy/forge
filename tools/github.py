"""Custom GitHub tool for the Forge flow.

Declared in flow_direct.json as ``custom:github``. Full repository access
(read/write contents, branches, pull requests, issues) against the user's
GitHub account.

The token is read lazily at call time from ``GITHUB_TOKEN`` (fallback
``GITHUB_PERSONAL_ACCESS_TOKEN``) in the runner environment — it is never
stored in this file or the flow JSON.
"""

import base64
import json
import os

from crewai.tools import BaseTool
from pydantic import BaseModel, Field
import requests

API = "https://api.github.com"


class GitHubToolArgs(BaseModel):
    action: str = Field(
        ...,
        description=(
            "Operation to perform. One of: user, repos, create_repo, contents, "
            "tree, branches, create_branch, create_file, update_file, "
            "delete_file, create_pr, get_pr, merge_pr, create_issue, comment, "
            "commit_status."
        ),
    )
    repo: str = Field(
        "anasalsawy/cua",
        description="Repository as owner/name. Default: anasalsawy/cua.",
    )
    path: str | None = Field(
        None, description="Repository-relative file path for file actions."
    )
    content: str | None = Field(
        None, description="File content for create_file/update_file."
    )
    message: str | None = Field(None, description="Git commit message.")
    branch: str | None = Field(
        None, description="Branch to operate on. Default: main."
    )
    base: str | None = Field(None, description="Base branch for PRs.")
    head: str | None = Field(None, description="Head branch for PRs.")
    title: str | None = Field(None, description="PR or issue title.")
    body: str | None = Field(None, description="PR or issue body.")
    ref: str | None = Field(
        None, description="Full ref name to create, e.g. refs/heads/feature-x."
    )
    sha: str | None = Field(None, description="Commit SHA (update_file, branch).")
    number: int | None = Field(None, description="PR or issue number.")
    name: str | None = Field(None, description="Repo name for create_repo.")
    description: str | None = Field(None, description="Repo/PR description.")
    private: bool = Field(False, description="Private repo for create_repo.")


class GitHubTool(BaseTool):
    name: str = "GitHubTool"
    description: str = (
        "Full access to the user's GitHub account: read and write repository "
        "contents, create branches, open and merge pull requests, create "
        "issues and comments. Default repository is anasalsawy/cua. Requires "
        "the GITHUB_TOKEN environment variable. Use 'contents' to read a "
        "file, 'create_file' or 'update_file' to commit content to a branch, "
        "and 'create_pr'/'merge_pr' to land changes."
    )
    args_schema: type[BaseModel] = GitHubToolArgs

    def _headers(self) -> dict:
        token = os.getenv("GITHUB_TOKEN") or os.getenv(
            "GITHUB_PERSONAL_ACCESS_TOKEN"
        )
        if not token:
            raise RuntimeError(
                "No GitHub token found in environment. Set GITHUB_TOKEN "
                "(or GITHUB_PERSONAL_ACCESS_TOKEN) in the runner environment."
            )
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _req(self, method: str, url: str, **kwargs) -> dict:
        resp = requests.request(method, url, headers=self._headers(), **kwargs)
        if resp.status_code >= 400:
            raise RuntimeError(
                f"GitHub API {method} {url} -> {resp.status_code}: "
                f"{resp.text[:500]}"
            )
        if not resp.content:
            return {"status": resp.status_code}
        return resp.json()

    def _b64(self, s: str) -> str:
        return base64.b64encode(s.encode()).decode()

    def _default_branch(self, repo: str) -> str:
        data = self._req("GET", f"{API}/repos/{repo}")
        return data.get("default_branch", "main")

    def _run(self, args: GitHubToolArgs) -> str:
        try:
            result = self._execute(args)
            return json.dumps({"ok": True, "result": result})
        except Exception as exc:  # noqa: BLE001 - surface to the agent as text
            return json.dumps({"ok": False, "error": str(exc)})

    def _execute(self, a: GitHubToolArgs) -> dict:
        repo, token = a.repo, None
        branch = a.branch or self._default_branch(repo)
        action = a.action.lower()

        if action == "user":
            return self._req("GET", f"{API}/user")
        if action == "repos":
            return self._req("GET", f"{API}/user/repos?per_page=100&sort=updated")
        if action == "create_repo":
            return self._req(
                "POST",
                f"{API}/user/repos",
                json={
                    "name": a.name,
                    "description": a.description,
                    "private": a.private,
                    "auto_init": True,
                },
            )
        if action == "contents":
            return self._req(
                "GET", f"{API}/repos/{repo}/contents/{a.path}", params={"ref": branch}
            )
        if action == "tree":
            sha = a.sha or branch
            return self._req(
                "GET",
                f"{API}/repos/{repo}/git/trees/{sha}",
                params={"recursive": "1"},
            )
        if action == "branches":
            return self._req("GET", f"{API}/repos/{repo}/branches?per_page=100")
        if action == "create_branch":
            head = self._req("GET", f"{API}/repos/{repo}/branches/{branch}")
            sha = a.sha or head["commit"]["sha"]
            ref = a.ref or f"refs/heads/{a.name}"
            return self._req(
                "POST", f"{API}/repos/{repo}/git/refs", json={"ref": ref, "sha": sha}
            )
        if action in ("create_file", "update_file"):
            payload = {
                "message": a.message or f"{action} {a.path} via flow",
                "content": self._b64(a.content or ""),
                "branch": branch,
            }
            if action == "update_file" and a.sha:
                payload["sha"] = a.sha
            return self._req(
                "PUT", f"{API}/repos/{repo}/contents/{a.path}", json=payload
            )
        if action == "delete_file":
            payload = {"message": a.message or f"delete {a.path}", "branch": branch}
            if a.sha:
                payload["sha"] = a.sha
            return self._req(
                "DELETE", f"{API}/repos/{repo}/contents/{a.path}", json=payload
            )
        if action == "create_pr":
            return self._req(
                "POST",
                f"{API}/repos/{repo}/pulls",
                json={
                    "title": a.title,
                    "head": a.head,
                    "base": a.base or "main",
                    "body": a.body,
                },
            )
        if action == "get_pr":
            return self._req("GET", f"{API}/repos/{repo}/pulls/{a.number}")
        if action == "merge_pr":
            return self._req(
                "PUT", f"{API}/repos/{repo}/pulls/{a.number}/merge", json={}
            )
        if action == "create_issue":
            return self._req(
                "POST",
                f"{API}/repos/{repo}/issues",
                json={"title": a.title, "body": a.body},
            )
        if action == "comment":
            return self._req(
                "POST",
                f"{API}/repos/{repo}/issues/{a.number}/comments",
                json={"body": a.body},
            )
        if action == "commit_status":
            return self._req(
                "POST",
                f"{API}/repos/{repo}/statuses/{a.sha}",
                json={
                    "state": a.body or "success",
                    "context": a.title or "flow",
                    "description": a.description,
                },
            )
        raise ValueError(
            f"Unknown action '{action}'. Supported: user, repos, create_repo, "
            "contents, tree, branches, create_branch, create_file, update_file, "
            "delete_file, create_pr, get_pr, merge_pr, create_issue, comment, "
            "commit_status."
        )