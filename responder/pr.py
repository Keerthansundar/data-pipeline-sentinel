"""Pull request creation via PyGithub or local preview generation."""

import os
import tempfile
from pathlib import Path
from typing import Any

from pipeline.config import get_project_root
from pipeline.logging_config import get_logger
from responder.redact import redact_text

logger = get_logger(__name__)

RULES_PATH = "config/silver_rules.yml"

RERUN_LINE = '- Then rerun the failed batch (replace `<date>`): `docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf \'{\\"batch_date\\": \\"<date>\\"}\'`'


def generate_pr_body(
    incident_id: str,
    patch_diff: str,
    audit_data: dict[str, Any],
    mode: str = "dry_run",
) -> str:
    """Format structured markdown audit for PR body or preview file.

    Args:
        incident_id: Target incident ID.
        patch_diff: Unified diff string of the proposed rules change.
        audit_data: Full audit dictionary containing diagnosis and sandbox results.
        mode: "dry_run" (local preview) or "github" (real pull request).

    Returns:
        Structured Markdown audit string.
    """
    diagnosis = audit_data.get("diagnosis", {})
    sandbox = audit_data.get("sandbox_results", {})
    summary = diagnosis.get("summary", "Automated rules patch proposed by PipelineSentinel.")
    category = diagnosis.get("root_cause_category", "unknown")
    confidence = diagnosis.get("confidence", 0.0)
    patch = diagnosis.get("patch", {})
    evidence = diagnosis.get("evidence", [])

    if mode == "github":
        next_steps = [
            "- **Human-in-the-loop Guardrail:** The agent can open this PR but cannot merge it. A human must review the diff and merge.",
            "- After merging, run `git pull` in the project folder so `config/silver_rules.yml` is updated.",
            RERUN_LINE,
        ]
    else:
        next_steps = [
            "- **Human-in-the-loop Guardrail:** This PR cannot be automatically merged by the agent.",
            '- Once reviewed and merged, rerun the failed batch (replace `<date>`): `docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf \'{\\"batch_date\\": \\"<date>\\"}\'`',
            f"- To simulate the human merge locally in dry-run mode, run: `docker compose exec airflow-scheduler python -m responder.patcher --incident-id {incident_id}`",
        ]

    lines = [
        f"# Autofix Pull Request: Incident `{incident_id}`",
        "",
        "## 1. Incident Overview",
        f"- **Incident ID:** `{incident_id}`",
        f"- **Root Cause Category:** `{category}`",
        f"- **Agent Confidence:** `{confidence:.2f}`",
        f"- **Summary:** {summary}",
        "",
        "## 2. Evidence Cited",
    ]

    if evidence:
        for ev in evidence:
            lines.append(f"- **[{ev.get('tool_name')}]** (Call ID: `{ev.get('tool_call_id')}`):")
            lines.append(f'  > "{ev.get("quote")}"')
    else:
        lines.append("- No explicit evidence quotes cited.")

    lines.extend(
        [
            "",
            "## 3. Proposed Patch (`config/silver_rules.yml`)",
            f"- **Target File:** `{patch.get('file', RULES_PATH)}`",
            f"- **Operation:** `{patch.get('operation', 'append_unique')}`",
            f"- **Key Path:** `{patch.get('key_path', '')}`",
            f"- **Value:** `{patch.get('value', '')}`",
            "",
            "```diff",
            patch_diff.strip() if patch_diff else "# No diff generated",
            "```",
            "",
            "## 4. Sandbox Verification Results",
            f"- **Status:** `{'PASSED' if sandbox.get('passed') else 'FAILED'}`",
            f"- **Rows In:** `{sandbox.get('rows_in', 'N/A')}`",
            f"- **Rows Out (Silver):** `{sandbox.get('rows_out', 'N/A')}`",
            f"- **Rows Quarantined:** `{sandbox.get('rows_quarantined', 'N/A')}`",
            f"- **Unparseable Date Ratio:** `{sandbox.get('unparseable_ratio', 0.0):.4f}`",
            f"- **Revenue Reconciliation:** `{'Reconciled' if sandbox.get('checks', {}).get('revenue_reconciles') else 'Mismatch'}`",
            "",
            "## 5. Merging & Next Steps",
            *next_steps,
            "",
            "---",
            "*Generated deterministically by PipelineSentinel Incident Responder.*",
        ]
    )

    return "\n".join(lines)


def _write_dry_run_preview(incident_id: str, patch_diff: str, pr_body: str) -> tuple[str, str]:
    """Write the local PR preview (.md and .diff) and return ('dry_run', path)."""
    preview_dir = get_project_root() / "data" / "pr_previews"
    preview_dir.mkdir(parents=True, exist_ok=True)

    md_path = preview_dir / f"{incident_id}.md"
    diff_path = preview_dir / f"{incident_id}.diff"

    md_path.write_text(pr_body, encoding="utf-8")
    diff_path.write_text(patch_diff, encoding="utf-8")

    logger.info("PR dry run preview written to %s", md_path)
    return "dry_run", str(md_path)


def _open_github_pr(incident_id: str, audit_data: dict[str, Any]) -> str:
    """Create a branch, commit the patched rules file and open a PR. Returns the PR URL.

    The patch is applied to the rules file as it exists on the base branch on GitHub,
    not to the local copy, so the PR contains only the one proposed change.
    Raises on any failure; the caller falls back to a local preview.
    """
    from github import Auth, Github, GithubException

    from responder.patcher import apply_patch_to_file

    token = os.environ["GITHUB_TOKEN"]
    repo_name = os.environ["GITHUB_REPO"]
    base_branch = os.getenv("GITHUB_BASE_BRANCH", "main")

    patch = (audit_data.get("diagnosis") or {}).get("patch")
    if not patch:
        raise ValueError("No patch found in audit data")

    gh = Github(auth=Auth.Token(token))
    repo = gh.get_repo(repo_name)
    branch_name = f"autofix/{incident_id}"
    head = f"{repo.owner.login}:{branch_name}"

    # Reuse an already open PR for this incident (retries must not create duplicates).
    existing = list(repo.get_pulls(state="open", head=head))
    if existing:
        logger.info("PR already open for %s: %s", incident_id, existing[0].html_url)
        return str(existing[0].html_url)

    base_ref = repo.get_branch(base_branch)
    base_file = repo.get_contents(RULES_PATH, ref=base_branch)
    original = base_file.decoded_content.decode("utf-8")

    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "silver_rules.yml"
        dst = Path(tmp) / "silver_rules_patched.yml"
        src.write_text(original, encoding="utf-8")
        diff = apply_patch_to_file(src, dst, patch)
        new_content = dst.read_text(encoding="utf-8")

    if new_content == original:
        raise ValueError("Patch makes no change to the rules file on the base branch")

    try:
        repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=base_ref.commit.sha)
    except GithubException as exc:
        if exc.status != 422:  # 422 = branch already exists, reuse it
            raise

    branch_file = repo.get_contents(RULES_PATH, ref=branch_name)
    repo.update_file(
        path=RULES_PATH,
        message=f"fix(rules): Auto-fix for incident {incident_id}",
        content=new_content,
        sha=branch_file.sha,
        branch=branch_name,
    )

    body = redact_text(generate_pr_body(incident_id, diff, audit_data, mode="github"))
    pr = repo.create_pull(
        title=f"fix(rules): Auto-fix for incident {incident_id}",
        body=body,
        head=branch_name,
        base=base_branch,
    )
    logger.info("Successfully opened GitHub PR: %s", pr.html_url)
    return str(pr.html_url)


def open_fix_pull_request(
    incident_id: str,
    patch_diff: str,
    audit_data: dict[str, Any],
) -> tuple[str, str]:
    """Open a GitHub PR or write a local PR preview file based on PR_MODE.

    Any GitHub problem (missing credentials, API error) falls back to a local preview.

    Args:
        incident_id: Target incident identifier.
        patch_diff: Unified diff of the proposed change.
        audit_data: Full investigation audit dictionary.

    Returns:
        Tuple of (pr_mode, url_or_path).
    """
    pr_mode = os.getenv("PR_MODE", "dry_run").lower()

    if pr_mode == "github":
        if not os.getenv("GITHUB_TOKEN") or not os.getenv("GITHUB_REPO"):
            logger.warning(
                "GITHUB_TOKEN or GITHUB_REPO missing. Writing dry_run preview for incident %s",
                incident_id,
            )
        else:
            try:
                return "github", _open_github_pr(incident_id, audit_data)
            except Exception as exc:
                logger.error(
                    "Failed to open GitHub PR for %s: %s. Writing dry_run preview instead.",
                    incident_id,
                    redact_text(str(exc)),
                )
    elif pr_mode != "dry_run":
        logger.warning("Unrecognized PR_MODE '%s'. Defaulting to dry_run.", pr_mode)

    pr_body = redact_text(generate_pr_body(incident_id, patch_diff, audit_data))
    return _write_dry_run_preview(incident_id, patch_diff, pr_body)
