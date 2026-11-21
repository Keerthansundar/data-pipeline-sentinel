"""Pull request creation via PyGithub or local preview generation."""

import os
import re
from typing import Any

from pipeline.config import get_project_root
from pipeline.logging_config import get_logger

logger = get_logger(__name__)


def generate_pr_body(incident_id: str, patch_diff: str, audit_data: dict[str, Any]) -> str:
    """Format structured markdown audit for PR body or preview file.

    Args:
        incident_id: Target incident ID.
        patch_diff: Unified diff string of the proposed rules change.
        audit_data: Full audit dictionary containing diagnosis and sandbox results.

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
            f"- **Target File:** `{patch.get('file', 'config/silver_rules.yml')}`",
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
            "- **Human-in-the-loop Guardrail:** This PR cannot be automatically merged by the agent.",
            "- Once reviewed and merged, rerun the failed pipeline batch with: `make rerun BATCH=<date>`",
            f"- To simulate human merge locally in dry-run mode, run: `make apply-fix INCIDENT={incident_id}`",
            "",
            "---",
            "*Generated deterministically by PipelineSentinel Incident Responder.*",
        ]
    )

    return "\n".join(lines)


def open_fix_pull_request(
    incident_id: str,
    patch_diff: str,
    audit_data: dict[str, Any],
) -> tuple[str, str]:
    """Open a GitHub PR or write local PR preview file based on PR_MODE.

    Args:
        incident_id: Target incident identifier.
        patch_diff: Unified diff of the proposed change.
        audit_data: Full investigation audit dictionary.

    Returns:
        Tuple of (pr_mode, url_or_path).
    """
    pr_mode = os.getenv("PR_MODE", "dry_run").lower()
    root = get_project_root()
    pr_body = generate_pr_body(incident_id, patch_diff, audit_data)

    # 1. Dry run mode (default)
    if pr_mode == "dry_run":
        preview_dir = root / "data" / "pr_previews"
        preview_dir.mkdir(parents=True, exist_ok=True)

        md_path = preview_dir / f"{incident_id}.md"
        diff_path = preview_dir / f"{incident_id}.diff"

        with open(md_path, "w", encoding="utf-8") as f:
            f.write(pr_body)

        with open(diff_path, "w", encoding="utf-8") as f:
            f.write(patch_diff)

        logger.info("PR dry run preview written to %s", md_path)
        return "dry_run", str(md_path)

    # 2. GitHub Mode
    elif pr_mode == "github":
        token = os.getenv("GITHUB_TOKEN")
        repo_name = os.getenv("GITHUB_REPO")
        base_branch = os.getenv("GITHUB_BASE_BRANCH", "main")

        if not token or not repo_name:
            logger.warning(
                "GITHUB_TOKEN or GITHUB_REPO missing. Falling back to dry_run preview for incident %s",
                incident_id,
            )
            return open_fix_pull_request(
                incident_id, patch_diff, audit_data
            )  # Recursively fallback to dry_run

        from github import Auth, Github

        auth = Auth.Token(token)
        gh = Github(auth=auth)

        try:
            repo = gh.get_repo(repo_name)
            base_ref = repo.get_branch(base_branch)

            slug = re.sub(r"[^a-zA-Z0-9_-]", "-", incident_id).lower()[:30]
            branch_name = f"autofix/{incident_id}-{slug}"

            # Create branch from base
            repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=base_ref.commit.sha)

            # Get current rules file on branch and commit updated rules
            rules_path = "config/silver_rules.yml"
            current_file = repo.get_contents(rules_path, ref=branch_name)

            # Read patched rules from sandbox copy
            sandbox_patched = root / "data" / "sandbox" / incident_id / "silver_rules_patched.yml"
            if sandbox_patched.exists():
                with open(sandbox_patched, encoding="utf-8") as f:
                    new_content = f.read()
            else:
                local_rules = root / "config" / "silver_rules.yml"
                with open(local_rules, encoding="utf-8") as f:
                    new_content = f.read()

            repo.update_file(
                path=rules_path,
                message=f"fix(rules): Auto-fix for incident {incident_id}",
                content=new_content,
                sha=current_file.sha,
                branch=branch_name,
            )

            # Open PR
            pr = repo.create_pull(
                title=f"fix(rules): Auto-fix for incident {incident_id}",
                body=pr_body,
                head=branch_name,
                base=base_branch,
            )

            logger.info("Successfully opened GitHub PR: %s", pr.html_url)
            return "github", str(pr.html_url)

        except Exception as exc:
            logger.error("Failed to open GitHub PR: %s. Falling back to dry_run preview.", exc)
            return open_fix_pull_request(incident_id, patch_diff, audit_data)

    else:
        logger.warning("Unrecognized PR_MODE '%s'. Defaulting to dry_run.", pr_mode)
        return open_fix_pull_request(incident_id, patch_diff, audit_data)
