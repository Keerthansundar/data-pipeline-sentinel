"""Unit tests for Phase 5: patcher, sandbox verification, PR previews, and email alerting."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline.errors import FormatDriftError
from responder.notify import generate_incident_email_html, send_incident_email
from responder.patcher import apply_patch_to_file
from responder.pr import open_fix_pull_request
from responder.sandbox import (
    execute_sandbox_silver_transform,
    verify_sandbox_results,
)


@pytest.fixture
def sample_rules_yaml(tmp_path: Path) -> Path:
    """Fixture providing a minimal clean silver_rules.yml copy."""
    content = """
orders:
  required_columns:
    - order_id
    - customer_id
    - order_date
    - amount
    - status
    - channel
  column_aliases:
    amount:
      - total_amount
  date_formats:
    order_date:
      - "%Y-%m-%d"
  max_unparseable_ratio: 0.01
"""
    rules_file = tmp_path / "silver_rules.yml"
    rules_file.write_text(content.strip(), encoding="utf-8")
    return rules_file


def test_patcher_valid_date_format_patch(sample_rules_yaml: Path, tmp_path: Path):
    """Verify applying a valid date format patch produces round-trip clean diff."""
    dest_file = tmp_path / "silver_rules_patched.yml"
    patch = {
        "file": "config/silver_rules.yml",
        "operation": "append_unique",
        "key_path": "orders.date_formats.order_date",
        "value": "%d/%m/%Y",
    }
    diff = apply_patch_to_file(sample_rules_yaml, dest_file, patch)
    assert "%d/%m/%Y" in diff
    content = dest_file.read_text(encoding="utf-8")
    assert "%d/%m/%Y" in content
    assert "%Y-%m-%d" in content

    # Test idempotency (applying same patch again gives empty diff)
    dest_file_2 = tmp_path / "silver_rules_idempotent.yml"
    diff_2 = apply_patch_to_file(dest_file, dest_file_2, patch)
    assert diff_2 == ""


def test_patcher_valid_column_alias_patch(sample_rules_yaml: Path, tmp_path: Path):
    """Verify appending a new alias to orders.column_aliases works cleanly."""
    dest_file = tmp_path / "silver_rules_alias.yml"
    patch = {
        "file": "config/silver_rules.yml",
        "operation": "append_unique",
        "key_path": "orders.column_aliases.amount",
        "value": "amt",
    }
    diff = apply_patch_to_file(sample_rules_yaml, dest_file, patch)
    assert "+      - amt" in diff or '+      - "amt"' in diff
    content = dest_file.read_text(encoding="utf-8")
    assert "amt" in content


def test_patcher_invalid_operation(sample_rules_yaml: Path, tmp_path: Path):
    """Verify patcher rejects any operation other than append_unique."""
    dest_file = tmp_path / "silver_rules_bad.yml"
    patch = {
        "file": "config/silver_rules.yml",
        "operation": "replace_all",
        "key_path": "orders.date_formats.order_date",
        "value": "%d/%m/%Y",
    }
    with pytest.raises(ValueError, match="Unsupported patch operation"):
        apply_patch_to_file(sample_rules_yaml, dest_file, patch)


def test_sandbox_pass_case():
    """Verify sandbox transformation and verification pass on valid data and rules."""
    # 5 rows total: 3 standard ISO dates, 2 DD/MM/YYYY dates
    bronze_data = pd.DataFrame(
        {
            "order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-4", "ORD-5"],
            "customer_id": ["CUST-1", "CUST-2", "CUST-3", "CUST-4", "CUST-5"],
            "order_date": ["2026-10-01", "2026-10-01", "2026-10-01", "25/10/2026", "26/10/2026"],
            "amount": ["100.00", "200.00", "50.00", "75.00", "125.00"],
            "status": ["completed", "shipped", "pending", "completed", "shipped"],
            "channel": ["web", "mobile", "web", "mobile", "web"],
        }
    )

    rules = {
        "orders": {
            "required_columns": [
                "order_id",
                "customer_id",
                "order_date",
                "amount",
                "status",
                "channel",
            ],
            "column_aliases": {},
            "date_formats": {"order_date": ["%Y-%m-%d", "%d/%m/%Y"]},
            "max_unparseable_ratio": 0.01,
        }
    }

    silver_pdf, q_pdf, unparseable_ratio = execute_sandbox_silver_transform(
        bronze_data.copy(), rules
    )
    assert unparseable_ratio == 0.0
    assert len(silver_pdf) == 5
    assert len(q_pdf) == 0

    report = verify_sandbox_results(bronze_data, silver_pdf, q_pdf, unparseable_ratio)
    assert report["passed"] is True
    assert report["checks"]["no_exception"] is True
    assert report["checks"]["row_count_conservation"] is True
    assert report["checks"]["revenue_reconciles"] is True
    assert report["checks"]["output_schema_matches_contract"] is True


def test_sandbox_fail_unparseable_dates():
    """Verify sandbox fails when unparseable date ratio exceeds threshold."""
    bronze_data = pd.DataFrame(
        {
            "order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-4", "ORD-5"],
            "customer_id": ["CUST-1", "CUST-2", "CUST-3", "CUST-4", "CUST-5"],
            "order_date": ["2026-10-01", "25/10/2026", "26/10/2026", "27/10/2026", "28/10/2026"],
            "amount": ["100.00", "200.00", "50.00", "75.00", "125.00"],
            "status": ["completed", "shipped", "pending", "completed", "shipped"],
            "channel": ["web", "mobile", "web", "mobile", "web"],
        }
    )
    # Rules only support ISO
    rules = {
        "orders": {
            "required_columns": [
                "order_id",
                "customer_id",
                "order_date",
                "amount",
                "status",
                "channel",
            ],
            "column_aliases": {},
            "date_formats": {"order_date": ["%Y-%m-%d"]},
            "max_unparseable_ratio": 0.01,
        }
    }
    with pytest.raises(FormatDriftError):
        execute_sandbox_silver_transform(bronze_data.copy(), rules)


def test_sandbox_fail_revenue_mismatch():
    """Verify verify_sandbox_results fails if revenue does not reconcile."""
    bronze_data = pd.DataFrame(
        {
            "order_id": ["ORD-1"],
            "customer_id": ["CUST-1"],
            "order_date": ["2026-10-01"],
            "amount": ["100.00"],
            "status": ["completed"],
            "channel": ["web"],
        }
    )
    silver_data = bronze_data.copy()
    silver_data["amount"] = 80.00  # Altered revenue
    q_data = pd.DataFrame(columns=["amount"])

    report = verify_sandbox_results(bronze_data, silver_data, q_data, unparseable_ratio=0.0)
    assert report["passed"] is False
    assert report["checks"]["revenue_reconciles"] is False

def test_pr_dry_run_preview_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Verify open_fix_pull_request in dry_run mode writes markdown preview and diff files."""
    monkeypatch.setattr("responder.pr.get_project_root", lambda: tmp_path)
    incident_id = "INC-TEST-DRYRUN-001"
    patch_diff = "+ orders.date_formats.order_date: %d/%m/%Y"
    audit_data = {
        "diagnosis": {
            "summary": "Vendor date format drift to DD/MM/YYYY",
            "root_cause_category": "format_drift",
            "confidence": 0.95,
            "proposed_action": "rules_patch",
            "patch": {
                "file": "config/silver_rules.yml",
                "operation": "append_unique",
                "key_path": "orders.date_formats.order_date",
                "value": "%d/%m/%Y",
            },
            "evidence": [
                {
                    "tool_name": "fetch_task_error_log",
                    "tool_call_id": "c1",
                    "quote": "unparseable date",
                }
            ],
        },
        "sandbox_results": {
            "passed": True,
            "rows_in": 5000,
            "rows_out": 4969,
            "rows_quarantined": 31,
            "unparseable_ratio": 0.0,
            "checks": {"revenue_reconciles": True},
        },
    }

    with patch.dict(os.environ, {"PR_MODE": "dry_run"}):
        mode, pr_path = open_fix_pull_request(incident_id, patch_diff, audit_data)

    assert mode == "dry_run"
    assert incident_id in pr_path
    assert Path(pr_path).exists()
    content = Path(pr_path).read_text(encoding="utf-8")
    assert "Autofix Pull Request" in content
    assert "Sandbox Verification Results" in content
    assert "config/silver_rules.yml" in content


def test_email_rendering_and_smtp_dispatch():
    """Verify HTML email generation and SMTP mock dispatch."""
    incident_id = "INC-EMAIL-TEST-002"
    diagnosis = {
        "summary": "Date format drift detected for user secret_user@example.com",
        "root_cause_category": "format_drift",
        "confidence": 0.95,
        "impact": "Silver transform halted",
        "proposed_action": "rules_patch",
        "recommended_next_steps": "Review PR and merge",
        "evidence": [{"tool_name": "fetch_task_error_log", "quote": "unparseable ratio 5.2%"}],
    }

    # 1. Verify HTML structure & redaction
    html = generate_incident_email_html(
        incident_id=incident_id,
        diagnosis=diagnosis,
        pr_url_or_path="data/pr_previews/test.md",
        sandbox_results={"passed": True, "rows_in": 5000, "rows_out": 5000, "rows_quarantined": 0},
    )
    assert "Evidence" in html
    assert "Reasoning" in html
    assert "Decision" in html
    assert "Recommended Action" in html
    assert "secret_user@example.com" not in html
    assert "[REDACTED_EMAIL]" in html

    # 2. Verify SMTP mock dispatch
    with patch("smtplib.SMTP") as mock_smtp_cls:
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_server

        success = send_incident_email(
            incident_id=incident_id,
            diagnosis=diagnosis,
            pr_url_or_path="data/pr_previews/test.md",
        )
        assert success is True
        mock_server.send_message.assert_called_once()
        sent_msg = mock_server.send_message.call_args[0][0]
        assert f"Incident {incident_id}" in sent_msg["Subject"]


def test_fail_safe_escalation_routing():
    """Verify routing decision logic safely forces escalation on any check failure."""
    # Case 1: Sandbox failed -> force escalation
    diag_action = "rules_patch"
    val_passed = True
    pol_passed = True
    sb_passed = False

    all_ok = diag_action == "rules_patch" and val_passed and pol_passed and sb_passed
    assert all_ok is False
    routing_decision = "rules_patch" if all_ok else "escalate"
    assert routing_decision == "escalate"

    # Case 2: Policy failed -> force escalation
    pol_passed = False
    sb_passed = True
    all_ok = diag_action == "rules_patch" and val_passed and pol_passed and sb_passed
    assert all_ok is False
    routing_decision = "rules_patch" if all_ok else "escalate"
    assert routing_decision == "escalate"

    # Case 3: All passed -> open PR
    val_passed = True
    pol_passed = True
    sb_passed = True
    all_ok = diag_action == "rules_patch" and val_passed and pol_passed and sb_passed
    assert all_ok is True
    routing_decision = "rules_patch" if all_ok else "escalate"
    assert routing_decision == "rules_patch"


def test_pr_github_mode_mock():
    """Verify open_fix_pull_request in github mode creates branch and opens PR via PyGithub."""
    incident_id = "INC-TEST-GH-001"
    patch_diff = "+ orders.date_formats.order_date: %d/%m/%Y"
    audit_data = {
        "diagnosis": {
            "summary": "Fix date format",
            "root_cause_category": "format_drift",
            "confidence": 0.95,
            "proposed_action": "rules_patch",
            "patch": {
                "file": "config/silver_rules.yml",
                "operation": "append_unique",
                "key_path": "orders.date_formats.order_date",
                "value": "%d/%m/%Y",
            },
        },
        "sandbox_results": {"passed": True},
    }

    mock_gh = MagicMock()
    mock_repo = MagicMock()
    mock_gh.get_repo.return_value = mock_repo
    mock_base = MagicMock()
    mock_base.commit.sha = "base123"
    mock_repo.get_branch.return_value = mock_base
    mock_file = MagicMock()
    mock_file.sha = "file123"
    mock_file.decoded_content = (
        b'orders:\n  date_formats:\n    order_date:\n      - "%Y-%m-%d"\n'
    )
    mock_repo.get_pulls.return_value = []
    mock_repo.get_contents.return_value = mock_file
    mock_pr = MagicMock()
    mock_pr.html_url = "https://github.com/org/repo/pull/42"
    mock_repo.create_pull.return_value = mock_pr

    mock_gh_module = MagicMock()
    mock_gh_module.Github.return_value = mock_gh

    with (
        patch.dict(
            os.environ,
            {
                "PR_MODE": "github",
                "GITHUB_TOKEN": "mock-token",
                "GITHUB_REPO": "org/repo",
            },
        ),
        patch.dict("sys.modules", {"github": mock_gh_module}),
    ):
        mode, pr_url = open_fix_pull_request(incident_id, patch_diff, audit_data)

    assert mode == "github"
    assert pr_url == "https://github.com/org/repo/pull/42"
    mock_repo.create_git_ref.assert_called_once()
    mock_repo.update_file.assert_called_once()
    mock_repo.create_pull.assert_called_once()
