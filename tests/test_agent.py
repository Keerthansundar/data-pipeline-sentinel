"""Unit tests for agent tools, LLM wrapper, and autonomous diagnosis loop."""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from responder.agent import diagnose_incident
from responder.llm_client import get_openai_model
from responder.schemas import ProposedAction, RootCauseCategory
from responder.tools import (
    diff_schema_vs_last_good_batch,
    fetch_data_quality_results,
    fetch_run_history,
    fetch_task_error_log,
    inspect_column_profile,
    read_silver_rules,
    set_repository,
    trace_downstream_impact,
)


@pytest.fixture
def mock_repo():
    repo = MagicMock()
    set_repository(repo)
    yield repo
    # Reset
    from responder.repository import OpsRepository

    set_repository(OpsRepository())


def test_tool_fetch_task_error_log(mock_repo):
    """Test fetch_task_error_log returns error details and prior log lines."""
    mock_repo.get_task_error_log.return_value = {
        "incident_id": "INC-001",
        "task_id": "silver_transform",
        "batch_date": "2026-10-01",
        "error_type": "FormatDriftError",
        "error_message": "Date format drift detected for user test@example.com",
        "traceback": "Traceback: line 42 in silver.py",
        "recent_logs": [
            "2026-10-01 INFO Starting Silver",
            "2026-10-01 ERROR Unparseable date ratio 5.2%",
        ],
    }

    res = fetch_task_error_log("INC-001")
    assert res["incident_id"] == "INC-001"
    assert "FormatDriftError" in res["error_type"]
    assert len(res["recent_logs"]) == 2
    # Verify PII was redacted
    assert "test@example.com" not in res["error_message"]
    assert "[REDACTED_EMAIL]" in res["error_message"]


def test_tool_fetch_task_error_log_empty(mock_repo):
    """Test fetch_task_error_log returns clean empty message when no logs exist."""
    mock_repo.get_task_error_log.return_value = {}
    res = fetch_task_error_log("INC-MISSING")
    assert "no data available" in res["status"]


def test_tool_fetch_run_history(mock_repo):
    """Test fetch_run_history returns past task execution metrics."""
    mock_repo.get_run_history.return_value = [
        {
            "dag_run_id": "run_1",
            "task_id": "silver_transform",
            "status": "SUCCESS",
            "rows_in": 5000,
        },
        {"dag_run_id": "run_2", "task_id": "silver_transform", "status": "FAILED", "rows_in": 5000},
    ]
    res = fetch_run_history("silver_transform")
    assert len(res) == 2
    assert res[0]["status"] == "SUCCESS"


def test_tool_fetch_run_history_empty(mock_repo):
    """Test fetch_run_history handles missing history gracefully."""
    mock_repo.get_run_history.return_value = []
    res = fetch_run_history("unknown_task")
    assert "no data available" in res[0]["message"]


def test_tool_inspect_column_profile(mock_repo):
    """Test inspect_column_profile returns profile metrics and diff against prior good batch."""
    mock_repo.get_column_profiles.side_effect = [
        # Current batch
        [
            {
                "column_name": "order_date",
                "total_rows": 5000,
                "null_count": 0,
                "null_rate": 0.0,
                "ambiguous_day_month_share": 0.0,
                "patterns": json.dumps({"slash_format_count": 250}),
            }
        ],
        # Previous good batch
        [
            {
                "column_name": "order_date",
                "total_rows": 5000,
                "null_count": 0,
                "null_rate": 0.0,
            }
        ],
    ]
    mock_repo.get_last_good_batch_date.return_value = "2026-09-30"

    res = inspect_column_profile("2026-10-01")
    assert res["batch_date"] == "2026-10-01"
    assert res["previous_good_batch_date"] == "2026-09-30"
    assert "order_date" in res["columns"]
    assert res["columns"]["order_date"]["patterns"]["slash_format_count"] == 250


def test_tool_diff_schema_vs_last_good_batch(mock_repo):
    """Test diff_schema_vs_last_good_batch detects renamed or added columns."""
    mock_repo.get_schema_snapshot.side_effect = [
        {
            "order_id": "StringType",
            "customer_id": "StringType",
            "total_amount": "StringType",
        },  # Current
        {"order_id": "StringType", "customer_id": "StringType", "amount": "StringType"},  # Prev
    ]
    mock_repo.get_last_good_batch_date.return_value = "2026-09-30"

    diff = diff_schema_vs_last_good_batch("2026-10-01")
    assert "total_amount" in diff["added_columns"]
    assert "amount" in diff["removed_columns"]


def test_tool_fetch_data_quality_results(mock_repo):
    """Test fetch_data_quality_results returns checks."""
    mock_repo.get_dq_results.return_value = [
        {
            "check_name": "max_null_rate:amount",
            "observed_value": "0.2980",
            "threshold_value": "0.0100",
            "passed": False,
        }
    ]
    res = fetch_data_quality_results("2026-10-01")
    assert len(res) == 1
    assert res[0]["passed"] is False


def test_tool_trace_downstream_impact():
    """Test trace_downstream_impact reads lineage from config/lineage.yml."""
    impact = trace_downstream_impact("silver_transform")
    assert "silver.orders" in impact["downstream_tables"]
    assert "Live Run Monitor" in impact["dashboard_pages"]


def test_tool_read_silver_rules():
    """Test read_silver_rules safely returns silver_rules.yml contents."""
    content = read_silver_rules()
    assert "orders:" in content
    assert "required_columns:" in content


def test_openai_model_from_env():
    """Verify OPENAI_MODEL is read from environment and raises if missing."""
    with patch.dict(os.environ, {"OPENAI_MODEL": "gpt-4o-mini"}):
        assert get_openai_model() == "gpt-4o-mini"

    with patch.dict(os.environ, {}, clear=True):
        if "OPENAI_MODEL" in os.environ:
            del os.environ["OPENAI_MODEL"]
        with pytest.raises(ValueError, match="OPENAI_MODEL"):
            get_openai_model()


def test_agent_full_diagnosis_loop_date_format():
    """Test autonomous agent loop with a multi-turn mocked LLM client."""
    fake_repo = MagicMock()
    fake_repo.get_incident.return_value = {
        "id": "INC-2026-10-01-silver_transform-abcd1234",
        "task_id": "silver_transform",
        "batch_date": "2026-10-01",
        "error_type": "FormatDriftError",
        "error_message": "unparseable date ratio 5.20% exceeds max 1.00% (column=order_date, accepted_formats=['%Y-%m-%d'])",
    }
    fake_repo.get_task_error_log.return_value = {
        "incident_id": "INC-2026-10-01-silver_transform-abcd1234",
        "task_id": "silver_transform",
        "batch_date": "2026-10-01",
        "error_type": "FormatDriftError",
        "error_message": "unparseable date ratio 5.20% exceeds max 1.00%",
        "traceback": "",
        "recent_logs": [
            "2026-10-01 INFO Starting Silver transform",
            "2026-10-01 ERROR unparseable date ratio 5.20% exceeds max 1.00% with sample values ['25/10/2026']",
        ],
    }
    fake_repo.get_column_profiles.return_value = [
        {"column_name": "order_date", "ambiguous_day_month_share": 0.0}
    ]

    set_repository(fake_repo)

    # Mock LLM Client turn 1: Call fetch_task_error_log
    # Mock LLM Client turn 2: Call submit_incident_diagnosis
    call_1 = MagicMock(
        id="call_err_log",
        function=MagicMock(
            name="fetch_task_error_log",
            arguments=json.dumps({"incident_id": "INC-2026-10-01-silver_transform-abcd1234"}),
        ),
    )
    call_1.function.name = "fetch_task_error_log"
    call_1.function.arguments = json.dumps(
        {"incident_id": "INC-2026-10-01-silver_transform-abcd1234"}
    )

    call_2 = MagicMock(
        id="call_submit",
        function=MagicMock(
            name="submit_incident_diagnosis",
            arguments=json.dumps(
                {
                    "root_cause_category": "format_drift",
                    "confidence": 0.95,
                    "summary": "Vendor introduced DD/MM/YYYY date formats into order_date",
                    "impact": "Silver transformation halted due to unparseable dates exceeding 1% threshold",
                    "evidence": [
                        {
                            "tool_name": "fetch_task_error_log",
                            "tool_call_id": "call_err_log",
                            "quote": "unparseable date ratio 5.20% exceeds max 1.00%",
                        }
                    ],
                    "proposed_action": "rules_patch",
                    "patch": {
                        "file": "config/silver_rules.yml",
                        "operation": "append_unique",
                        "key_path": "orders.date_formats.order_date",
                        "value": "%d/%m/%Y",
                    },
                    "recommended_next_steps": "Apply patch and rerun batch 2026-10-01",
                }
            ),
        ),
    )
    call_2.function.name = "submit_incident_diagnosis"
    call_2.function.arguments = json.dumps(
        {
            "root_cause_category": "format_drift",
            "confidence": 0.95,
            "summary": "Vendor introduced DD/MM/YYYY date formats into order_date",
            "impact": "Silver transformation halted due to unparseable dates exceeding 1% threshold",
            "evidence": [
                {
                    "tool_name": "fetch_task_error_log",
                    "tool_call_id": "call_err_log",
                    "quote": "unparseable date ratio 5.20% exceeds max 1.00%",
                }
            ],
            "proposed_action": "rules_patch",
            "patch": {
                "file": "config/silver_rules.yml",
                "operation": "append_unique",
                "key_path": "orders.date_formats.order_date",
                "value": "%d/%m/%Y",
            },
            "recommended_next_steps": "Apply patch and rerun batch 2026-10-01",
        }
    )

    resp_1 = MagicMock(
        choices=[MagicMock(message=MagicMock(tool_calls=[call_1], content=None))],
        usage=MagicMock(prompt_tokens=50, completion_tokens=20),
    )
    resp_2 = MagicMock(
        choices=[MagicMock(message=MagicMock(tool_calls=[call_2], content=None))],
        usage=MagicMock(prompt_tokens=80, completion_tokens=40),
    )

    fake_client = MagicMock()
    fake_client.chat.completions.create.side_effect = [resp_1, resp_2]

    with patch.dict(
        os.environ,
        {"OPENAI_MODEL": "gpt-4o-mini", "AGENT_ENABLED": "true", "OPENAI_API_KEY": "sk-test"},
    ):
        diagnosis = diagnose_incident(
            incident_id="INC-2026-10-01-silver_transform-abcd1234",
            client=fake_client,
            repo=fake_repo,
        )

    assert diagnosis.root_cause_category == RootCauseCategory.FORMAT_DRIFT
    assert diagnosis.proposed_action == ProposedAction.RULES_PATCH
    assert diagnosis.confidence == 0.95
    assert diagnosis.patch is not None
    assert diagnosis.patch.value == "%d/%m/%Y"
    assert diagnosis.patch.key_path == "orders.date_formats.order_date"

    # Verify tool calls and audit were recorded
    assert fake_repo.record_tool_call.call_count >= 1
    fake_repo.record_audit.assert_called_once()
