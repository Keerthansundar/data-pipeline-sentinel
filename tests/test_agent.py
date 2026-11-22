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


def test_ops_repository_pipeline_log_passes_string_batch_date():
    """Verify get_task_error_log passes str(batch_date) to pipeline_log query."""
    import datetime

    from responder.repository import OpsRepository

    repo = OpsRepository()
    repo.get_incident = MagicMock(
        return_value={
            "id": "INC-001",
            "task_id": "silver_transform",
            "batch_date": datetime.date(2026, 10, 1),
            "error_type": "FormatDriftError",
            "error_message": "Error occurred",
            "error_trace": "Traceback...",
        }
    )

    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = []
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    repo._get_connection = MagicMock(return_value=mock_conn)

    result = repo.get_task_error_log("INC-001")
    assert result["incident_id"] == "INC-001"
    assert mock_cur.execute.called
    query_sql, query_params = mock_cur.execute.call_args[0]
    assert "FROM pipeline_log" in query_sql
    assert query_params[0] == "2026-10-01"
    assert isinstance(query_params[0], str)
    assert query_params[1] == "silver_transform"


def test_ops_repository_schema_snapshot_latest_row_per_table():
    """Verify get_schema_snapshot queries columns column ordered by created_at DESC LIMIT 1."""
    import datetime

    from responder.repository import OpsRepository

    repo = OpsRepository()
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = ({"order_id": "string", "amount": "double"},)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    repo._get_connection = MagicMock(return_value=mock_conn)

    batch_date = datetime.date(2026, 10, 1)
    snapshot = repo.get_schema_snapshot(batch_date, "bronze.orders")

    assert snapshot == {"order_id": "string", "amount": "double"}
    assert mock_cur.execute.called
    query_sql, query_params = mock_cur.execute.call_args[0]
    assert "SELECT columns" in query_sql
    assert "FROM schema_snapshot" in query_sql
    assert "ORDER BY created_at DESC" in query_sql
    assert "LIMIT 1" in query_sql
    assert query_params == (batch_date, "bronze.orders")


def test_validate_diagnosis_truncates_long_quotes():
    """Verify evidence quotes > 200 chars are truncated to exactly 200 chars."""
    from responder.validate import validate_diagnosis_schema

    long_quote = "A" * 350
    raw_diagnosis = {
        "root_cause_category": "format_drift",
        "confidence": 0.9,
        "summary": "Date format drift detected",
        "impact": "Pipeline failed",
        "evidence": [
            {
                "tool_name": "fetch_task_error_log",
                "tool_call_id": "call_123",
                "quote": long_quote,
            }
        ],
        "proposed_action": "escalate",
        "recommended_next_steps": "Investigate error logs",
    }

    is_valid, diagnosis, err = validate_diagnosis_schema(raw_diagnosis)
    assert is_valid is True
    assert err is None
    assert diagnosis is not None
    assert len(diagnosis.evidence[0].quote) == 200
    assert diagnosis.evidence[0].quote == "A" * 200


def test_validate_diagnosis_patch_value_dict_handling():
    """Verify string patch.value is accepted while dict value is rejected."""
    from responder.validate import validate_diagnosis_schema

    base_diagnosis = {
        "root_cause_category": "format_drift",
        "confidence": 0.9,
        "summary": "Date format drift detected",
        "impact": "Pipeline failed",
        "evidence": [],
        "proposed_action": "rules_patch",
        "patch": {
            "file": "config/silver_rules.yml",
            "operation": "append_unique",
            "key_path": "orders.date_formats.order_date",
            "value": "%d/%m/%Y",
        },
        "recommended_next_steps": "Apply patch and rerun",
    }

    # 1. Plain string value: accepted
    is_valid, diag_str, err = validate_diagnosis_schema(base_diagnosis.copy())
    assert is_valid is True
    assert diag_str is not None
    assert diag_str.patch.value == "%d/%m/%Y"

    # 2. Dict value with 'format': rejected
    diag_dict_format = base_diagnosis.copy()
    diag_dict_format["patch"] = {
        "file": "config/silver_rules.yml",
        "operation": "append_unique",
        "key_path": "orders.date_formats.order_date",
        "value": {"format": "%d/%m/%Y"},
    }
    is_valid, diag_dict, err = validate_diagnosis_schema(diag_dict_format)
    assert is_valid is False
    assert diag_dict is None
    assert "patch.value must be a plain string" in (err or "")

    # 3. Invalid dict value: rejected cleanly without crashing
    diag_invalid_dict = base_diagnosis.copy()
    diag_invalid_dict["patch"] = {
        "file": "config/silver_rules.yml",
        "operation": "append_unique",
        "key_path": "orders.date_formats.order_date",
        "value": {"unknown_field": 12345},
    }
    is_valid, diag_inv, err = validate_diagnosis_schema(diag_invalid_dict)
    assert is_valid is False
    assert diag_inv is None
    assert "patch.value must be a plain string" in (err or "")


def test_citation_check_accepts_verbatim_quote_with_wrong_or_missing_tool_call_id_and_rejects_non_verbatim():
    """Verify validate_evidence_citations accepts verbatim quote with wrong/missing tool_call_id and rejects non-verbatim."""
    from responder.schemas import (
        EvidenceItem,
        IncidentDiagnosis,
        ProposedAction,
        RootCauseCategory,
        RulesPatch,
    )
    from responder.validate import validate_evidence_citations

    tool_outputs = {
        "fetch_task_error_log": "Error occurred in silver: unparseable date ratio 5.2% exceeds max 1.0% limit."
    }

    # 1. Verbatim quote with missing tool_call_id (None): accepted
    diag_missing_id = IncidentDiagnosis(
        root_cause_category=RootCauseCategory.FORMAT_DRIFT,
        confidence=0.9,
        summary="Format drift",
        impact="Silver stopped",
        evidence=[
            EvidenceItem(
                tool_name="fetch_task_error_log",
                tool_call_id=None,
                quote="unparseable date ratio 5.2% exceeds max 1.0%",
            )
        ],
        proposed_action=ProposedAction.ESCALATE,
        recommended_next_steps="Review error logs",
    )
    ok, err = validate_evidence_citations(diag_missing_id, tool_outputs)
    assert ok is True
    assert err is None

    # 2. Verbatim quote with wrong tool_call_id and functions. prefix: accepted
    diag_wrong_id = IncidentDiagnosis(
        root_cause_category=RootCauseCategory.FORMAT_DRIFT,
        confidence=0.9,
        summary="Format drift",
        impact="Silver stopped",
        evidence=[
            EvidenceItem(
                tool_name="functions.fetch_task_error_log",
                tool_call_id="INC-2026-10-01-silver_transform",
                quote="unparseable   date ratio\n5.2% exceeds max 1.0%",
            )
        ],
        proposed_action=ProposedAction.ESCALATE,
        recommended_next_steps="Review error logs",
    )
    ok, err = validate_evidence_citations(diag_wrong_id, tool_outputs)
    assert ok is True
    assert err is None

    # 3. Non-verbatim quote for rules_patch: rejected
    diag_non_verbatim = IncidentDiagnosis(
        root_cause_category=RootCauseCategory.FORMAT_DRIFT,
        confidence=0.9,
        summary="Format drift",
        impact="Silver stopped",
        evidence=[
            EvidenceItem(
                tool_name="fetch_task_error_log",
                tool_call_id=None,
                quote="this quote does not exist in output at all",
            )
        ],
        proposed_action=ProposedAction.RULES_PATCH,
        patch=RulesPatch(
            file="config/silver_rules.yml",
            operation="append_unique",
            key_path="orders.date_formats.order_date",
            value="%d/%m/%Y",
        ),
        recommended_next_steps="Review error logs",
    )
    ok, err = validate_evidence_citations(diag_non_verbatim, tool_outputs)
    assert ok is False
    assert "Rules patch requires a verified quote from fetch_task_error_log" in (err or "")


def test_policy_rejects_operation_update_and_invalid_key_path():
    """Verify policy rejects operation 'update' and invalid key_path 'rules.order_date.format'."""
    from responder.policy import enforce_patch_policy
    from responder.schemas import IncidentDiagnosis, ProposedAction, RootCauseCategory, RulesPatch

    # 1. Operation 'update' is rejected
    diag_update = IncidentDiagnosis(
        root_cause_category=RootCauseCategory.FORMAT_DRIFT,
        confidence=0.9,
        summary="Format drift",
        impact="Silver stopped",
        evidence=[],
        proposed_action=ProposedAction.RULES_PATCH,
        patch=RulesPatch(
            file="config/silver_rules.yml",
            operation="update",
            key_path="orders.date_formats.order_date",
            value="%d/%m/%Y",
        ),
        recommended_next_steps="Apply patch",
    )
    ok, err = enforce_patch_policy(diag_update, ambiguous_ratio=0.0)
    assert ok is False
    assert "not allowed" in (err or "")

    # 2. Key path 'rules.order_date.format' is rejected
    diag_key_path = IncidentDiagnosis(
        root_cause_category=RootCauseCategory.FORMAT_DRIFT,
        confidence=0.9,
        summary="Format drift",
        impact="Silver stopped",
        evidence=[],
        proposed_action=ProposedAction.RULES_PATCH,
        patch=RulesPatch(
            file="config/silver_rules.yml",
            operation="append_unique",
            key_path="rules.order_date.format",
            value="%d/%m/%Y",
        ),
        recommended_next_steps="Apply patch",
    )
    ok, err = enforce_patch_policy(diag_key_path, ambiguous_ratio=0.0)
    assert ok is False
    assert "not an authorized patch target" in (err or "")


def test_citation_check_drops_bad_secondary_quote_and_passes():
    """Verify bad secondary quote is dropped and diagnosis passes when error log quote is verbatim."""
    from responder.schemas import (
        EvidenceItem,
        IncidentDiagnosis,
        ProposedAction,
        RootCauseCategory,
        RulesPatch,
    )
    from responder.validate import validate_evidence_citations

    tool_outputs = {
        "fetch_task_error_log": "Error: unparseable date ratio 5.2% exceeds max 1.0% seen in batch",
        "inspect_column_profile": "Column profiling completed for batch 2026-10-01",
    }
    diag = IncidentDiagnosis(
        root_cause_category=RootCauseCategory.FORMAT_DRIFT,
        confidence=0.95,
        summary="Format drift detected",
        impact="Silver stopped",
        evidence=[
            EvidenceItem(
                tool_name="fetch_task_error_log",
                quote="unparseable date ratio 5.2% exceeds max 1.0%",
            ),
            EvidenceItem(
                tool_name="inspect_column_profile",
                quote="completely hallucinated quote that does not appear in column profile at all",
            ),
        ],
        proposed_action=ProposedAction.RULES_PATCH,
        patch=RulesPatch(
            file="config/silver_rules.yml",
            operation="append_unique",
            key_path="orders.date_formats.order_date",
            value="%d/%m/%Y",
        ),
        recommended_next_steps="Apply patch and rerun",
    )

    cit_res = validate_evidence_citations(diag, tool_outputs)
    assert cit_res[0] is True
    assert cit_res[1] is None
    # Bad secondary quote was removed from diagnosis.evidence
    assert len(diag.evidence) == 1
    assert diag.evidence[0].tool_name == "fetch_task_error_log"
    # Dropped item is recorded with quote truncated to 80 chars
    assert len(cit_res.dropped_items) == 1
    assert cit_res.dropped_items[0]["tool_name"] == "inspect_column_profile"
    assert "completely hallucinated quote" in cit_res.dropped_items[0]["quote"]
    assert len(cit_res.dropped_items[0]["quote"]) <= 80


def test_citation_check_rules_patch_only_bad_error_log_quote_fails():
    """Verify rules_patch with only a bad fetch_task_error_log quote fails."""
    from responder.schemas import (
        EvidenceItem,
        IncidentDiagnosis,
        ProposedAction,
        RootCauseCategory,
        RulesPatch,
    )
    from responder.validate import validate_evidence_citations

    tool_outputs = {
        "fetch_task_error_log": "Pipeline log output without matching error string",
    }
    diag = IncidentDiagnosis(
        root_cause_category=RootCauseCategory.FORMAT_DRIFT,
        confidence=0.95,
        summary="Format drift detected",
        impact="Silver stopped",
        evidence=[
            EvidenceItem(
                tool_name="fetch_task_error_log",
                quote="unparseable date ratio 5.2% exceeds max 1.0%",
            ),
        ],
        proposed_action=ProposedAction.RULES_PATCH,
        patch=RulesPatch(
            file="config/silver_rules.yml",
            operation="append_unique",
            key_path="orders.date_formats.order_date",
            value="%d/%m/%Y",
        ),
        recommended_next_steps="Apply patch and rerun",
    )

    cit_res = validate_evidence_citations(diag, tool_outputs)
    assert cit_res[0] is False
    assert "Rules patch requires a verified quote from fetch_task_error_log" in (cit_res[1] or "")
    # Bad quote was removed
    assert len(diag.evidence) == 0
    assert len(cit_res.dropped_items) == 1


def test_citation_check_tool_never_called_fails():
    """Verify evidence referencing a tool that was never called strictly fails."""
    from responder.schemas import (
        EvidenceItem,
        IncidentDiagnosis,
        ProposedAction,
        RootCauseCategory,
    )
    from responder.validate import validate_evidence_citations

    tool_outputs = {
        "fetch_task_error_log": "Pipeline log output",
    }
    diag = IncidentDiagnosis(
        root_cause_category=RootCauseCategory.FORMAT_DRIFT,
        confidence=0.9,
        summary="Format drift",
        impact="Silver stopped",
        evidence=[
            EvidenceItem(
                tool_name="uncalled_tool_name",
                quote="some quote",
            ),
        ],
        proposed_action=ProposedAction.ESCALATE,
        recommended_next_steps="Review logs",
    )

    cit_res = validate_evidence_citations(diag, tool_outputs)
    assert cit_res[0] is False
    assert "was not called in this run" in (cit_res[1] or "")


def test_citation_check_escalate_without_patch_valid_with_all_evidence_dropped():
    """Verify escalate-without-patch diagnoses remain valid even if all evidence was dropped."""
    from responder.schemas import (
        EvidenceItem,
        IncidentDiagnosis,
        ProposedAction,
        RootCauseCategory,
    )
    from responder.validate import validate_evidence_citations

    tool_outputs = {
        "fetch_task_error_log": "Pipeline log output without the quoted string",
    }
    diag = IncidentDiagnosis(
        root_cause_category=RootCauseCategory.UNKNOWN,
        confidence=0.5,
        summary="Investigation inconclusive",
        impact="Pipeline stopped",
        evidence=[
            EvidenceItem(
                tool_name="fetch_task_error_log",
                quote="this quote is not in the log output",
            ),
        ],
        proposed_action=ProposedAction.ESCALATE,
        recommended_next_steps="Manual review needed",
    )

    cit_res = validate_evidence_citations(diag, tool_outputs)
    assert cit_res[0] is True
    assert cit_res[1] is None
    assert len(diag.evidence) == 0
    assert len(cit_res.dropped_items) == 1
