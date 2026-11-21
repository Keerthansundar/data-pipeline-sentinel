"""Unit tests for dashboard: adapters, Delta reader, UI components, and import smoke tests."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from dashboard.lib.airflow_meta import AirflowMetaAdapter
from dashboard.lib.delta_reader import (
    get_revenue_kpis,
    read_gold_channel_revenue,
    read_gold_customer_revenue,
    read_gold_daily_revenue,
)
from dashboard.lib.ops_db import OpsDbAdapter
from dashboard.lib.ui_components import (
    generate_pipeline_graph_dot,
    render_status_pill,
)


def test_dashboard_imports_smoke():
    """Verify all dashboard pages and libraries import cleanly."""
    import dashboard.lib.airflow_meta
    import dashboard.lib.delta_reader
    import dashboard.lib.ops_db
    import dashboard.lib.ui_components

    assert dashboard.lib.airflow_meta is not None
    assert dashboard.lib.delta_reader is not None
    assert dashboard.lib.ops_db is not None
    assert dashboard.lib.ui_components is not None


def test_status_pill_rendering():
    """Verify HTML status pills have expected colors and icons."""
    pill_success = render_status_pill("success")
    assert "#28a745" in pill_success
    assert "✓" in pill_success
    assert "success" in pill_success.lower()

    pill_failed = render_status_pill("failed")
    assert "#dc3545" in pill_failed
    assert "✗" in pill_failed
    assert "failed" in pill_failed.lower()

    pill_running = render_status_pill("running")
    assert "#17a2b8" in pill_running
    assert "running" in pill_running.lower()

    pill_default = render_status_pill(None)
    assert "queued" in pill_default.lower()


def test_pipeline_graph_dot_generation():
    """Verify Graphviz dot string generation for known DAG topology."""
    tasks = [
        {"Task ID": "wait_for_vendor_files", "State": "success"},
        {"Task ID": "bronze_ingest", "State": "success"},
        {"Task ID": "silver_transform", "State": "failed"},
    ]
    dot = generate_pipeline_graph_dot(tasks, dag_id="orders_medallion_daily")
    assert "digraph Pipeline" in dot
    assert "wait_for_vendor_files" in dot
    assert "bronze_ingest" in dot
    assert "silver_transform" in dot
    assert "#28a745" in dot  # Green for success
    assert "#dc3545" in dot  # Red for failed


def test_airflow_meta_adapter_mock():
    """Verify AirflowMetaAdapter handles queries and formats columns cleanly."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Mock get_dags, get_dag_runs, get_task_instances
    mock_cur.fetchall.side_effect = [
        [{"dag_id": "orders_medallion_daily"}, {"dag_id": "vendor_feed_simulator"}],
        [
            {
                "run_id": "manual__1",
                "state": "success",
                "logical_date": datetime(2026, 10, 1),
                "start_date": datetime(2026, 10, 1, 5, 0),
                "end_date": datetime(2026, 10, 1, 5, 2),
                "run_type": "manual",
                "run_after": datetime(2026, 10, 1, 5, 0),
            }
        ],
        [
            {
                "task_id": "silver_transform",
                "rendered_map_index": None,
                "state": "success",
                "start_date": datetime(2026, 10, 1, 5, 1),
                "end_date": datetime(2026, 10, 1, 5, 2),
                "duration": 42.5,
                "try_number": 1,
                "pool": "default_pool",
                "operator": "PythonOperator",
            }
        ],
    ]

    adapter = AirflowMetaAdapter(db_url="postgresql://test:test@localhost:5432/airflow")
    with patch.object(adapter, "_get_connection", return_value=mock_conn):
        dags = adapter.get_dags()
        assert "orders_medallion_daily" in dags

        runs = adapter.get_dag_runs("orders_medallion_daily")
        assert len(runs) == 1
        assert runs[0]["run_id"] == "manual__1"

        ti = adapter.get_task_instances("orders_medallion_daily", "manual__1")
        assert len(ti) == 1
        assert ti[0]["Task ID"] == "silver_transform"
        assert "`" not in ti[0]["Task ID"]  # Ensure no backticks in raw task ID
        assert ti[0]["Map Index"] == "-"
        assert ti[0]["Duration"] == "42.50s"
        assert ti[0]["State"] == "success"


def test_ops_db_adapter_mock():
    """Verify OpsDbAdapter queries incidents, logs, dq_results and demo_config."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchall.side_effect = [
        [{"id": 1, "logged_at": datetime.now(), "level": "INFO", "message": "Test log"}],
        [
            {
                "id": "INC-1",
                "status": "OPEN",
                "batch_date": "2026-10-01",
                "task_id": "silver_transform",
            }
        ],
        [{"check_name": "row_conservation", "passed": True}],
    ]
    mock_cur.fetchone.side_effect = [
        {"id": "INC-1", "status": "OPEN", "error_message": "drift"},
        {
            "incident_id": "INC-1",
            "diagnosis": "{}",
            "pr_url_or_preview": "data/pr_previews/INC-1.md",
            "final_action": "pr_opened",
        },
        {"failure_mode": "date_format", "target_batch_date": "2026-10-01"},
    ]

    adapter = OpsDbAdapter(db_url="postgresql://test:test@localhost:5434/ops")
    with patch.object(adapter, "_get_connection", return_value=mock_conn):
        logs = adapter.get_recent_logs(dag_id="orders_medallion_daily")
        assert len(logs) == 1

        incs = adapter.get_incidents(status="OPEN")
        assert len(incs) == 1

        dq = adapter.get_dq_results()
        assert len(dq) == 1

        inc = adapter.get_incident_detail("INC-1")
        assert inc["id"] == "INC-1"

        audit = adapter.get_incident_audit("INC-1")
        assert audit is not None
        assert audit["pr_url_or_preview"] == "data/pr_previews/INC-1.md"

        cfg = adapter.get_demo_config()
        assert cfg["failure_mode"] == "date_format"


def test_delta_reader_empty_state():
    """Verify Delta reader helpers return safe empty DataFrames when tables are absent."""
    with patch("dashboard.lib.delta_reader._load_delta_table", return_value=pd.DataFrame()):
        df_daily = read_gold_daily_revenue()
        assert isinstance(df_daily, pd.DataFrame)
        assert "total_revenue" in df_daily.columns
        assert df_daily.empty

        df_channel = read_gold_channel_revenue()
        assert isinstance(df_channel, pd.DataFrame)
        assert "channel" in df_channel.columns
        assert df_channel.empty

        df_cust = read_gold_customer_revenue()
        assert isinstance(df_cust, pd.DataFrame)
        assert "lifetime_revenue" in df_cust.columns
        assert df_cust.empty

        kpis = get_revenue_kpis()
        assert kpis["total_revenue"] == 0.0
        assert kpis["total_orders"] == 0
        assert kpis["avg_order_value"] == 0.0


def test_delta_reader_real_schema_daily_revenue():
    """Verify daily_revenue reader and KPI calculation with real Gold schema columns."""
    # Real columns written by pipeline/gold.py: _batch_date, total_revenue, order_count, avg_order_value
    real_gold_df = pd.DataFrame(
        {
            "_batch_date": ["2026-10-01", "2026-10-02"],
            "total_revenue": [1267351.07, 1254320.50],
            "order_count": [4969, 4974],
            "avg_order_value": [255.05, 252.17],
        }
    )

    with patch("dashboard.lib.delta_reader._load_delta_table", return_value=real_gold_df):
        df = read_gold_daily_revenue()
        assert "order_date" in df.columns  # Aliased from _batch_date
        assert "total_orders" in df.columns  # Aliased from order_count
        assert df["order_date"].iloc[0] == "2026-10-01"

        kpis = get_revenue_kpis()
        assert kpis["total_revenue"] == 1267351.07 + 1254320.50
        assert kpis["total_orders"] == 4969 + 4974
        assert kpis["total_orders"] > 0
        expected_aov = (1267351.07 + 1254320.50) / (4969 + 4974)
        assert abs(kpis["avg_order_value"] - expected_aov) < 0.01


def test_delta_reader_real_schema_channel_revenue():
    """Verify revenue_by_channel reader with real Gold schema columns."""
    real_channel_df = pd.DataFrame(
        {
            "_batch_date": ["2026-10-01", "2026-10-01", "2026-10-01"],
            "channel": ["web", "mobile", "store"],
            "channel_revenue": [500000.0, 400000.0, 367351.07],
            "order_count": [2000, 1600, 1369],
        }
    )

    with patch("dashboard.lib.delta_reader._load_delta_table", return_value=real_channel_df):
        df = read_gold_channel_revenue()
        assert not df.empty
        assert "channel" in df.columns
        assert "channel_revenue" in df.columns
        assert "total_revenue" in df.columns  # Alias
        assert "total_orders" in df.columns  # Alias
        assert df["channel_revenue"].sum() == 1267351.07


def test_delta_reader_real_schema_customer_summary():
    """Verify customer_summary reader with real Gold schema columns."""
    real_cust_df = pd.DataFrame(
        {
            "customer_id": ["CUST-001", "CUST-002"],
            "name": ["Alice Smith", "Bob Jones"],
            "segment": ["Enterprise", "SMB"],
            "_batch_date": ["2026-10-01", "2026-10-01"],
            "customer_revenue": [1500.00, 850.50],
            "total_orders": [5, 3],
        }
    )

    with patch("dashboard.lib.delta_reader._load_delta_table", return_value=real_cust_df):
        df = read_gold_customer_revenue()
        assert not df.empty
        assert "customer_id" in df.columns
        assert "name" in df.columns
        assert "customer_name" in df.columns  # Alias
        assert "lifetime_revenue" in df.columns  # Alias
        assert "lifetime_orders" in df.columns  # Alias
        assert df["customer_revenue"].sum() == 2350.50
