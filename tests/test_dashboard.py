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


def test_metrics_variance_and_filter():
    """Verify variance calculation and date filtering logic."""
    from dashboard.lib.metrics import calculate_variance_pct, filter_by_date_range

    # Variance tests
    assert calculate_variance_pct(110.0, 100.0) == 10.0
    assert calculate_variance_pct(80.0, 100.0) == -20.0
    assert calculate_variance_pct(100.0, None) is None
    assert calculate_variance_pct(100.0, 0.0) is None  # Guard against div by zero

    # Filter by date range tests
    dates = [f"2026-10-{d:02d}" for d in range(1, 15)]  # 14 dates
    df = pd.DataFrame({"order_date": dates, "val": range(14)})

    cur, prior = filter_by_date_range(df, "Last 7 days")
    assert cur["order_date"].nunique() == 7
    assert prior is not None
    assert prior["order_date"].nunique() == 7
    assert cur["order_date"].min() == "2026-10-08"
    assert prior["order_date"].max() == "2026-10-07"

    # All dates test
    cur_all, prior_all = filter_by_date_range(df, "All dates")
    assert len(cur_all) == 14
    assert prior_all is None


def test_metrics_revenue_scorecard_and_customers():
    """Verify executive scorecard aggregation and customer name resolution."""
    from dashboard.lib.metrics import (
        compute_channel_scorecard,
        compute_daily_revenue_trend,
        compute_revenue_kpis,
        compute_top_customers,
    )

    df_daily = pd.DataFrame(
        {
            "order_date": [f"2026-10-{d:02d}" for d in range(1, 11)],
            "total_revenue": [1000.0] * 10,
            "order_count": [10] * 10,
        }
    )
    df_cust = pd.DataFrame(
        {
            "customer_id": ["C1", "C2", "C1"],
            "customer_revenue": [500.0, 1000.0, 200.0],
            "total_orders": [2, 4, 1],
        }
    )

    kpis = compute_revenue_kpis(df_daily, df_cust)
    assert kpis["total_revenue"] == 10000.0
    assert kpis["total_orders"] == 100
    assert kpis["avg_order_value"] == 100.0
    assert kpis["active_customers"] == 2

    # Trend moving average
    trend = compute_daily_revenue_trend(df_daily)
    assert "moving_avg_7d" in trend.columns
    assert len(trend) == 10
    assert trend["moving_avg_7d"].iloc[-1] == 1000.0

    # Top customers
    lookup = {"C1": {"name": "Customer One"}, "C2": {"name": "Customer Two"}}
    top = compute_top_customers(df_cust, customer_names=lookup, limit=5)
    assert len(top) == 2
    assert top["name"].iloc[0] == "Customer Two"  # 1000 revenue vs 700
    assert "Customer Two (C2)" in top["customer_label"].iloc[0]

    # Channel scorecard
    df_chan = pd.DataFrame(
        {
            "channel": ["web", "mobile"],
            "channel_revenue": [6000.0, 4000.0],
            "order_count": [60, 40],
        }
    )
    sc = compute_channel_scorecard(df_chan)
    assert len(sc) == 2
    assert sc["Share %"].iloc[0] == 60.0
    assert sc["Average Order Value"].iloc[0] == 100.0


def test_metrics_pipeline_health_status():
    """Verify one-line pipeline status banner logic."""
    from dashboard.lib.metrics import (
        compute_batch_durations,
        compute_latest_row_flow,
        compute_pipeline_health_kpis,
        compute_pipeline_status,
    )

    # 1. Failing status
    st_fail = compute_pipeline_status(
        {"status": "FAILED"}, hours_since_refresh=2.0, open_incident_count=1
    )
    assert st_fail["kind"] == "failing"
    assert st_fail["title"] == "Failing"

    # 2. Stale data status
    st_stale = compute_pipeline_status(
        {"status": "SUCCESS"}, hours_since_refresh=30.0, open_incident_count=0
    )
    assert st_stale["kind"] == "attention"
    assert "stale" in st_stale["detail"].lower()

    # 3. Open incident status
    st_inc = compute_pipeline_status(
        {"status": "SUCCESS"}, hours_since_refresh=5.0, open_incident_count=2
    )
    assert st_inc["kind"] == "attention"
    assert "2 open" in st_inc["detail"]

    # 4. Healthy status
    st_ok = compute_pipeline_status(
        {"status": "SUCCESS"}, hours_since_refresh=5.0, open_incident_count=0
    )
    assert st_ok["kind"] == "healthy"
    assert st_ok["title"] == "All systems healthy"

    # KPI calculations
    now = datetime.now()
    runs = [
        {
            "task_id": "gold_build",
            "batch_date": "2026-10-01",
            "status": "SUCCESS",
            "rows_in": 5000,
            "rows_out": 4974,
            "rows_quarantined": 26,
            "started_at": now,
            "ended_at": now,
        }
    ]
    hkpis = compute_pipeline_health_kpis(runs, last_refresh=now, resolved_by_retry=3)
    assert hkpis["last_run_status"] == "SUCCESS"
    assert hkpis["success_rate"] == 100.0
    assert hkpis["resolved_by_retry"] == 3
    assert abs(hkpis["quarantine_rate"] - (26.0 / 5000.0 * 100.0)) < 0.01

    # Batch durations and row flow
    dur_df = compute_batch_durations(runs)
    assert not dur_df.empty

    flow = compute_latest_row_flow(runs)
    assert len(flow) == 4
    assert flow[0]["stage"] == "1. Orders In"
    assert flow[0]["count"] == 5000


def test_ui_components_status_pill():
    """Verify status_pill rendering."""
    from dashboard.lib.ui_components import status_pill

    pill_pass = status_pill("Pass", "success")
    assert "Pass" in pill_pass
    assert "#116943" in pill_pass

    pill_fail = status_pill("Fail", "error")
    assert "Fail" in pill_fail
    assert "#991B1B" in pill_fail


def test_group_runs_by_batch_and_per_run_totals():
    """Verify group_runs_by_batch groups tasks into batch runs with real schema."""
    from datetime import datetime, timedelta

    from dashboard.lib.metrics import compute_batch_durations, group_runs_by_batch

    t0 = datetime(2026, 10, 2, 10, 0, 0)
    # 4 tasks belonging to one DAG run for 2026-10-02
    runs = [
        {
            "dag_run_id": "scheduled__2026-10-02T00:00:00",
            "task_id": "gold_build",
            "batch_date": "2026-10-02",
            "status": "SUCCESS",
            "rows_in": 4974,
            "rows_out": 4974,
            "rows_quarantined": 0,
            "started_at": t0 + timedelta(seconds=90),
            "ended_at": t0 + timedelta(seconds=120),
        },
        {
            "dag_run_id": "scheduled__2026-10-02T00:00:00",
            "task_id": "data_quality_checks",
            "batch_date": "2026-10-02",
            "status": "SUCCESS",
            "rows_in": 5000,
            "rows_out": 4974,
            "rows_quarantined": 0,
            "started_at": t0 + timedelta(seconds=60),
            "ended_at": t0 + timedelta(seconds=90),
        },
        {
            "dag_run_id": "scheduled__2026-10-02T00:00:00",
            "task_id": "silver_transform",
            "batch_date": "2026-10-02",
            "status": "SUCCESS",
            "rows_in": 5000,
            "rows_out": 4974,
            "rows_quarantined": 26,
            "started_at": t0 + timedelta(seconds=20),
            "ended_at": t0 + timedelta(seconds=60),
        },
        {
            "dag_run_id": "scheduled__2026-10-02T00:00:00",
            "task_id": "bronze_ingest",
            "batch_date": "2026-10-02",
            "status": "SUCCESS",
            "rows_in": 5000,
            "rows_out": 5386,
            "rows_quarantined": 0,
            "started_at": t0,
            "ended_at": t0 + timedelta(seconds=20),
        },
    ]

    batch_runs = group_runs_by_batch(runs)
    assert len(batch_runs) == 1
    br = batch_runs[0]
    assert br["batch_date"] == "2026-10-02"
    assert br["status"] == "SUCCESS"
    assert br["orders_in"] == 5000
    assert br["quarantined"] == 26
    # Sum of task durations: 20s + 40s + 30s + 30s = 120s
    assert abs(br["total_duration_sec"] - 120.0) < 0.1

    # compute_batch_durations must use the same per-run totals
    dur_df = compute_batch_durations(runs)
    assert len(dur_df) == 1
    assert dur_df.iloc[0]["batch_date"] == "2026-10-02"
    assert abs(dur_df.iloc[0]["total_duration_sec"] - 120.0) < 0.1


def test_quarantine_rate_reads_silver_transform_or_sum():
    """Verify quarantine rate reads silver_transform row (26/5000 = 0.52%), not gold_build."""
    from dashboard.lib.metrics import compute_pipeline_health_kpis

    # gold_build is at runs[0] with quarantined=0
    runs = [
        {
            "dag_run_id": "manual__1",
            "task_id": "gold_build",
            "batch_date": "2026-10-02",
            "status": "SUCCESS",
            "rows_in": 4974,
            "rows_out": 4974,
            "rows_quarantined": 0,
        },
        {
            "dag_run_id": "manual__1",
            "task_id": "silver_transform",
            "batch_date": "2026-10-02",
            "status": "SUCCESS",
            "rows_in": 5000,
            "rows_out": 4974,
            "rows_quarantined": 26,
        },
        {
            "dag_run_id": "manual__1",
            "task_id": "bronze_ingest",
            "batch_date": "2026-10-02",
            "status": "SUCCESS",
            "rows_in": 5000,
            "rows_out": 5386,
            "rows_quarantined": 0,
        },
    ]

    kpis = compute_pipeline_health_kpis(runs, last_refresh=None, resolved_by_retry=0)
    # Expected: 26 / 5000 * 100 = 0.52%
    assert abs(kpis["quarantine_rate"] - 0.52) < 0.01


def test_cleanup_batch_logic():
    """Verify cleanup_batch script logic for Delta tables and Ops DB."""
    from pathlib import Path
    from unittest.mock import MagicMock, patch

    from tests.cleanup_batch import (
        delete_from_delta_tables,
        delete_from_ops_db,
        validate_batch_date,
    )

    assert validate_batch_date("2026-11-01") is True
    assert validate_batch_date("invalid-date") is False

    # Mock Delta table deletes
    with patch("tests.cleanup_batch.DeltaTable") as mock_dt:
        mock_instance = MagicMock()
        mock_dt.return_value = mock_instance
        # Create a fake root where one table path exists
        with patch.object(Path, "exists", return_value=True):
            res = delete_from_delta_tables("2026-11-01", root_dir=Path("/fake/root"))
            assert len(res) == 8
            # DeltaTable.delete should be called with predicate
            assert mock_instance.delete.called
            call_arg = mock_instance.delete.call_args[0][0]
            assert call_arg == "_batch_date = '2026-11-01'"

    # Mock Postgres Ops DB deletes
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 3
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    ops_res = delete_from_ops_db("2026-11-01", conn=mock_conn)
    assert "pipeline_run_log" in ops_res
    assert "incidents" in ops_res
    assert ops_res["pipeline_run_log"] == 3
    assert mock_cursor.execute.call_count == 8
