"""Data quality checks executed on Silver and Bronze before Gold build."""

from datetime import UTC, datetime
from typing import Any

from pipeline.column_resolve import candidate_columns
from pipeline.config import get_project_root, load_yaml_config
from pipeline.errors import DataQualityError
from pipeline.logging_config import get_logger
from pipeline.observability import record_dq_result, record_pipeline_run
from pipeline.spark_session import get_spark_session

logger = get_logger(__name__)


def run_dq_checks(
    batch_date: str,
    dag_run_id: str = "manual",
    task_id: str = "data_quality_checks",
    rules_file_path: str = "config/dq_rules.yml",
) -> dict[str, Any]:
    """Execute DQ checks per config/dq_rules.yml.

    Args:
        batch_date: Target batch date YYYY-MM-DD.
        dag_run_id: DagRun ID for observability tracking.
        task_id: Task name.
        rules_file_path: Relative path to DQ rules YAML.

    Returns:
        Summary dict of DQ check results.
    """
    start_time = datetime.now(UTC)
    root = get_project_root()
    bronze_dir = root / "data" / "bronze"
    silver_dir = root / "data" / "silver"
    quarantine_dir = root / "data" / "quarantine"

    logger.info(
        "Starting Data Quality checks: batch_date=%s, rules_file=%s",
        batch_date,
        rules_file_path,
    )

    rules = load_yaml_config(rules_file_path)
    orders_rules = rules.get("orders", {})
    row_count_tol = float(orders_rules.get("row_count_tolerance_pct", 0.20))
    max_null_rates = orders_rules.get("max_null_rate", {})
    rev_tol = float(orders_rules.get("revenue_reconciliation_tolerance", 0.001))

    spark = get_spark_session("DQChecks")

    bronze_orders = (
        spark.read.format("delta")
        .load(str(bronze_dir / "orders"))
        .filter(f"_batch_date = '{batch_date}'")
    ).cache()
    silver_orders = (
        spark.read.format("delta")
        .load(str(silver_dir / "orders"))
        .filter(f"_batch_date = '{batch_date}'")
    ).cache()

    quarantine_orders = None
    q_path = quarantine_dir / "orders"
    if q_path.exists():
        quarantine_orders = (
            spark.read.format("delta").load(str(q_path)).filter(f"_batch_date = '{batch_date}'")
        ).cache()

    from pyspark.sql.functions import abs as spark_abs
    from pyspark.sql.functions import coalesce, col, count, trim, when
    from pyspark.sql.functions import sum as spark_sum

    # Single pass aggregations for silver, quarantine, and bronze
    s_stats = silver_orders.agg(
        count("*").alias("count"),
        spark_sum("amount").alias("revenue"),
    ).collect()[0]
    s_count = int(s_stats["count"] or 0)
    silver_revenue = float(s_stats["revenue"] or 0.0)

    if quarantine_orders:
        q_amt = col("amount").cast("double")
        q_stats = quarantine_orders.agg(
            count("*").alias("count"),
            spark_sum(when(q_amt.isNotNull(), spark_abs(q_amt)).otherwise(0.0)).alias("revenue"),
        ).collect()[0]
        q_count = int(q_stats["count"] or 0)
        quarantine_revenue = float(q_stats["revenue"] or 0.0)
    else:
        q_count = 0
        quarantine_revenue = 0.0

    silver_rules = load_yaml_config("config/silver_rules.yml")
    aliases = silver_rules.get("orders", {}).get("column_aliases", {})
    target_dq_cols = [
        c for c in max_null_rates if candidate_columns(c, bronze_orders.columns, aliases)
    ]
    amt_cols = candidate_columns("amount", bronze_orders.columns, aliases) or ["amount"]
    b_amt = coalesce(*[col(x).cast("double") for x in amt_cols])
    b_agg_exprs = [
        count("*").alias("count"),
        spark_sum(when(b_amt.isNotNull(), spark_abs(b_amt)).otherwise(0.0)).alias("revenue"),
    ]
    for c in target_dq_cols:
        srcs = candidate_columns(c, bronze_orders.columns, aliases)
        c_str = coalesce(*[col(x).cast("string") for x in srcs])
        null_cond = (
            c_str.isNull() | (trim(c_str) == "") | (trim(c_str) == "None") | (trim(c_str) == "nan")
        )
        b_agg_exprs.append(spark_sum(when(null_cond, 1).otherwise(0)).alias(f"null_{c}"))

    b_stats = bronze_orders.agg(*b_agg_exprs).collect()[0]
    b_count = int(b_stats["count"] or 0)
    bronze_revenue = float(b_stats["revenue"] or 0.0)

    dq_failures = []

    # Check 1: Row count conservation and tolerance
    expected_rows = 5000
    min_rows = int(expected_rows * (1.0 - row_count_tol))
    max_rows = int(expected_rows * (1.0 + row_count_tol))
    row_count_passed = (s_count + q_count == b_count) and (min_rows <= b_count <= max_rows)

    record_dq_result(
        batch_date=batch_date,
        table_name="orders",
        check_name="row_count_conservation",
        observed_value=f"silver={s_count},quarantine={q_count},bronze={b_count}",
        threshold_value=f"expected_range=[{min_rows}, {max_rows}]",
        passed=row_count_passed,
        details=f"Conservation check: {s_count} + {q_count} == {b_count}",
    )
    logger.info(
        "DQ check 'row_count_conservation': observed=%d, range=[%d, %d], pass=%s",
        b_count,
        min_rows,
        max_rows,
        row_count_passed,
    )
    if not row_count_passed:
        dq_failures.append(("row_count_conservation", b_count, f"[{min_rows}, {max_rows}]"))

    # Check 2: Null rate limits on raw Bronze columns (from single-pass stats)
    for col_name, limit in max_null_rates.items():
        if col_name in target_dq_cols:
            nulls = int(b_stats[f"null_{col_name}"] or 0)
            null_rate = float(nulls / b_count) if b_count > 0 else 0.0
            passed = null_rate <= float(limit)

            record_dq_result(
                batch_date=batch_date,
                table_name="bronze.orders",
                check_name=f"max_null_rate:{col_name}",
                observed_value=f"{null_rate:.4f}",
                threshold_value=f"{float(limit):.4f}",
                passed=passed,
                details=f"null_count={nulls}/{b_count}",
            )
            logger.info(
                "DQ check 'max_null_rate:%s': observed=%.4f (nulls=%d), threshold=%.4f, pass=%s",
                col_name,
                null_rate,
                nulls,
                float(limit),
                passed,
            )
            if not passed:
                dq_failures.append((f"max_null_rate:{col_name}", null_rate, float(limit)))

    # Check 3: Revenue reconciliation (fast native Spark aggregations)
    reconciled_revenue = silver_revenue + quarantine_revenue
    diff = abs(bronze_revenue - reconciled_revenue)
    rel_diff = (diff / bronze_revenue) if bronze_revenue > 0 else 0.0
    rev_passed = rel_diff <= rev_tol

    record_dq_result(
        batch_date=batch_date,
        table_name="orders",
        check_name="revenue_reconciliation",
        observed_value=f"rel_diff={rel_diff:.6f} (raw={bronze_revenue:.2f}, silver={silver_revenue:.2f}, q={quarantine_revenue:.2f})",
        threshold_value=f"tolerance={rev_tol:.6f}",
        passed=rev_passed,
        details=f"Absolute diff: {diff:.2f}",
    )
    logger.info(
        "DQ check 'revenue_reconciliation': raw=%.2f, silver=%.2f, quarantine=%.2f, rel_diff=%.6f, threshold=%.6f, pass=%s",
        bronze_revenue,
        silver_revenue,
        quarantine_revenue,
        rel_diff,
        rev_tol,
        rev_passed,
    )
    if not rev_passed:
        dq_failures.append(("revenue_reconciliation", rel_diff, rev_tol))

    # Free cached dataframes
    bronze_orders.unpersist()
    silver_orders.unpersist()
    if quarantine_orders:
        quarantine_orders.unpersist()

    end_time = datetime.now(UTC)

    # Fail fast if any DQ check failed
    if dq_failures:
        first_failure = dq_failures[0]
        check_name, obs, thresh = first_failure
        msg = f"Data quality check '{check_name}' failed. Observed: {obs}, Threshold: {thresh}"
        logger.error(msg)
        record_pipeline_run(
            dag_run_id=dag_run_id,
            task_id=task_id,
            batch_date=batch_date,
            status="FAILED",
            rows_in=b_count,
            rows_out=s_count,
            error_type="DataQualityError",
            error_message=msg,
            started_at=start_time,
            ended_at=end_time,
        )
        raise DataQualityError(
            check_name=check_name,
            observed_value=obs,
            threshold_value=thresh,
            message=msg,
        )

    logger.info("All Data Quality checks passed successfully for batch %s", batch_date)
    record_pipeline_run(
        dag_run_id=dag_run_id,
        task_id=task_id,
        batch_date=batch_date,
        status="SUCCESS",
        rows_in=b_count,
        rows_out=s_count,
        started_at=start_time,
        ended_at=end_time,
    )

    return {
        "batch_date": batch_date,
        "passed": True,
        "bronze_rows": b_count,
        "silver_rows": s_count,
        "quarantine_rows": q_count,
    }
