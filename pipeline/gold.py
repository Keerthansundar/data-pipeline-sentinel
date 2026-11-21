"""Gold business aggregation layer."""

from datetime import UTC, datetime
from typing import Any

from pyspark.sql.functions import avg, broadcast, count, round, sum

from pipeline.config import get_project_root
from pipeline.logging_config import get_logger
from pipeline.observability import record_pipeline_run
from pipeline.spark_session import get_spark_session

logger = get_logger(__name__)


def build_gold(
    batch_date: str,
    dag_run_id: str = "manual",
    task_id: str = "gold_build",
) -> dict[str, Any]:
    """Build Gold tables: daily_revenue, revenue_by_channel, customer_summary.

    Args:
        batch_date: Target batch date YYYY-MM-DD.
        dag_run_id: DagRun ID for observability tracking.
        task_id: Task name.

    Returns:
        Summary dict containing status and metrics.
    """
    start_time = datetime.now(UTC)
    root = get_project_root()
    silver_dir = root / "data" / "silver"
    gold_dir = root / "data" / "gold"

    logger.info("Starting Gold build for batch_date=%s", batch_date)

    spark = get_spark_session("GoldBuild")
    silver_orders_path = silver_dir / "orders"
    silver_cust_path = silver_dir / "customers"

    if not silver_orders_path.exists():
        raise FileNotFoundError(f"Silver orders missing at {silver_orders_path}")

    orders_df = (
        spark.read.format("delta")
        .load(str(silver_orders_path))
        .filter(f"_batch_date = '{batch_date}'")
    ).cache()
    rows_in = orders_df.count()

    # 1. Gold: daily_revenue
    daily_rev_df = (
        orders_df.groupBy("_batch_date")
        .agg(
            round(sum("amount"), 2).alias("total_revenue"),
            count("order_id").alias("order_count"),
            round(avg("amount"), 2).alias("avg_order_value"),
        )
        .coalesce(1)
    )
    daily_rev_path = gold_dir / "daily_revenue"
    daily_rev_path.mkdir(parents=True, exist_ok=True)
    daily_rev_df.write.format("delta").mode("overwrite").option(
        "replaceWhere", f"_batch_date = '{batch_date}'"
    ).save(str(daily_rev_path))

    total_revenue = daily_rev_df.select("total_revenue").first()[0]

    # 2. Gold: revenue_by_channel
    channel_rev_df = (
        orders_df.groupBy("_batch_date", "channel")
        .agg(
            round(sum("amount"), 2).alias("channel_revenue"),
            count("order_id").alias("order_count"),
        )
        .coalesce(1)
    )
    channel_rev_path = gold_dir / "revenue_by_channel"
    channel_rev_path.mkdir(parents=True, exist_ok=True)
    channel_rev_df.write.format("delta").mode("overwrite").option(
        "replaceWhere", f"_batch_date = '{batch_date}'"
    ).save(str(channel_rev_path))

    # 3. Gold: customer_summary (join with customers)
    if silver_cust_path.exists():
        cust_df = (
            spark.read.format("delta")
            .load(str(silver_cust_path))
            .filter(f"_batch_date = '{batch_date}'")
            .select("customer_id", "name", "segment")
        )
        cust_summary_df = (
            orders_df.join(broadcast(cust_df), on="customer_id", how="left")
            .groupBy("customer_id", "name", "segment", "_batch_date")
            .agg(
                round(sum("amount"), 2).alias("customer_revenue"),
                count("order_id").alias("total_orders"),
            )
            .coalesce(1)
        )
        cust_summary_path = gold_dir / "customer_summary"
        cust_summary_path.mkdir(parents=True, exist_ok=True)
        cust_summary_df.write.format("delta").mode("overwrite").option(
            "replaceWhere", f"_batch_date = '{batch_date}'"
        ).save(str(cust_summary_path))

    orders_df.unpersist()
    end_time = datetime.now(UTC)
    duration_s = (end_time - start_time).total_seconds()

    logger.info(
        "Gold build completed: batch=%s, total_revenue=%.2f, orders=%d, duration=%.2fs",
        batch_date,
        float(total_revenue or 0.0),
        rows_in,
        duration_s,
    )

    record_pipeline_run(
        dag_run_id=dag_run_id,
        task_id=task_id,
        batch_date=batch_date,
        status="SUCCESS",
        rows_in=rows_in,
        rows_out=rows_in,
        started_at=start_time,
        ended_at=end_time,
    )

    return {
        "batch_date": batch_date,
        "status": "SUCCESS",
        "total_revenue": float(total_revenue or 0.0),
        "orders_count": rows_in,
        "duration_seconds": duration_s,
    }
