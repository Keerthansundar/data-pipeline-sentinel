"""Silver layer cleaning, aliasing, and quarantine partitioning."""

from datetime import UTC, datetime
from typing import Any

from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from pipeline.config import get_project_root, load_yaml_config
from pipeline.errors import FormatDriftError, MissingColumnError
from pipeline.logging_config import get_logger
from pipeline.observability import record_pipeline_run
from pipeline.profiling import profile_batch
from pipeline.spark_session import get_spark_session

logger = get_logger(__name__)


def transform_silver(
    batch_date: str,
    dag_run_id: str = "manual",
    task_id: str = "silver_transform",
    rules_file_path: str = "config/silver_rules.yml",
) -> dict[str, Any]:
    """Transform Bronze data to Silver applying silver_rules.yml.

    Args:
        batch_date: Batch date string in YYYY-MM-DD format.
        dag_run_id: DagRun ID for observability tracking.
        task_id: Task name.
        rules_file_path: Relative path to silver rules YAML.

    Returns:
        Summary dict containing rows out and rows quarantined.
    """
    start_time = datetime.now(UTC)
    root = get_project_root()
    bronze_dir = root / "data" / "bronze"
    silver_dir = root / "data" / "silver"
    quarantine_dir = root / "data" / "quarantine"

    logger.info(
        "Starting Silver transform: batch_date=%s, rules_file=%s",
        batch_date,
        rules_file_path,
    )

    rules = load_yaml_config(rules_file_path)
    orders_rules = rules.get("orders", {})
    required_cols = orders_rules.get("required_columns", [])
    aliases = orders_rules.get("column_aliases", {})
    accepted_formats = orders_rules.get("date_formats", {}).get("order_date", ["%Y-%m-%d"])
    max_unparseable_ratio = float(orders_rules.get("max_unparseable_ratio", 0.01))

    spark = get_spark_session("SilverTransform")
    bronze_orders_path = bronze_dir / "orders"
    bronze_cust_path = bronze_dir / "customers"

    if not bronze_orders_path.exists():
        raise FileNotFoundError(f"Bronze orders Delta table missing at {bronze_orders_path}")

    orders_df = (
        spark.read.format("delta")
        .load(str(bronze_orders_path))
        .filter(f"_batch_date = '{batch_date}'")
    )

    # 1. Convert to pandas for robust checking, parsing, and quarantine splitting
    pdf = orders_df.toPandas()
    rows_in = len(pdf)
    data_cols = [c for c in pdf.columns if not c.startswith("_")]
    populated_cols = [c for c in data_cols if pdf[c].notna().sum() > 0]

    # Check required columns and apply aliases
    for req_col in required_cols:
        is_missing = (req_col not in pdf.columns) or (pdf[req_col].notna().sum() == 0)
        if is_missing:
            col_aliases = aliases.get(req_col, [])
            mapped = False
            for alias in col_aliases:
                if alias in pdf.columns and pdf[alias].notna().sum() > 0:
                    logger.info("Applying column alias: %s -> %s", alias, req_col)
                    pdf[req_col] = pdf[alias]
                    mapped = True
                    break
            if not mapped:
                msg = f"Missing required column '{req_col}'; found columns: {populated_cols}"
                logger.error(msg)
                record_pipeline_run(
                    dag_run_id,
                    task_id,
                    batch_date,
                    "FAILED",
                    rows_in=rows_in,
                    error_type="MissingColumnError",
                    error_message=msg,
                    started_at=start_time,
                )
                raise MissingColumnError(missing_column=req_col, available_columns=populated_cols)

    # 2. Fast vectorized date parsing and format drift check
    import pandas as pd

    date_series = pdf["order_date"].astype(str).str.strip()
    parsed_s = pd.Series(pd.NaT, index=pdf.index, dtype="datetime64[ns]")

    for fmt in accepted_formats:
        remaining = parsed_s.isna() & ~date_series.isin(["", "None", "nan"])
        if not remaining.any():
            break
        parsed_s[remaining] = pd.to_datetime(date_series[remaining], format=fmt, errors="coerce")

    parse_success = parsed_s.notna()
    parsed_dates = parsed_s.dt.strftime("%Y-%m-%d").where(parse_success, None)

    total_valid_date_targets = len(pdf)
    unparseable_mask = ~parse_success
    unparseable_count = int(unparseable_mask.sum())
    unparseable_ratio = (
        float(unparseable_count / total_valid_date_targets) if total_valid_date_targets > 0 else 0.0
    )

    bad_samples = [
        str(v)
        for v in pdf.loc[unparseable_mask, "order_date"].dropna().unique()
        if str(v).strip() not in ("", "None", "nan")
    ][:5]

    logger.info(
        "Date parsing results for column 'order_date': total=%d, unparseable=%d (%.2f%%), accepted_formats=%s",
        total_valid_date_targets,
        unparseable_count,
        unparseable_ratio * 100,
        accepted_formats,
    )

    if unparseable_ratio > max_unparseable_ratio:
        msg = (
            f"unparseable date ratio {unparseable_ratio:.2%} exceeds max {max_unparseable_ratio:.2%} "
            f"(column=order_date, accepted_formats={accepted_formats}, sample_bad_values={bad_samples})"
        )
        logger.error(msg)
        record_pipeline_run(
            dag_run_id,
            task_id,
            batch_date,
            "FAILED",
            rows_in=rows_in,
            error_type="FormatDriftError",
            error_message=msg,
            started_at=start_time,
        )
        raise FormatDriftError(
            column="order_date",
            unparseable_ratio=unparseable_ratio,
            threshold=max_unparseable_ratio,
            accepted_formats=accepted_formats,
            sample_bad_values=bad_samples,
            message=msg,
        )

    # 3. Fast Vectorized Quarantine Classification
    pdf["_parsed_order_date"] = parsed_dates
    reasons = pd.Series(None, index=pdf.index, dtype=object)

    # Low priority: unparseable date
    reasons.loc[unparseable_mask] = "unparseable_date"

    # Medium priority: invalid or negative amount
    amt_num = pd.to_numeric(pdf["amount"], errors="coerce")
    reasons.loc[amt_num.isna() & pdf["amount"].notna()] = "invalid_amount_format"
    reasons.loc[amt_num < 0] = "negative_amount"

    # High priority: null keys
    null_cust = pdf["customer_id"].isna() | pdf["customer_id"].astype(str).str.strip().isin(
        ["", "None", "nan"]
    )
    reasons.loc[null_cust] = "null_key:customer_id"

    null_order = pdf["order_id"].isna() | pdf["order_id"].astype(str).str.strip().isin(
        ["", "None", "nan"]
    )
    reasons.loc[null_order] = "null_key:order_id"

    pdf["_quarantine_reason"] = reasons
    quarantined_pdf = pdf[reasons.notna()].copy()
    silver_pdf = pdf[reasons.isna()].copy()

    # Deduplicate silver on order_id
    silver_pdf = silver_pdf.drop_duplicates(subset=["order_id"])
    silver_pdf["order_date"] = silver_pdf["_parsed_order_date"]
    silver_pdf["amount"] = pd.to_numeric(silver_pdf["amount"], errors="coerce").fillna(0.0)
    silver_pdf = silver_pdf.drop(columns=["_parsed_order_date", "_quarantine_reason"])

    # 4. Save Quarantine Delta table
    rows_quarantined = len(quarantined_pdf)
    if rows_quarantined > 0:
        quarantine_orders_path = quarantine_dir / "orders"
        quarantine_orders_path.mkdir(parents=True, exist_ok=True)
        quarantine_schema = StructType(
            [
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("order_date", StringType(), True),
                StructField("amount", StringType(), True),
                StructField("status", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("_batch_date", StringType(), True),
                StructField("_quarantine_reason", StringType(), True),
            ]
        )
        q_clean_pdf = quarantined_pdf[
            [
                "order_id",
                "customer_id",
                "order_date",
                "amount",
                "status",
                "channel",
                "_batch_date",
                "_quarantine_reason",
            ]
        ].astype(str)
        q_df = spark.createDataFrame(q_clean_pdf, schema=quarantine_schema).coalesce(1)
        q_df.write.format("delta").mode("overwrite").option(
            "replaceWhere", f"_batch_date = '{batch_date}'"
        ).save(str(quarantine_orders_path))

    # 5. Save Silver Orders Delta table
    rows_out = len(silver_pdf)
    silver_orders_path = silver_dir / "orders"
    silver_orders_path.mkdir(parents=True, exist_ok=True)
    silver_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("_batch_date", StringType(), True),
        ]
    )
    s_df = spark.createDataFrame(silver_pdf, schema=silver_schema).coalesce(1)
    s_df.write.format("delta").mode("overwrite").option(
        "replaceWhere", f"_batch_date = '{batch_date}'"
    ).save(str(silver_orders_path))

    # 6. Transform and Save Customers Silver (PII Masking)
    if bronze_cust_path.exists():
        cust_df = (
            spark.read.format("delta")
            .load(str(bronze_cust_path))
            .filter(f"_batch_date = '{batch_date}'")
        )
        # Email masking: 'e***@example.com'
        masked_cust_df = (
            cust_df.withColumn(
                "email",
                when(col("email").isNotNull(), lit("[REDACTED_PII]")).otherwise(None),
            )
            .dropDuplicates(["customer_id"])
            .coalesce(1)
        )

        silver_cust_path = silver_dir / "customers"
        silver_cust_path.mkdir(parents=True, exist_ok=True)
        masked_cust_df.write.format("delta").mode("overwrite").option(
            "replaceWhere", f"_batch_date = '{batch_date}'"
        ).save(str(silver_cust_path))

    # 7. Compute Column Profiles (reusing in-memory DataFrame)
    profile_batch(batch_date, pdf=pdf)

    end_time = datetime.now(UTC)
    duration_s = (end_time - start_time).total_seconds()

    logger.info(
        "Silver transform completed: batch=%s, rows_in=%d, rows_out=%d, rows_quarantined=%d, duration=%.2fs",
        batch_date,
        rows_in,
        rows_out,
        rows_quarantined,
        duration_s,
    )

    record_pipeline_run(
        dag_run_id=dag_run_id,
        task_id=task_id,
        batch_date=batch_date,
        status="SUCCESS",
        rows_in=rows_in,
        rows_out=rows_out,
        rows_quarantined=rows_quarantined,
        started_at=start_time,
        ended_at=end_time,
    )

    return {
        "batch_date": batch_date,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_quarantined": rows_quarantined,
        "duration_seconds": duration_s,
    }
