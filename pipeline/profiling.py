"""Dataset column and value profiling."""

from typing import Any

from pipeline.config import get_project_root
from pipeline.logging_config import get_logger
from pipeline.observability import record_column_profile
from pipeline.spark_session import get_spark_session

logger = get_logger(__name__)


def profile_batch(batch_date: str, pdf: Any | None = None) -> dict[str, Any]:
    """Compute column profile metrics including ambiguous_day_month_share.

    Args:
        batch_date: Target batch date YYYY-MM-DD.
        pdf: Optional pre-loaded Pandas DataFrame for the batch.

    Returns:
        Summary dictionary of profiled columns and metrics.
    """
    logger.info("Starting column profiling for batch_date=%s", batch_date)

    if pdf is None:
        spark = get_spark_session("Profiling")
        root = get_project_root()
        bronze_orders_path = root / "data" / "bronze" / "orders"

        if not bronze_orders_path.exists():
            logger.warning(
                "Bronze orders table not found at %s for profiling",
                bronze_orders_path,
            )
            return {"batch_date": batch_date, "profiles": {}}

        df = (
            spark.read.format("delta")
            .load(str(bronze_orders_path))
            .filter(f"_batch_date = '{batch_date}'")
        )

        active_cols = [c for c in df.columns if not c.startswith("_")]
        pdf = df.select(active_cols).toPandas()
    else:
        active_cols = [c for c in pdf.columns if not c.startswith("_")]
    total_rows = len(pdf)

    if total_rows == 0:
        logger.warning("No rows found in Bronze orders for batch %s", batch_date)
        return {"batch_date": batch_date, "profiles": {}}

    profiles = {}

    for col in active_cols:
        series = pdf[col]

        null_count = int(series.isna().sum() + (series == "").sum())
        null_rate = float(null_count / total_rows)
        distinct_count = int(series.nunique(dropna=True))

        patterns: dict[str, Any] = {}
        ambiguous_share = 0.0

        if col == "order_date":
            # Analyze date formats and ambiguity
            non_null_dates = [
                str(v)
                for v in series
                if v is not None and str(v).strip() != "" and str(v) != "None"
            ]
            slash_dates = [d for d in non_null_dates if "/" in d]
            iso_dates = [d for d in non_null_dates if "-" in d]

            patterns["iso_format_count"] = len(iso_dates)
            patterns["slash_format_count"] = len(slash_dates)

            if slash_dates:
                ambiguous_slash_count = 0
                for d in slash_dates:
                    parts = d.split("/")
                    if len(parts) == 3:
                        try:
                            p1, p2 = int(parts[0]), int(parts[1])
                            if p1 <= 12 and p2 <= 12:
                                ambiguous_slash_count += 1
                        except ValueError:
                            pass
                ambiguous_share = float(ambiguous_slash_count / len(slash_dates))

            patterns["ambiguous_slash_count"] = ambiguous_slash_count

        profiles[col] = {
            "total_rows": total_rows,
            "null_count": null_count,
            "null_rate": null_rate,
            "distinct_count": distinct_count,
            "patterns": patterns,
            "ambiguous_day_month_share": ambiguous_share,
        }

        record_column_profile(
            batch_date=batch_date,
            table_name="bronze.orders",
            column_name=col,
            total_rows=total_rows,
            null_count=null_count,
            null_rate=null_rate,
            distinct_count=distinct_count,
            patterns=patterns,
            ambiguous_day_month_share=ambiguous_share,
        )

    logger.info(
        "Completed column profiling for batch=%s, total_rows=%d, date_ambiguous_share=%.4f",
        batch_date,
        total_rows,
        profiles.get("order_date", {}).get("ambiguous_day_month_share", 0.0),
    )

    return {"batch_date": batch_date, "profiles": profiles}
