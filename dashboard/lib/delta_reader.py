"""Delta Lake table reader using the deltalake package without Spark."""

from datetime import datetime
from typing import Any

import pandas as pd

from pipeline.config import get_project_root
from pipeline.logging_config import get_logger

logger = get_logger(__name__)


def _load_delta_table(relative_path: str) -> pd.DataFrame:
    """Read a Delta table directly from filesystem without Spark.

    Args:
        relative_path: Relative path under project root (e.g. 'data/gold/daily_revenue').

    Returns:
        Pandas DataFrame of table contents, or empty DataFrame on failure/empty state.
    """
    root = get_project_root()
    table_dir = root / relative_path

    if not table_dir.exists():
        logger.debug("Delta table path does not exist: %s", table_dir)
        return pd.DataFrame()

    try:
        from deltalake import DeltaTable

        dt = DeltaTable(str(table_dir))
        return dt.to_pandas()
    except Exception as exc:
        logger.debug("Could not read Delta table at %s: %s", table_dir, exc)
        return pd.DataFrame()


def read_gold_daily_revenue() -> pd.DataFrame:
    """Read data/gold/daily_revenue into DataFrame.

    Real schema in Gold:
        _batch_date: string (YYYY-MM-DD)
        total_revenue: double
        order_count: bigint
        avg_order_value: double
    """
    df = _load_delta_table("data/gold/daily_revenue")
    if df.empty:
        return pd.DataFrame(
            columns=[
                "_batch_date",
                "order_date",
                "total_revenue",
                "order_count",
                "total_orders",
                "avg_order_value",
            ]
        )

    # Alias _batch_date to order_date for charts if order_date is missing
    if "_batch_date" in df.columns and "order_date" not in df.columns:
        df["order_date"] = pd.to_datetime(df["_batch_date"]).dt.strftime("%Y-%m-%d")
    elif "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"]).dt.strftime("%Y-%m-%d")

    # Ensure total_orders alias exists alongside order_count
    if "order_count" in df.columns and "total_orders" not in df.columns:
        df["total_orders"] = df["order_count"]
    elif "total_orders" in df.columns and "order_count" not in df.columns:
        df["order_count"] = df["total_orders"]

    date_col = "order_date" if "order_date" in df.columns else "_batch_date"
    if date_col in df.columns:
        df = df.sort_values(by=date_col, ascending=True)

    return df


def read_gold_channel_revenue() -> pd.DataFrame:
    """Read data/gold/revenue_by_channel into DataFrame.

    Real schema in Gold:
        _batch_date: string (YYYY-MM-DD)
        channel: string
        channel_revenue: double
        order_count: bigint
    """
    df = _load_delta_table("data/gold/revenue_by_channel")
    if df.empty:
        return pd.DataFrame(
            columns=["_batch_date", "channel", "channel_revenue", "order_count", "total_revenue"]
        )

    if "channel_revenue" in df.columns and "total_revenue" not in df.columns:
        df["total_revenue"] = df["channel_revenue"]
    if "order_count" in df.columns and "total_orders" not in df.columns:
        df["total_orders"] = df["order_count"]

    return df


def read_gold_customer_revenue() -> pd.DataFrame:
    """Read data/gold/customer_summary into DataFrame.

    Real schema in Gold:
        customer_id: string
        name: string
        segment: string
        _batch_date: string (YYYY-MM-DD)
        customer_revenue: double
        total_orders: bigint
    """
    df = _load_delta_table("data/gold/customer_summary")
    if df.empty:
        df = _load_delta_table("data/gold/customer_revenue")

    if df.empty:
        return pd.DataFrame(
            columns=[
                "customer_id",
                "name",
                "customer_name",
                "segment",
                "_batch_date",
                "customer_revenue",
                "lifetime_revenue",
                "total_orders",
                "lifetime_orders",
            ]
        )

    # Provide backward-compatible aliases
    if "name" in df.columns and "customer_name" not in df.columns:
        df["customer_name"] = df["name"]
    elif "customer_name" in df.columns and "name" not in df.columns:
        df["name"] = df["customer_name"]

    if "customer_revenue" in df.columns and "lifetime_revenue" not in df.columns:
        df["lifetime_revenue"] = df["customer_revenue"]
    elif "lifetime_revenue" in df.columns and "customer_revenue" not in df.columns:
        df["customer_revenue"] = df["lifetime_revenue"]

    if "total_orders" in df.columns and "lifetime_orders" not in df.columns:
        df["lifetime_orders"] = df["total_orders"]
    elif "lifetime_orders" in df.columns and "total_orders" not in df.columns:
        df["total_orders"] = df["lifetime_orders"]

    # Fill missing customer names from latest Silver/Bronze records across all batches
    try:
        lookup = get_customer_lookup_map()
        if lookup and "customer_id" in df.columns:
            for idx in df.index:
                cid = df.at[idx, "customer_id"]
                cur_name = df.at[idx, "name"] if "name" in df.columns else None
                if pd.isna(cur_name) or str(cur_name).strip() in ("", "None", "nan"):
                    meta = lookup.get(cid, {})
                    resolved = meta.get("name", "Unknown")
                    df.at[idx, "name"] = resolved
                    df.at[idx, "customer_name"] = resolved
                    if "segment" in df.columns and (
                        pd.isna(df.at[idx, "segment"])
                        or str(df.at[idx, "segment"]).strip() in ("", "None", "nan")
                    ):
                        df.at[idx, "segment"] = meta.get("segment", "Unknown")
    except Exception as exc:
        logger.debug("Failed resolving customer lookup: %s", exc)

    return df


def get_customer_lookup_map() -> dict[str, dict[str, str]]:
    """Build a lookup map of customer_id -> {'name': ..., 'segment': ...} across all Silver/Bronze batches."""
    df = _load_delta_table("data/silver/customers")
    if df.empty:
        df = _load_delta_table("data/bronze/customers")
    if df.empty or "customer_id" not in df.columns:
        return {}

    if "_batch_date" in df.columns:
        df = df.sort_values(by="_batch_date", ascending=True)

    df_clean = df.dropna(subset=["customer_id"]).drop_duplicates(
        subset=["customer_id"], keep="last"
    )
    return {
        row["customer_id"]: {
            "name": row["name"] if pd.notna(row.get("name")) else "Unknown",
            "segment": row["segment"] if pd.notna(row.get("segment")) else "Unknown",
        }
        for _, row in df_clean.iterrows()
    }


def get_gold_refresh_time() -> datetime | None:
    """Determine the most recent refresh time of the Gold layer."""
    root = get_project_root()
    table_dir = root / "data" / "gold" / "daily_revenue"
    if not table_dir.exists():
        return None

    try:
        from deltalake import DeltaTable

        dt = DeltaTable(str(table_dir))
        history = dt.history(limit=1)
        if history and "timestamp" in history[0]:
            ts = history[0]["timestamp"]
            if isinstance(ts, int):
                return datetime.fromtimestamp(ts / 1000.0)
            elif isinstance(ts, datetime):
                return ts
    except Exception:
        pass

    try:
        mtime = table_dir.stat().st_mtime
        return datetime.fromtimestamp(mtime)
    except Exception:
        return None


def get_revenue_kpis() -> dict[str, Any]:
    """Calculate aggregated revenue KPIs from Gold daily revenue."""
    df = read_gold_daily_revenue()
    if df.empty or "total_revenue" not in df.columns:
        return {
            "total_revenue": 0.0,
            "total_orders": 0,
            "avg_order_value": 0.0,
            "last_refresh": None,
        }

    total_rev = float(df["total_revenue"].sum())

    if "order_count" in df.columns:
        total_ord = int(df["order_count"].sum())
    elif "total_orders" in df.columns:
        total_ord = int(df["total_orders"].sum())
    else:
        total_ord = 0

    aov = (total_rev / total_ord) if total_ord > 0 else 0.0
    refresh_time = get_gold_refresh_time()

    return {
        "total_revenue": total_rev,
        "total_orders": total_ord,
        "avg_order_value": aov,
        "last_refresh": refresh_time,
    }
