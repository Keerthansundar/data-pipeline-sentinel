import sys
import types

import pandas as pd

if "pyspark" not in sys.modules:
    fake_session = types.ModuleType("pipeline.spark_session")
    fake_session.get_spark_session = lambda *a, **k: None
    sys.modules.setdefault("pipeline.spark_session", fake_session)

from pipeline import profiling  # noqa: E402


def test_profile_batch_without_slash_dates(monkeypatch):
    monkeypatch.setattr(profiling, "record_column_profile", lambda **kwargs: None)
    pdf = pd.DataFrame(
        {
            "order_id": ["1", "2"],
            "order_date": ["2026-10-05", "2026-10-05"],
        }
    )
    result = profiling.profile_batch("2026-10-05", pdf=pdf)
    p = result["profiles"]["order_date"]
    assert p["patterns"]["ambiguous_slash_count"] == 0
    assert p["ambiguous_day_month_share"] == 0.0
