import datetime
import pandas as pd

from algo_scanner_cloud.decision_engine.pipeline import run_pipeline
from algo_scanner_cloud.utils.supabase_rest_client import (
    supabase_insert,
    supabase_select,
)

CAPITAL = 1_000_000
BLOTTER_TABLE = "trade_blotter_daily"


def get_last_trade_date():
    rows = supabase_select(
        table=BLOTTER_TABLE,
        columns="trade_date",
        order="trade_date.desc",
        limit=1,
    )
    if rows:
        return pd.to_datetime(rows[0]["trade_date"]).date()
    return None


if __name__ == "__main__":
    print("🚀 Running daily trade pipeline")

    execution_df = run_pipeline(
        parquet_path=None,
        total_capital=CAPITAL,
    )

    if execution_df.empty:
        print("⚠️ No trades generated")
        exit(0)

    execution_df["trade_date"] = datetime.date.today()

    supabase_insert(
        table=BLOTTER_TABLE,
        records=execution_df.to_dict("records"),
        on_conflict="trade_date,symbol",
    )

    print(f"✅ Inserted {len(execution_df)} trades")
