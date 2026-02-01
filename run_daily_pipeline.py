import datetime
import pandas as pd

from decision_engine.pipeline import run_pipeline
from utils.supabase_rest_client import supabase_insert, supabase_select


# ==================================================
# CONFIG
# ==================================================
CAPITAL = 1_000_000
BLOTTER_TABLE = "trade_blotter_daily"


# ==================================================
# GET LAST INSERTED DATE (INCREMENTAL)
# ==================================================
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


# ==================================================
# MAIN ENTRY (RUN BY GITHUB ACTION)
# ==================================================
if __name__ == "__main__":
    print("🚀 Running daily trade pipeline from GitHub Actions")

    last_date = get_last_trade_date()
    print("Last processed date:", last_date)

    execution_df = run_pipeline(
        parquet_path=None,   # Supabase-native input handled inside pipeline
        total_capital=CAPITAL,
    )

    if execution_df.empty:
        print("⚠️ No trades generated")
        exit(0)

    today = datetime.date.today()
    execution_df["trade_date"] = today

    # --------------------------------------------------
    # OPTIONAL SAFETY: FILTER IF SAME DAY ALREADY EXISTS
    # --------------------------------------------------
    if last_date and last_date >= today:
        print("✅ Trades already generated for today. Skipping insert.")
        exit(0)

    supabase_insert(
        table=BLOTTER_TABLE,
        records=execution_df.to_dict("records"),
        on_conflict="trade_date,symbol",
    )

    print(f"✅ Inserted {len(execution_df)} trades into Supabase")
