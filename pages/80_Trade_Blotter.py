import streamlit as st
import pandas as pd

from utils.supabase_rest_client import supabase_select


# ======================================================
# PAGE CONFIG
# ======================================================
st.set_page_config(page_title="Trade Blotter", layout="wide")

st.title("📒 Trade Blotter")
st.caption("Live trades sourced from Supabase")


# ======================================================
# LOAD DATA FROM SUPABASE
# ======================================================
@st.cache_data(ttl=60)
def load_trade_blotter():
    rows = supabase_select(
        table_name="portfolio_top20_dynamic",
        order="confidence_score.desc",
        limit=20
    )
    return pd.DataFrame(rows)


df = load_trade_blotter()


# ======================================================
# UI RENDER
# ======================================================
if df.empty:
    st.info("ℹ️ No trades available yet.")
else:
    st.success(f"✅ Showing {len(df)} trades")

    display_cols = [
        "portfolio_rank",
        "symbol",
        "trend",
        "zone_timeframe",
        "entry",
        "stoploss",
        "target",
        "quantity",
        "confidence_score",
    ]

    available_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(
        df[available_cols],
        use_container_width=True
    )
