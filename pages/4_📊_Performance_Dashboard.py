import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(
    page_title="📊 Performance Dashboard",
    layout="wide"
)

PERF_FILE = Path("data/forward_performance.parquet")

st.title("📊 Positional Scanner – Performance Dashboard")

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
if not PERF_FILE.exists():
    st.error("❌ forward_performance.parquet not found")
    st.stop()

df = pd.read_parquet(PERF_FILE)

if df.empty:
    st.warning("⚠️ No forward performance data yet")
    st.stop()

# --------------------------------------------------
# BASIC METRICS
# --------------------------------------------------
st.subheader("📈 Overall Performance Snapshot")

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total Signals", len(df))
col2.metric("Hit 1R %", f"{df['hit_1R'].mean() * 100:.1f}%")
col3.metric("Hit 2R %", f"{df['hit_2R'].mean() * 100:.1f}%")
col4.metric("Hit 3R %", f"{df['hit_3R'].mean() * 100:.1f}%")
col5.metric("SL Hit %", f"{df['hit_SL'].mean() * 100:.1f}%")

st.divider()

# --------------------------------------------------
# PERFORMANCE BY TIMEFRAME
# --------------------------------------------------
st.subheader("⏱️ Performance by Timeframe")

tf_df = (
    df.groupby("tf")
    .agg(
        Signals=("symbol", "count"),
        Hit_1R_pct=("hit_1R", "mean"),
        Hit_2R_pct=("hit_2R", "mean"),
        Avg_MFE_R=("mfe_R", "mean"),
        Avg_MAE_R=("mae_R", "mean"),
    )
    .reset_index()
)

tf_df["Hit_1R_pct"] *= 100
tf_df["Hit_2R_pct"] *= 100

st.dataframe(tf_df, use_container_width=True)

st.divider()

# --------------------------------------------------
# PERFORMANCE BY ZONE STRENGTH
# --------------------------------------------------
st.subheader("🔥 Performance by Zone Strength")

zone_df = (
    df.groupby("zone_strength")
    .agg(
        Signals=("symbol", "count"),
        Hit_1R_pct=("hit_1R", "mean"),
        Hit_2R_pct=("hit_2R", "mean"),
        Avg_MFE_R=("mfe_R", "mean"),
        Avg_MAE_R=("mae_R", "mean"),
    )
    .reset_index()
)

zone_df["Hit_1R_pct"] *= 100
zone_df["Hit_2R_pct"] *= 100

st.dataframe(zone_df, use_container_width=True)

st.divider()

# --------------------------------------------------
# TOP PERFORMERS
# --------------------------------------------------
st.subheader("🏆 Top Performing Trades (MFE ≥ 2R)")

top_df = df[df["mfe_R"] >= 2].sort_values("mfe_R", ascending=False)

if top_df.empty:
    st.info("ℹ️ No trades have reached 2R yet")
else:
    st.dataframe(
        top_df[
            [
                "symbol",
                "scan_date",
                "tf",
                "zone_strength",
                "mfe_R",
                "mae_R",
                "hit_1R",
                "hit_2R",
                "hit_3R",
            ]
        ],
        use_container_width=True
    )

st.divider()

# --------------------------------------------------
# RAW TABLE (OPTIONAL)
# --------------------------------------------------
with st.expander("🔍 View Raw Forward Performance Data"):
    st.dataframe(df, use_container_width=True)
