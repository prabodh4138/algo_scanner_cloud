import streamlit as st
import pandas as pd

from decision_engine.utils.timeframe_resampler import build_timeframes
from decision_engine.htf_zones.htf_demand_zone_engine import detect_htf_demand_zones
from decision_engine.htf_zones.htf_supply_zone_engine import detect_htf_supply_zones
from decision_engine.zone_strength.htf_zone_strength_scorer import score_htf_zones
from decision_engine.alignment.daily_htf_alignment_gate import apply_daily_htf_alignment
from decision_engine.confidence.directional_confidence_engine import compute_directional_confidence
from decision_engine.execution.execution_engine import build_execution_plan

# ===============================
# CONFIG
# ===============================
SCAN_BASE_PATH = "scan_base.parquet"  # cloud-safe later

st.set_page_config(
    page_title="📒 Trade Blotter",
    layout="wide"
)

st.title("📒 Trade Blotter — Execution Ready Trades")

st.caption(
    "This page shows FINAL trades after HTF alignment, confidence scoring, "
    "and capital allocation. No logic runs inside the UI."
)

# ===============================
# USER CONTROLS
# ===============================
with st.sidebar:
    st.header("⚙️ Controls")

    total_capital = st.number_input(
        "Total Capital (₹)",
        min_value=100_000,
        value=1_000_000,
        step=50_000,
    )

    min_confidence = st.slider(
        "Minimum Directional Confidence",
        min_value=40,
        max_value=90,
        value=60,
        step=5,
    )

    refresh = st.button("🔄 Refresh Trades")

# ===============================
# CORE PIPELINE (FAST)
# ===============================
@st.cache_data(show_spinner=False)
def generate_execution_table(total_capital):
    daily, weekly, monthly = build_timeframes(SCAN_BASE_PATH)

    latest_daily = (
        daily
        .sort_values("trade_date")
        .groupby("symbol")
        .tail(1)
        .reset_index(drop=True)
        [["symbol", "close"]]
    )

    demand = detect_htf_demand_zones(weekly, monthly)
    supply = detect_htf_supply_zones(weekly, monthly)

    htf_zones = score_htf_zones(
        pd.concat([demand, supply], ignore_index=True)
    )

    aligned = apply_daily_htf_alignment(
        latest_daily,
        htf_zones,
    )

    confident = compute_directional_confidence(
        aligned,
        htf_zones,
    )

    execution = build_execution_plan(
        confident,
        total_capital=total_capital,
    )

    return execution


# ===============================
# LOAD DATA
# ===============================
if refresh:
    st.cache_data.clear()

execution_df = generate_execution_table(total_capital)

# ===============================
# FILTERS
# ===============================
filtered = execution_df[
    execution_df["directional_confidence"] >= min_confidence
].copy()

# ===============================
# DISPLAY
# ===============================
if filtered.empty:
    st.warning("⚠️ No executable trades for current conditions.")
else:
    st.subheader("✅ Execution Ready Trades")

    st.dataframe(
        filtered.sort_values(
            "directional_confidence",
            ascending=False
        ),
        use_container_width=True
    )

    st.divider()

    st.subheader("📊 Summary")

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Total Trades",
        len(filtered)
    )

    col2.metric(
        "Total Risk (₹)",
        int(filtered["risk_per_trade"].sum())
    )

    col3.metric(
        "Avg Confidence",
        round(filtered["directional_confidence"].mean(), 1)
    )
