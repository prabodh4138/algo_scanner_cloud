import streamlit as st
import pandas as pd

FORWARD_PATH = "data/forward_trades.parquet"

st.set_page_config(
    page_title="📊 Performance Dashboard",
    layout="wide"
)

st.title("📊 Performance Dashboard — Forward Results")
st.caption(
    "This dashboard shows REAL forward performance. "
    "No backtests. No repainting. No curve fitting."
)

# ===============================
# LOAD DATA
# ===============================
@st.cache_data(show_spinner=False)
def load_forward_trades():
    try:
        return pd.read_parquet(FORWARD_PATH)
    except Exception:
        return pd.DataFrame()

df = load_forward_trades()

if df.empty:
    st.warning("⚠️ No forward trades found yet.")
    st.stop()

# ===============================
# CLOSED TRADES ONLY
# ===============================
closed = df[df["exit_reason"] != "OPEN"].copy()

if closed.empty:
    st.warning("⚠️ Trades exist but none closed yet.")
    st.stop()

# ===============================
# METRICS
# ===============================
total_trades = len(closed)
wins = closed[closed["pnl"] > 0]
losses = closed[closed["pnl"] <= 0]

win_rate = len(wins) / total_trades * 100
avg_r = closed["r_multiple"].mean()
total_pnl = closed["pnl"].sum()

# ===============================
# SUMMARY
# ===============================
st.subheader("📌 Summary")

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Trades", total_trades)
c2.metric("Win Rate (%)", round(win_rate, 2))
c3.metric("Avg R Multiple", round(avg_r, 2))
c4.metric("Total PnL (₹)", round(total_pnl, 0))

st.divider()

# ===============================
# EQUITY CURVE
# ===============================
st.subheader("📈 Equity Curve")

closed = closed.sort_values("exit_date")
closed["equity"] = closed["pnl"].cumsum()

st.line_chart(
    closed.set_index("exit_date")["equity"]
)

# ===============================
# CONFIDENCE BUCKET ANALYSIS
# ===============================
st.subheader("🎯 Confidence Bucket Analysis")

bins = [0, 55, 65, 75, 100]
labels = ["<55", "55–65", "65–75", "75+"]

closed["confidence_bucket"] = pd.cut(
    closed["directional_confidence"],
    bins=bins,
    labels=labels
)

bucket_stats = (
    closed
    .groupby("confidence_bucket")
    .agg(
        trades=("symbol", "count"),
        win_rate=("pnl", lambda x: (x > 0).mean() * 100),
        avg_r=("r_multiple", "mean"),
        total_pnl=("pnl", "sum"),
    )
    .reset_index()
)

st.dataframe(bucket_stats, use_container_width=True)

st.divider()

# ===============================
# EXIT REASONS
# ===============================
st.subheader("🚦 Exit Breakdown")

exit_counts = closed["exit_reason"].value_counts()

st.bar_chart(exit_counts)

st.divider()

# ===============================
# RAW DATA (OPTIONAL)
# ===============================
with st.expander("🔍 View Raw Closed Trades"):
    st.dataframe(
        closed.sort_values("exit_date", ascending=False),
        use_container_width=True
    )
