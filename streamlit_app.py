import streamlit as st
import pandas as pd

st.set_page_config(page_title="📈 Positional Trade Scanner", layout="wide")
st.title("📈 Positional Trade Scanner")

# ==================================================
# LOGGER
# ==================================================
st.subheader("🧠 Execution Log")
log_box = st.empty()
logs = []

def ui_logger(msg: str):
    logs.append(msg)
    log_box.markdown("\n".join(logs))

st.success("UI loaded successfully")

# ==================================================
# SAFE IMPORTS (NO BLANK SCREEN)
# ==================================================
try:
    from ingestion_engine import ingest_csv
    from fast_scan_engine import run_fast_scan
except Exception as e:
    st.error(f"❌ Import failed: {e}")
    st.stop()
st.info(
    "ℹ️ Upload CSV only when new trading day data is available. "
    "You can run scan anytime without uploading CSV."
)

# ==================================================
# CSV UPLOAD
# ==================================================
st.subheader("📤 Upload Daily CSV")

csv_type = st.radio(
    "Select CSV Type",
    ["Stock", "Index"],
    horizontal=True
)

index_name = None
if csv_type == "Index":
    index_name = st.selectbox(
        "Select Index",
        ["NIFTY 50", "BANKNIFTY"]
    )

uploaded_files = st.file_uploader(
    "Upload CSV file(s)",
    type=["csv"],
    accept_multiple_files=True
)

if st.button("⬆️ Validate & Append"):
    logs.clear()

    if not uploaded_files:
        st.error("Please upload at least one CSV")
        st.stop()

    df = pd.concat(
        [pd.read_csv(f) for f in uploaded_files],
        ignore_index=True
    )

    try:
        ingest_csv(
            df,
            data_type="stock" if csv_type == "Stock" else "index",
            source="raw",
            index_name=index_name,
            logger=ui_logger
        )
        st.success("✅ CSV validated and appended")
    except Exception as e:
        st.error(str(e))
        st.stop()

# ==================================================
# RUN SCAN
# ==================================================
st.divider()
st.subheader("🚀 Run Positional Scan")

if st.button("🚀 Run Scan"):
    logs.clear()
    ui_logger("🚀 Scan started")

    top20_df = run_fast_scan(
        logger=ui_logger,
        top_n=20
    )

    if top20_df.empty:
        st.warning("⚠️ No stocks due to Risk-OFF regime")
    else:
        st.subheader("🏆 TOP-20 POSITIONAL STOCKS")
        st.dataframe(top20_df, use_container_width=True)

