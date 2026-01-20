import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# --------------------------------------------------
# Make project root importable (Streamlit Cloud fix)
# --------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

# ======================================================
# PAGE CONFIG
# ======================================================
st.set_page_config(page_title="Data Upload", layout="wide")

st.title("📤 Equity CSV Upload")
st.caption("NSE Bhavcopy → Filter EQ → Clean → Upload")


# ======================================================
# FILE UPLOADER
# ======================================================
uploaded_file = st.file_uploader(
    "Upload NSE Bhavcopy CSV (sec_bhavdata_full_*.csv)",
    type=["csv"]
)


# ======================================================
# MAIN LOGIC (INDENTATION SAFE)
# ======================================================
if uploaded_file is not None:

    st.write(f"📄 File selected: {uploaded_file.name}")

    # ---------- READ CSV ----------
    try:
        raw_df = pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"❌ CSV read failed: {e}")
        st.stop()

    st.subheader("🔍 Raw CSV Preview")
    st.dataframe(raw_df.head(), use_container_width=True)
    st.write("Detected columns:", list(raw_df.columns))

    # ---------- CLEAN ----------
    try:
        clean_df = clean_equity_dataframe(raw_df)
    except Exception as e:
        st.error(f"❌ Cleaning failed: {e}")
        st.stop()

    st.success("✅ CSV cleaned successfully")
    st.info(f"Rows ready: {len(clean_df)}")
    st.info(
        f"Date range: {clean_df['trade_date'].min()} → {clean_df['trade_date'].max()}"
    )

    st.subheader("✅ Cleaned Data Preview")
    st.dataframe(clean_df.head(), use_container_width=True)

    # ---------- UPLOAD ----------
    if st.button("🚀 Upload EQ Data to Supabase"):
        supabase_insert(
            table_name="equity_daily_raw",
            rows=clean_df.to_dict("records")
        )
        st.success("✅ Data uploaded successfully")

else:
    st.info("📂 Please upload an NSE Bhavcopy CSV file to begin")
