import streamlit as st
import pandas as pd
from decision_engine.utils.data_cleaner import (
    clean_stock_df,
    clean_index_df,
)
from utils.supabase_rest_client import supabase_insert
st.divider()
st.subheader("📊 Index Daily Data")

index_name = st.text_input(
    "Index Name (example: NIFTY_50, BANKNIFTY)",
    value=""
)

index_file = st.file_uploader(
    "Upload Index CSV",
    type=["csv"],
    key="index_csv"
)

if index_file is not None:
    if not index_name.strip():
        st.warning("⚠️ Please enter Index Name before upload")
    else:
        raw_df = pd.read_csv(index_file)

        try:
            clean_df = clean_index_df(raw_df, index_name=index_name)
        except Exception as e:
            st.error(f"❌ Cleaning failed: {e}")
        else:
            st.success("✅ Index CSV cleaned successfully")
            st.info(f"Clean rows ready: {len(clean_df)}")
            st.info(f"Latest trade_date in upload: {clean_df['trade_date'].max()}")

            st.dataframe(clean_df.head(), use_container_width=True)

            if st.button("🚀 Upload Cleaned Index Data"):
                supabase_insert(
                    "index_daily_raw",
                    clean_df.to_dict("records")
                )
                st.success("✅ Index data uploaded to Supabase")
