import streamlit as st
import os
import pandas as pd
from etiket_merge_stream import etiket_merge_process
from ortakdb_streamlit import ortakdb_process

st.set_page_config(page_title="Veri İşleme ve Görüntüleme", layout="wide")

st.title("Veri İşleme ve Görüntüleme")

# Geçici dosya yolları
input_db_path = "temp_database.db"
merged_db_path = "merged_database.db"
filtered_db_path = "filtered_database.db"

if os.path.exists(input_db_path):
    # Etiket Merge İşlemi
    st.subheader("Step 1: Etiket Merge İşlemi")
    try:
        merged_data = etiket_merge_process(input_db_path, merged_db_path)
        st.success("Etiket Merge işlemi başarıyla tamamlandı!")
    except Exception as e:
        st.error(f"Etiket Merge işleminde hata: {e}")
        merged_data = pd.DataFrame()

    # Ortak DB İşlemi
    st.subheader("Step 2: Ortak DB İşlemi")
    try:
        filtered_data = ortakdb_process(merged_db_path, filtered_db_path)
        st.success("Ortak DB işlemi başarıyla tamamlandı!")
    except Exception as e:
        st.error(f"Ortak DB işleminde hata: {e}")
        filtered_data = pd.DataFrame()

    # Sonuçların Gösterimi
    if not filtered_data.empty:
        st.subheader("Processed Data Preview")
        default_limit = 10
        row_limit = st.number_input(
            "Enter number of rows to display:",
            min_value=1,
            max_value=len(filtered_data),
            value=default_limit,
            step=1,
        )
        show_all = st.button("Show All Rows")

        if show_all:
            st.write(f"Displaying all {len(filtered_data)} rows:")
            st.write(filtered_data)
        else:
            st.write(f"Displaying {min(row_limit, len(filtered_data))} rows:")
            st.write(filtered_data.head(row_limit))

        # İndirme seçeneği
        with open(filtered_db_path, "rb") as f:
            st.download_button(
                label="Download Processed Database",
                data=f,
                file_name="processed_database.db",
                mime="application/octet-stream",
            )
else:
    st.warning("Please upload a database on the main page first.")

