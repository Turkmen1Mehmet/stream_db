import streamlit as st
import sqlite3
import pandas as pd
import os
from db_merger import merge_and_process_databases

st.set_page_config(page_title="Veritabanı Görüntüleyici", layout="wide")
st.title("SQLite Veritabanı Yükleyici ve Görüntüleyici")

# Klasördeki dosyaları seçmek için bir fonksiyon
def file_selector(folder_path='.'):
    filenames = os.listdir(folder_path)
    selected_filename = st.selectbox('Bir dosya seçin:', filenames)
    return os.path.join(folder_path, selected_filename)

# Kullanıcıdan işlem seçimi
option = st.radio("Bir işlem seçin:", ("Veritabanı yükle", "Klasör seç ve birleştir"))

if option == "Veritabanı yükle":
    # Kullanıcı SQLite dosyasını yükler
    uploaded_file = st.file_uploader("SQLite veritabanı dosyasını seçin (.db)", type=["db"])

    if uploaded_file:
        with open("temp_database.db", "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.success("Veritabanı başarıyla yüklendi! Aşağıda önizleme yapabilirsiniz.")

        conn = sqlite3.connect("temp_database.db")
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql_query(query, conn)
        st.write("Veritabanındaki tablolar:", tables)

        if not tables.empty:
            first_table = tables["name"][0]
            st.write(f"Önizleme Yapılan Tablo: {first_table}")
            
            data = pd.read_sql_query(f'SELECT * FROM "{first_table}"', conn)

            if not data.empty:
                default_limit = 10
                row_limit = st.number_input(
                    "Gösterilecek satır sayısını girin:",
                    min_value=1,
                    max_value=len(data),
                    value=default_limit,
                    step=1,
                )
                show_all = st.button("Tüm Satırları Göster", key=f"show_all_{first_table}")

                if show_all:
                    st.write(data)
                else:
                    st.write(data.head(row_limit))
            else:
                st.warning(f"Tablo '{first_table}' boş.")
        else:
            st.warning("Veritabanında hiçbir tablo bulunamadı.")
        conn.close()

elif option == "Klasör seç ve birleştir":
    folder_path = st.text_input("Bir klasör yolu girin:")
    if folder_path and os.path.isdir(folder_path):
        st.success(f"Klasör bulundu: {folder_path}")
        selected_file = file_selector(folder_path)
        st.write(f"Seçilen dosya: {selected_file}")
        
        output_db_path = "merged_database.db"
        merge_and_process_databases(folder_path, output_db_path)
        st.success(f"Veritabanları başarıyla birleştirildi ve {output_db_path} olarak kaydedildi.")

        conn = sqlite3.connect(output_db_path)
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql_query(query, conn)
        st.write("Birleştirilmiş Veritabanındaki Tablolar:", tables)

        if not tables.empty:
            first_table = tables["name"][0]
            st.write(f"Önizleme Yapılan Tablo: {first_table}")
            
            data = pd.read_sql_query(f'SELECT * FROM "{first_table}"', conn)

            if not data.empty:
                default_limit = 10
                row_limit = st.number_input(
                    "Gösterilecek satır sayısını girin:",
                    min_value=1,
                    max_value=len(data),
                    value=default_limit,
                    step=1,
                )
                show_all = st.button("Tüm Satırları Göster", key=f"show_all_{first_table}")

                if show_all:
                    st.write(data)
                else:
                    st.write(data.head(row_limit))
            else:
                st.warning(f"Tablo '{first_table}' boş.")
        else:
            st.warning("Birleştirilmiş veritabanında hiçbir tablo bulunamadı.")
        conn.close()
