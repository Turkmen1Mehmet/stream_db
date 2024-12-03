import streamlit as st
import sqlite3
import pandas as pd
import os
from db_merger import merge_and_process_databases

st.set_page_config(page_title="Veritabanı Görüntüleyici", layout="wide")
st.title("SQLite Veritabanı Yükleyici ve Görüntüleyici")

# Seçim menüsü
option = st.radio("Bir işlem seçin:", ("Veritabanı yükle", "Klasör seç ve birleştir"))

if option == "Veritabanı yükle":
    # Kullanıcı SQLite dosyasını yükler
    uploaded_file = st.file_uploader("SQLite veritabanı dosyasını seçin (.db)", type=["db"])

    if uploaded_file:
        # Geçici dosyayı kaydet
        with open("temp_database.db", "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.success("Veritabanı başarıyla yüklendi! Aşağıda önizleme yapabilirsiniz.")

        # Veritabanı bağlantısını aç
        conn = sqlite3.connect("temp_database.db")

        # Tablo adlarını sorgula
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql_query(query, conn)
        st.write("Veritabanındaki tablolar:", tables)

        if not tables.empty:
            first_table = tables["name"][0]
            st.write(f"Önizleme Yapılan Tablo: {first_table}")

            # İlk tabloyu oku
            try:
                data = pd.read_sql_query(f'SELECT * FROM "{first_table}"', conn)

                if not data.empty:
                    # Kullanıcıdan satır sayısı al
                    default_limit = 10
                    row_limit = st.number_input(
                        "Gösterilecek satır sayısını girin:",
                        min_value=1,
                        max_value=len(data),
                        value=default_limit,
                        step=1,
                    )
                    show_all = st.button("Tüm Satırları Göster", key="show_all")

                    if show_all:
                        st.write(f"Tüm {len(data)} satır gösteriliyor:")
                        st.write(data)
                    else:
                        st.write(f"{min(row_limit, len(data))} satır gösteriliyor:")
                        st.write(data.head(row_limit))
                else:
                    st.warning(f"Tablo '{first_table}' boş.")
            except Exception as e:
                st.error(f"Tablo okunurken hata oluştu: {e}")
        else:
            st.warning("Veritabanında hiçbir tablo bulunamadı.")
        conn.close()

elif option == "Klasör seç ve birleştir":
    # Klasör seçim alanı
    folder_path = st.text_input("Klasör yolunu girin:")
    if folder_path and os.path.isdir(folder_path):
        # Birleştirme işlemi
        output_db_path = "temp_database.db"
        merge_and_process_databases(folder_path, output_db_path)
        st.success(f"Veritabanları başarıyla birleştirildi ve {output_db_path} olarak kaydedildi.")

        # Birleştirilmiş veritabanını aç
        conn = sqlite3.connect(output_db_path)

        # Tablo adlarını sorgula
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql_query(query, conn)
        st.write("Birleştirilmiş Veritabanındaki Tablolar:", tables)

        if not tables.empty:
            first_table = tables["name"][0]
            st.write(f"Önizleme Yapılan Tablo: {first_table}")

            # İlk tabloyu oku
            try:
                data = pd.read_sql_query(f'SELECT * FROM "{first_table}"', conn)

                if not data.empty:
                    # Kullanıcıdan satır sayısı al
                    default_limit = 10
                    row_limit = st.number_input(
                        "Gösterilecek satır sayısını girin:",
                        min_value=1,
                        max_value=len(data),
                        value=default_limit,
                        step=1,
                    )
                    show_all = st.button("Tüm Satırları Göster", key="show_all")

                    if show_all:
                        st.write(f"Tüm {len(data)} satır gösteriliyor:")
                        st.write(data)
                    else:
                        st.write(f"{min(row_limit, len(data))} satır gösteriliyor:")
                        st.write(data.head(row_limit))
                else:
                    st.warning(f"Tablo '{first_table}' boş.")
            except Exception as e:
                st.error(f"Tablo okunurken hata oluştu: {e}")
        else:
            st.warning("Birleştirilmiş veritabanında hiçbir tablo bulunamadı.")
        conn.close()
