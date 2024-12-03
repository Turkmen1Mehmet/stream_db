import streamlit as st
import sqlite3
import pandas as pd
import os
import zipfile
from db_merger import merge_and_process_databases

st.set_page_config(page_title="Veritabanı Görüntüleyici", layout="wide")
st.title("SQLite Veritabanı Yükleyici ve Görüntüleyici")

def find_db_files(folder_path):
    """Verilen klasörün altındaki tüm geçerli .db dosyalarını bul."""
    db_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".db") and not file.startswith("._") and "__MACOSX" not in root:
                db_files.append(os.path.join(root, file))
    return db_files

def validate_db_file(db_path):
    """Bir SQLite dosyasının geçerli olup olmadığını kontrol eder."""
    try:
        conn = sqlite3.connect(db_path)
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql_query(query, conn)
        conn.close()
        return not tables.empty  # Tablolar varsa geçerli bir veritabanı
    except Exception:
        return False

# Kullanıcıdan işlem seçimi
option = st.radio("Bir işlem seçin:", ("Veritabanı yükle", "ZIP dosyası yükle ve birleştir"))

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

elif option == "ZIP dosyası yükle ve birleştir":
    uploaded_zip = st.file_uploader("ZIP dosyasını yükleyin (içinde klasörler ve .db dosyaları olmalı):", type=["zip"])

    if uploaded_zip:
        temp_dir = "temp_folder"
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            with zipfile.ZipFile(uploaded_zip, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                st.success(f"ZIP dosyası başarıyla açıldı: {temp_dir}")
            
            # Klasör ve alt klasörlerdeki tüm .db dosyalarını bul
            db_files = find_db_files(temp_dir)
            valid_db_files = [db for db in db_files if validate_db_file(db)]
            
            if valid_db_files:
                st.write(f"Geçerli .db dosyaları: {valid_db_files}")
                
                # Geçerli dosyaları birleştirme
                output_db_path = "merged_database.db"
                merge_and_process_databases(valid_db_files, output_db_path)
                st.success(f"Veritabanları başarıyla birleştirildi ve {output_db_path} oluşturuldu.")
                
                # Birleştirilmiş veritabanını incele
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
            else:
                st.error("Geçerli bir .db dosyası bulunamadı. ZIP dosyasını kontrol edin.")
        except zipfile.BadZipFile:
            st.error("ZIP dosyası bozuk veya geçersiz. Lütfen doğru bir dosya yükleyin.")
        except Exception as e:
            st.error(f"Bir hata oluştu: {e}")
