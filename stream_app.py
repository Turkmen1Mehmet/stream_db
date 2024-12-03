import os
import streamlit as st

st.set_page_config(page_title="Veritabanı Görüntüleyici", layout="wide")
st.title("SQLite Veritabanı Yükleyici ve Görüntüleyici")

# Kullanıcıdan işlem seçimi
option = st.radio("Bir işlem seçin:", ("Veritabanı yükle", "Klasör seç ve birleştir"))

def list_files_in_folder(folder_path):
    """Klasördeki dosyaları listele."""
    try:
        filenames = os.listdir(folder_path)
        filenames = [f for f in filenames if os.path.isfile(os.path.join(folder_path, f))]
        return filenames
    except FileNotFoundError:
        return []

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
    # Klasör yolu girişi
    folder_path = st.text_input("Bir klasör yolu girin:")
    
    if folder_path:
        if os.path.isdir(folder_path):
            st.success(f"Klasör bulundu: {folder_path}")
            
            # Klasördeki dosyaları listele
            files = list_files_in_folder(folder_path)
            if files:
                selected_file = st.selectbox("Bir dosya seçin:", files)
                st.write(f"Seçilen dosya: {selected_file}")
                
                # Birleştirme işlemini buradan başlatabilirsiniz
                st.write(f"{folder_path} içindeki dosyalar işleniyor...")
                # merge_and_process_databases(folder_path, output_db_path)
            else:
                st.warning("Bu klasörde herhangi bir dosya bulunamadı.")
        else:
            st.error("Geçerli bir klasör yolu girin.")
