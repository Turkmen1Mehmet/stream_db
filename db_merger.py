import os
import sqlite3
from datetime import datetime
import pandas as pd

# Ay isimlerini ayıklamak için bir eşleme
MONTHS_MAPPING = {
    "ocak": 1, "subat": 2, "mart": 3, "nisan": 4,
    "mayis": 5, "haziran": 6, "temmuz": 7, "agustos": 8,
    "eylul": 9, "ekim": 10, "kasim": 11, "aralik": 12
}

def remove_duplicate_rows(df):
    """Tekrarlı satırları kaldıran fonksiyon."""
    return df.drop_duplicates(subset=["etiket"], keep="first")


def check_azalan(row, date_columns):
    """Mevcut beden sayısı değişimini kontrol eden sütun."""
    mevcut_values = [row[col] for col in date_columns]

    if all(x >= y for x, y in zip(mevcut_values, mevcut_values[1:])) and any(x > y for x, y in zip(mevcut_values, mevcut_values[1:])):
        return "Azalan"
    elif all(x <= y for x, y in zip(mevcut_values, mevcut_values[1:])) and any(x < y for x, y in zip(mevcut_values, mevcut_values[1:])):
        return "Artan"
    elif all(x == mevcut_values[0] for x in mevcut_values):
        return "Sabit"
    else:
        return "Dalgalanıyor"


def calculate_depletion_rate(row, date_columns):
    """Tükenme oranını hesaplayan fonksiyon."""
    mevcut_values = [row[col] for col in date_columns]
    if all(v == 0 or pd.isnull(v) for v in mevcut_values):
        return 1
    if all(v == mevcut_values[0] for v in mevcut_values):
        return 1

    decrease_days = [x - y for x, y in zip(mevcut_values, mevcut_values[1:]) if x >= y]
    increase_days = [y - x for x, y in zip(mevcut_values, mevcut_values[1:]) if x < y]
    max_stok = max(mevcut_values)

    total_decrease = sum(decrease_days)
    decrease_ratio = -1 * (total_decrease / (len(mevcut_values) * max_stok)) if total_decrease > 0 else 0
    total_increase = sum(increase_days)
    increase_ratio = total_increase / (len(mevcut_values) * max_stok) if total_increase > 0 else 0
    hybrid_ratio = decrease_ratio + increase_ratio
    return hybrid_ratio


def extract_date_from_filename(filename, file_path):
    """Dosya ismindeki tarihleri çıkarır ve yıl bilgisini oluşturma tarihinden alır."""
    try:
        # Örnek dosya ismi: zaraeticaretkadin3turkey-15kasim.db
        date_str = filename.split("-")[-1].replace(".db", "").strip()

        # Dosyanın oluşturulma tarihinden yıl bilgisi al
        created_timestamp = os.path.getctime(file_path)
        file_year = datetime.fromtimestamp(created_timestamp).year

        for month_name, month_num in MONTHS_MAPPING.items():
            if month_name in date_str.lower():
                day = int(date_str.replace(month_name, "").strip())
                return datetime(file_year, month_num, day)
        return None  # Eğer uygun bir tarih bulunamazsa
    except ValueError:
        return None


def merge_and_process_databases(main_folder, output_db_path):
    """Ana klasördeki veritabanlarını birleştirir ve işleme alır."""
    all_files = []

    # Klasör ve dosya tarama
    for folder in os.listdir(main_folder):
        folder_path = os.path.join(main_folder, folder)
        if os.path.isdir(folder_path):
            for file in os.listdir(folder_path):
                if file.endswith(".db"):
                    file_path = os.path.join(folder_path, file)
                    file_date = extract_date_from_filename(file, file_path)
                    if file_date:
                        all_files.append((file_path, file_date))

    # Tarihe göre sıralama
    all_files.sort(key=lambda x: x[1])

    all_data = []
    for file_path, date in all_files:
        #print(f"File: {file_path}, Date Extracted: {date}")
        conn = sqlite3.connect(file_path)
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql(query, conn)
        if tables.empty:
            conn.close()
            continue
        table_name = tables.iloc[0, 0]
        data = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        data = remove_duplicate_rows(data)
        data["tarih"] = date
        all_data.append(data)
        conn.close()

    # Verileri birleştir
    combined_data = pd.concat(all_data, ignore_index=True)

    # Tarih sütunlarına göre pivot tablo oluştur
    date_columns = [f"mevcut_{date.strftime('%d%m%y')}" for _, date in all_files]
    pivot_data = combined_data.pivot_table(
        index=["etiket", "isim", "renk"],
        columns="tarih",
        values="beden",
        aggfunc="count"
    )
    pivot_data.columns = date_columns

    # Final veri setine sütun eklemeleri
    combined_data = pd.merge(combined_data, pivot_data, on=["etiket", "isim", "renk"], how="left")
    combined_data["mevcut_azalan_mi"] = combined_data.apply(lambda row: check_azalan(row, date_columns), axis=1)
    combined_data["depletion_rate"] = combined_data.apply(lambda row: calculate_depletion_rate(row, date_columns), axis=1)

    # Final veriyi SQLite veritabanına yaz
    conn_new = sqlite3.connect(output_db_path)
    combined_data.to_sql("zara_eticaret_combined_final", conn_new, if_exists="replace", index=False)
    conn_new.close()
    print(f"İşlem tamamlandı. Veriler {output_db_path} dosyasına kaydedildi.")
