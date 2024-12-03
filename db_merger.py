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
    mevcut_values = [row[col] for col in date_columns if col in row and not pd.isnull(row[col])]

    if not mevcut_values:
        return "Boş"
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
    mevcut_values = [row[col] for col in date_columns if col in row and not pd.isnull(row[col])]
    if not mevcut_values or all(v == mevcut_values[0] for v in mevcut_values):
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
        date_str = filename.split("-")[-1].replace(".db", "").strip()
        created_timestamp = os.path.getctime(file_path)
        file_year = datetime.fromtimestamp(created_timestamp).year

        for month_name, month_num in MONTHS_MAPPING.items():
            if month_name in date_str.lower():
                day = int(date_str.replace(month_name, "").strip())
                return datetime(file_year, month_num, day)
        return None
    except ValueError:
        return None


def merge_and_process_databases(db_files, output_db_path):
    """ZIP'ten gelen .db dosyalarını birleştirir ve işleme alır."""
    all_data = []
    for file_path in db_files:
        conn = sqlite3.connect(file_path)
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql(query, conn)

        if tables.empty:
            conn.close()
            continue
        
        table_name = tables.iloc[0, 0]
        data = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()

        if not data.empty:
            data["tarih"] = extract_date_from_filename(os.path.basename(file_path), file_path)
            all_data.append(data)

    if not all_data:
        raise ValueError("Geçerli veri içeren .db dosyası bulunamadı.")

    combined_data = pd.concat(all_data, ignore_index=True)

    date_columns = [f"mevcut_{data['tarih'].iloc[0].strftime('%d%m%y')}" for data in all_data if not data.empty]
    pivot_data = combined_data.pivot_table(
        index=["etiket", "isim", "renk"],
        columns="tarih",
        values="beden",
        aggfunc="count"
    )
    pivot_data.columns = date_columns

    combined_data = pd.merge(combined_data, pivot_data, on=["etiket", "isim", "renk"], how="left")
    combined_data["mevcut_azalan_mi"] = combined_data.apply(lambda row: check_azalan(row, date_columns), axis=1)
    combined_data["depletion_rate"] = combined_data.apply(lambda row: calculate_depletion_rate(row, date_columns), axis=1)

    conn_new = sqlite3.connect(output_db_path)
    combined_data.to_sql("zara_eticaret_combined_final", conn_new, if_exists="replace", index=False)
    conn_new.close()
    print(f"İşlem tamamlandı. Veriler {output_db_path} dosyasına kaydedildi.")
