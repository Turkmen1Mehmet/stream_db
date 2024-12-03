import sqlite3
import pandas as pd
import numpy as np

def ortakdb_process(input_db_path, output_db_path):
    """
    Verilen veritabanındaki etiket verilerini işler ve performansı hesaplayarak yeni bir veritabanına yazar.

    Args:
    input_db_path (str): Giriş veritabanı dosyasının yolu.
    output_db_path (str): Çıkış veritabanı dosyasının yolu.

    Returns:
    pd.DataFrame: İşlenmiş verilerin DataFrame hali.
    """
    # Giriş veritabanı bağlantısı
    conn1 = sqlite3.connect(input_db_path)

    # Veriyi okuyun
    df1 = pd.read_sql_query("SELECT * FROM merged_zara_data", conn1)

    # 'mevcut_' ile başlayan sütunları seç
    mevcut_columns1 = [col for col in df1.columns if col.startswith('mevcut_')]

    # Bu sütunları içeren yeni bir DataFrame oluştur
    mevcut_df_1 = df1[mevcut_columns1].apply(pd.to_numeric, errors='coerce')

    # Null değerleri forward fill (ileriye doldurma) ve backward fill (geriye doldurma) ile doldur
    mevcut_df_1_filled = mevcut_df_1.ffill(axis=1).bfill(axis=1)

    # Günlük değişimleri (farkları) hesapla
    stock_change_1 = mevcut_df_1_filled.diff(axis=1)

    # Farkların mutlak değerlerini alarak performans hesapla
    stock_change_abs_1 = stock_change_1.abs()
    performance_1 = stock_change_abs_1.sum(axis=1)

    # Performansı asıl DataFrame'e ekle
    df1['performans'] = performance_1

    # Yinelenen kayıtları kaldırarak sadece tekil satırları tut
    df1 = df1.drop_duplicates(subset=['etiket'])

    # Çıkış veritabanı bağlantısı
    conn2 = sqlite3.connect(output_db_path)

    # İşlenmiş veriyi yeni veritabanına yaz
    df1.to_sql('filtered_zara_data', conn2, if_exists='replace', index=False)

    # Bağlantıları kapat
    conn1.close()
    conn2.close()

    return df1
