import sqlite3
import pandas as pd

def etiket_merge_process(input_db_path, output_db_path):
    """
    Aynı etiket numarasına sahip ürünleri birleştirir ve sonucu yeni bir veritabanına yazar.

    Args:
    input_db_path (str): Giriş veritabanı dosyasının yolu.
    output_db_path (str): Çıkış veritabanı dosyasının yolu.

    Returns:
    pd.DataFrame: Birleştirilmiş verilerin DataFrame hali.
    """
    # Veritabanı bağlantısını kur
    conn = sqlite3.connect(input_db_path)
    df = pd.read_sql_query("SELECT * FROM zara_eticaret_combined_final", conn)

    # Aynı etiket numarasına sahip ürünleri birleştirme
    def merge_products(df):
        grouped = df.groupby('etiket')
        merged_products = []

        for name, group in grouped:
            latest_row = group.loc[group['tarih'].idxmax()].copy()
            latest_row['tekrar'] = group['tarih'].nunique()
            beden_set = group['beden'].dropna().unique()
            latest_row['beden'] = ', '.join(beden_set)

            for col in ['isim', 'ana_fiyat', 'fiyat', 'renk', 'ana_renk', 'kumas', 'aciklama']:
                latest_value = group.loc[group['tarih'].idxmax(), col]
                latest_row[col] = latest_value

            mevcut_columns = [col for col in group.columns if col.startswith('mevcut_')]
            for col in mevcut_columns:
                latest_row[col] = group[col].bfill().iloc[0]

            merged_products.append(latest_row)

        merged_df = pd.DataFrame(merged_products)
        return merged_df

    # Ürünleri birleştir
    merged_df = merge_products(df)

    # Birleştirilmiş veriyi yeni veritabanına yaz
    conn_output = sqlite3.connect(output_db_path)
    merged_df.to_sql('merged_zara_data', conn_output, if_exists='replace', index=False)

    # Bağlantıları kapat
    conn.close()
    conn_output.close()

    return merged_df
