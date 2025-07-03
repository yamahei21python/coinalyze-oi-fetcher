# -*- coding: utf-8 -*-
import requests
import time
import pandas as pd
import sqlite3
import os
from google.cloud import storage, secretmanager

# --- ▼▼▼【重要：ここを書き換えてください】▼▼▼ ---

# Google CloudプロジェクトID (コンソール上部に表示されています)
PROJECT_ID = "activeoi-51634" # 自分のプロジェクトIDに書き換えてください

# Cloud Storageのバケット名 (先ほど作成したバケット名)
BUCKET_NAME = "coinalyze-db-files-activeoi" # 自分のバケット名に書き換えてください

# --- ▲▲▲【書き換えはここまで】▲▲▲ ---

# Cloud Storage上のDBファイルのパス
RAW_OI_DB_KEY = 'data/raw_oi.db'
ACTIVE_OI_DB_KEY = 'data/active_oi.db'

# Cloud Functionsでは /tmp ディレクトリのみ書き込み可能
TEMP_DIR = '/tmp'
RAW_OI_DB_PATH = os.path.join(TEMP_DIR, 'raw_oi.db')
ACTIVE_OI_DB_PATH = os.path.join(TEMP_DIR, 'active_oi.db')
TABLE_NAME = 'market_data'

# (元コードの EXCHANGE_CONFIG などはそのまま)
EXCHANGE_CONFIG = {
    'Binance': {'code': 'A', 'contracts': ['BTCUSD_PERP.', 'BTCUSDT_PERP.', 'BTCUSD.', 'BTCUSDT.']},
    'Bybit': {'code': '6', 'contracts': ['BTCUSD.', 'BTCUSDT.']},
    'OKX': {'code': '3', 'contracts': ['BTCUSD_PERP.', 'BTCUSDT_PERP.', 'BTCUSD.', 'BTCUSDT.']},
    'BitMEX': {'code': '0', 'contracts': ['BTCUSD_PERP.', 'BTCUSDT_PERP.', 'BTCUSD.', 'BTCUSDT.']}
}
EXCHANGE_NAMES = list(EXCHANGE_CONFIG.keys())
CODE_TO_NAME_MAP = {v['code']: k for k, v in EXCHANGE_CONFIG.items()}
OHLC_SUFFIXES = ['_Open', '_High', '_Low', '_Close']

def get_api_key():
    """Secret ManagerからAPIキーを取得する"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/coinalyze-api-key/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def build_symbol_string() -> str:
    symbols = []
    for name, config in EXCHANGE_CONFIG.items():
        for contract in config['contracts']:
            symbols.append(f"{contract}{config['code']}")
    return ','.join(symbols)

def fetch_open_interest_data(api_key: str) -> list:
    """Coinalyze APIからOI（Open Interest）の履歴データを取得する。"""
    symbols_str = build_symbol_string()
    headers = {"api-key": api_key}
    params = {
        "symbols": symbols_str,
        "interval": "5min",
        "from": int(time.time()) - 864000,  # 過去10日分
        "to": int(time.time()),
        "convert_to_usd": "true"
    }
    try:
        response = requests.get("https://api.coinalyze.net/v1/open-interest-history", headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"APIリクエストに失敗しました: {e}")
    except ValueError as e:
        print(f"JSONの解析に失敗しました: {e}")
    return []

def process_api_data(api_data: list) -> pd.DataFrame:
    if not api_data:
        print("APIデータが空のため、処理をスキップします。")
        return pd.DataFrame()
    all_dfs = []
    for item in api_data:
        symbol, history = item.get("symbol"), item.get("history")
        if not symbol or not isinstance(history, list) or not history:
            continue
        contract_type, exchange_code = symbol.rsplit('.', 1)
        df = pd.DataFrame(history)
        df['Datetime'] = pd.to_datetime(df['t'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')
        df = df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close'}).set_index('Datetime')
        ohlc_df = df[['Open', 'High', 'Low', 'Close']]
        exchange_name = CODE_TO_NAME_MAP[exchange_code]
        ohlc_df.columns = pd.MultiIndex.from_product([[exchange_name], [contract_type], ohlc_df.columns])
        all_dfs.append(ohlc_df)
    if not all_dfs:
        return pd.DataFrame()
    combined_df = pd.concat(all_dfs, axis=1)
    final_df = pd.DataFrame()
    for ex_name in EXCHANGE_NAMES:
        if ex_name in combined_df.columns.get_level_values(0):
            ex_df = combined_df[ex_name]
            summed_df = ex_df.T.groupby(level=1).sum(min_count=1).T
            summed_df.columns = [f"{ex_name}{suffix}" for suffix in OHLC_SUFFIXES]
            final_df = pd.concat([final_df, summed_df], axis=1)
    final_df = final_df.interpolate().reset_index().dropna().reset_index(drop=True)
    return final_df

def save_to_db(df: pd.DataFrame, db_path: str, table_name: str, if_exists: str = 'replace'):
    if df.empty:
        print("DataFrameが空のため、データベースへの保存をスキップしました。")
        return
    df_to_save = df.copy()
    if 'Datetime' in df_to_save.columns:
        df_to_save['Datetime'] = df_to_save['Datetime'].astype(str)
    try:
        conn = sqlite3.connect(db_path)
        df_to_save.to_sql(table_name, conn, if_exists=if_exists, index=False)
        print(f"データが正常に '{db_path}' の '{table_name}' に保存されました。")
    except Exception as e:
        print(f"データベースへの保存中にエラーが発生しました: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def read_from_db(db_path: str, table_name: str) -> pd.DataFrame:
    if not os.path.exists(db_path):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if cursor.fetchone() is None:
            return pd.DataFrame()
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn, parse_dates=['Datetime'])
        return df
    except Exception as e:
        print(f"データベースの読み込み中にエラーが発生しました: {e}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def calculate_active_oi(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index('Datetime')
    active_oi_df = pd.DataFrame(index=df.index)
    rolling_window_3d = 12 * 24 * 3
    for name in EXCHANGE_NAMES:
        low_col, close_col = f'{name}_Low', f'{name}_Close'
        min_3day = df[low_col].rolling(window=rolling_window_3d, min_periods=1).min()
        active_oi_df[f'{name}_Active_OI_5min'] = df[close_col] - min_3day
    active_oi_df = active_oi_df.dropna().reset_index()
    return active_oi_df

def aggregate_and_standardize_oi(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index('Datetime')
    active_oi_cols = [col for col in df.columns if 'Active_OI_5min' in col]
    df['ALL_Active_OI_5min'] = df[active_oi_cols].sum(axis=1)
    rolling_window_3d = 12 * 24 * 3
    rolling_stats = df['ALL_Active_OI_5min'].rolling(window=rolling_window_3d)
    mean, std = rolling_stats.mean(), rolling_stats.std()
    df['STD_Active_OI'] = (df['ALL_Active_OI_5min'] - mean) / std.replace(0, pd.NA)
    return df.dropna().reset_index()


def run_job(event, context):
    """Cloud Functionsのエントリーポイント関数。スケジューラから呼び出される。"""
    print("--- 処理開始 ---")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    print(f"GCSから '{RAW_OI_DB_KEY}' をダウンロードします...")
    try:
        blob_raw = bucket.blob(RAW_OI_DB_KEY)
        blob_raw.download_to_filename(RAW_OI_DB_PATH)
    except Exception as e:
        print(f"raw_oi.dbが見つかりません。新規作成します。エラー: {e}")
        os.makedirs(os.path.dirname(RAW_OI_DB_PATH), exist_ok=True)

    api_key = get_api_key()
    new_raw_data = process_api_data(fetch_open_interest_data(api_key))
    if new_raw_data.empty:
        print("有効なデータが取得できなかったため、処理を終了します。")
        return "No data fetched."

    existing_raw_data = read_from_db(RAW_OI_DB_PATH, TABLE_NAME)
    combined_raw_data = pd.concat([existing_raw_data, new_raw_data]).drop_duplicates(subset=['Datetime'], keep='last')
    combined_raw_data = combined_raw_data.sort_values(by='Datetime').reset_index(drop=True)

    save_to_db(combined_raw_data, RAW_OI_DB_PATH, TABLE_NAME, if_exists='replace')
    active_oi_data = calculate_active_oi(combined_raw_data)
    final_active_oi_data = aggregate_and_standardize_oi(active_oi_data)
    save_to_db(final_active_oi_data, ACTIVE_OI_DB_PATH, TABLE_NAME, if_exists='replace')

    print(f"'{RAW_OI_DB_KEY}' をGCSにアップロードします...")
    blob_raw_upload = bucket.blob(RAW_OI_DB_KEY)
    blob_raw_upload.upload_from_filename(RAW_OI_DB_PATH)

    print(f"'{ACTIVE_OI_DB_KEY}' をGCSにアップロードします...")
    blob_active_upload = bucket.blob(ACTIVE_OI_DB_KEY)
    blob_active_upload.upload_from_filename(ACTIVE_OI_DB_PATH)

    print("--- 処理完了 ---")
    return "Success"
