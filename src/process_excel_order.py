import os
import shutil
import logging
import pandas as pd
import re
from datetime import datetime
from database import get_db_engine

# --- 設定 Log 紀錄 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("process_excel.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- 路徑設定 ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 預設 Excel 來源資料夾
EXCEL_SOURCE_DIR = os.path.join(BASE_DIR, 'uploads', 'daily_excel')
# 處理後搬移位置
EXCEL_PROCESSED_DIR = os.path.join(BASE_DIR, 'uploads', 'daily_excel', 'processed')

def extract_mawb_from_filename(filename):
    """
    從檔名提取主提單號 (MAWB)
    假設規則: 檔名開頭的英數組合，例如 'AYLD25070901EX...' -> 'AYLD25070901EX'
    """
    match = re.match(r'^([A-Z0-9]+)', filename)
    if match:
        return match.group(1)
    return None

def process_excel_file(filepath, filename):
    """
    讀取並清洗客戶 Excel 檔案
    """
    engine = get_db_engine()
    if not engine:
        return

    try:
        # 1. 讀取 Excel
        # header=3 表示第 4 行是標題 (索引從 0 開始)
        # 根據您的 CSV 範例，前 3 行是報表資訊
        logging.info(f"正在讀取檔案: {filename}")
        
        # 嘗試讀取 CSV 或 Excel
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(filepath, header=3)
        else:
            df = pd.read_excel(filepath, header=3)

        # 2. 欄位正規化 (移除欄位名稱的空白)
        df.columns = [str(c).strip().replace('\n', '') for c in df.columns]
        
        # 驗證關鍵欄位是否存在
        required_cols = ['分提單號碼', '貨物編號', '货物名称', '數量', '單價金額', '發票總金額']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            logging.error(f"檔案格式錯誤，缺少欄位: {missing_cols}")
            return

        # 3. 資料清洗與拆單處理 (Critical Step)
        
        # (A) 填補分提單號 (Handle Merged Cells)
        # 修正說明: Pandas 3.0+ 已棄用 fillna(method='ffill')，改用 .ffill()
        df['分提單號碼'] = df['分提單號碼'].ffill()
        
        # (B) 過濾無效行
        # 如果沒有分提單號，或沒有項次，通常是頁尾加總或雜訊，應移除
        df = df[df['分提單號碼'].notna() & df['貨物編號'].notna()]

        # (C) 提取 MAWB (主單號)
        mawb_no = extract_mawb_from_filename(filename)

        # 4. 準備寫入資料庫的 DataFrame
        # 建立一個新的 DataFrame，欄位名稱對應 table_a_raw
        db_df = pd.DataFrame()
        
        db_df['mawb_no'] = mawb_no
        db_df['hawb_no'] = df['分提單號碼'].astype(str).str.strip()
        db_df['item_no'] = pd.to_numeric(df['貨物編號'], errors='coerce').fillna(0).astype(int)
        db_df['description_original'] = df['货物名称'].astype(str).str.strip()
        
        # 數量與單位
        db_df['qty'] = pd.to_numeric(df['數量'], errors='coerce').fillna(0)
        # 注意：您的 CSV 欄位可能有 '數量單位'
        if '數量單位' in df.columns:
            db_df['qty_unit'] = df['數量單位'].astype(str).str.strip()
            
        # 重量 (若有)
        if '淨重' in df.columns:
             db_df['net_weight'] = pd.to_numeric(df['淨重'], errors='coerce').fillna(0)
             
        # 金額
        db_df['unit_price'] = pd.to_numeric(df['單價金額'], errors='coerce').fillna(0)
        db_df['total_amount'] = pd.to_numeric(df['發票總金額'], errors='coerce').fillna(0)
        db_df['currency'] = 'TWD' # 預設，或從 Excel 讀取
        
        # 關係人
        if '進口人英文名稱' in df.columns:
            db_df['consignee_name'] = df['進口人英文名稱'].astype(str).str.strip()
        if '進口人統一編號' in df.columns:
            db_df['consignee_id'] = df['進口人統一編號'].astype(str).str.strip()
        if '進口人電話' in df.columns:
            db_df['consignee_phone'] = df['進口人電話'].astype(str).str.strip()

        # 狀態
        db_df['processing_status'] = 'PENDING'
        
        logging.info(f"解析完成，準備匯入 {len(db_df)} 筆資料...")

        # 5. 寫入 MySQL
        db_df.to_sql('table_a_raw', con=engine, if_exists='append', index=False)
        logging.info(f"成功寫入資料庫！")

        # 6. 移動檔案
        if not os.path.exists(EXCEL_PROCESSED_DIR):
            os.makedirs(EXCEL_PROCESSED_DIR)
        shutil.move(filepath, os.path.join(EXCEL_PROCESSED_DIR, filename))
        logging.info(f"檔案已移至 processed 資料夾")

    except Exception as e:
        logging.error(f"處理檔案 {filename} 時發生錯誤: {e}", exc_info=True)

def main():
    # 檢查來源資料夾
    if not os.path.exists(EXCEL_SOURCE_DIR):
        os.makedirs(EXCEL_SOURCE_DIR)
        logging.info(f"已建立資料夾: {EXCEL_SOURCE_DIR} (請將 Excel 檔案放入此處)")
        return

    files = [f for f in os.listdir(EXCEL_SOURCE_DIR) if f.lower().endswith(('.xlsx', '.xls', '.csv'))]
    
    if not files:
        logging.info("目前沒有需要處理的 Excel 檔案。")
        return

    logging.info(f"掃描到 {len(files)} 個檔案，開始作業...")
    
    for filename in files:
        filepath = os.path.join(EXCEL_SOURCE_DIR, filename)
        process_excel_file(filepath, filename)

if __name__ == "__main__":
    main()