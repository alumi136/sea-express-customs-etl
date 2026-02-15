import os
import re
import logging
import unicodedata
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from collections import Counter
from database import get_db_engine

# --- 設定 Log ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("training.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def normalize_text(text_str):
    """
    資料清洗邏輯：
    1. 全形轉半形 (NFKC)
    2. 強制轉大寫 (.upper)
    3. [新增] 針對 '/' 符號處理：只保留 '/' 之後的文字 (取最後一段)
    4. 移除標點符號與特殊字元 (將其替換為空白)
    5. 縮減多餘空白
    """
    if not text_str:
        return ""
    
    # 1. NFKC 標準化 (全形轉半形)
    text_val = unicodedata.normalize('NFKC', str(text_str))
    
    # 2. 強制轉大寫
    text_val = text_val.upper()

    # 3. [新增需求] 針對 '/' 處理：清除 '/' 之前的文字，只保留之後的
    # 例如 "英文/中文" -> "中文", "A/B/C" -> "C" (取最後一段最精確)
    if '/' in text_val:
        text_val = text_val.split('/')[-1]
    
    # 4. 使用 Regex 替換標點符號為空白
    # [^\w\s] 表示匹配 "非(文字、數字、底線、空白)" 的所有字元
    # 這會把 -, (, ), @ 等符號都變成空白，避免黏在一起
    text_val = re.sub(r'[^\w\s]', ' ', text_val)
    
    # 5. 縮減多餘空白 (將連續空白變為一個) 並 去除前後空白
    text_val = re.sub(r'\s+', ' ', text_val).strip()
    
    return text_val

def train_model():
    engine = get_db_engine()
    if not engine:
        return

    logging.info("🚀 開始執行批次訓練 (Batch Training)...")

    try:
        # 1. 撈取資料 (只撈取必要欄位以節省記憶體)
        # 必須確保 mawb_no 與 hawb_no 都不為空
        logging.info("正在讀取歷史資料 (Table A & Table B)...")
        
        sql_a = """
        SELECT mawb_no, hawb_no, item_no, description_original 
        FROM table_a_raw 
        WHERE mawb_no IS NOT NULL AND hawb_no IS NOT NULL 
          AND description_original IS NOT NULL
        """
        
        sql_b = """
        SELECT mawb_no, hawb_no, item_sequence, description_official, ccc_code
        FROM table_b_history
        WHERE mawb_no IS NOT NULL AND hawb_no IS NOT NULL
        """

        df_a = pd.read_sql(sql_a, engine)
        df_b = pd.read_sql(sql_b, engine)

        # 資料前處理 (清洗 Key 值以便 Join)
        # 移除 mawb/hawb 的空白與符號，確保對應率
        for df in [df_a, df_b]:
            df['mawb_clean'] = df['mawb_no'].astype(str).str.replace(r'[\s/-]', '', regex=True).str.upper()
            df['hawb_clean'] = df['hawb_no'].astype(str).str.replace(r'[\s/-]', '', regex=True).str.upper()
            df['link_key'] = df['mawb_clean'] + "_" + df['hawb_clean']

        # 2. 檢核項次數量 (Consolidation Logic)
        # 統計每個分提單(Key)有多少個項次
        count_a = df_a.groupby('link_key').size()
        count_b = df_b.groupby('link_key').size()

        # 找出項次數量完全一致的 Keys (交集)
        valid_keys = count_a.index.intersection(count_b.index)
        
        # 進一步過濾：數量必須相等 (Count A == Count B)
        matched_counts_mask = (count_a[valid_keys] == count_b[valid_keys])
        final_valid_keys = valid_keys[matched_counts_mask]

        logging.info(f"總分提單數: A={len(count_a)}, B={len(count_b)}")
        logging.info(f"符合「項次數量一致(1對1)」的有效訓練單數: {len(final_valid_keys)}")

        if len(final_valid_keys) == 0:
            logging.warning("沒有符合訓練條件的資料。請確認 Table A 與 B 是否有成對的主/分提單號。")
            return

        # 3. 建立訓練集 (Linking)
        df_a_clean = df_a[df_a['link_key'].isin(final_valid_keys)].copy()
        df_b_clean = df_b[df_b['link_key'].isin(final_valid_keys)].copy()

        # 排序
        df_a_clean.sort_values(by=['link_key', 'item_no'], inplace=True)
        df_b_clean.sort_values(by=['link_key', 'item_sequence'], inplace=True)

        # 提取欄位並進行清洗 (Normalize)
        train_source = df_a_clean['description_original'].apply(normalize_text).tolist()
        train_target_desc = df_b_clean['description_official'].tolist()
        train_target_ccc = df_b_clean['ccc_code'].tolist()

        # 4. 多數決投票 (Majority Vote)
        logging.info("正在進行知識萃取與多數決投票...")
        
        knowledge_map = {}

        for src, tgt_desc, tgt_ccc in zip(train_source, train_target_desc, train_target_ccc):
            if not src: continue # 略過空字串
            
            if src not in knowledge_map:
                knowledge_map[src] = Counter()
            
            # 投票
            knowledge_map[src][(tgt_desc, tgt_ccc)] += 1

        # 5. 產生最終知識庫 (Winner Takes All)
        final_records = []
        for src_desc, counter in knowledge_map.items():
            winner, count = counter.most_common(1)[0]
            official_desc, ccc = winner
            
            final_records.append({
                'original_description': src_desc,
                'official_description': official_desc,
                'ccc_code': ccc,
                'frequency': count
            })

        # 6. 資料庫操作 (備份 -> 清空 -> 寫入)
        logging.info(f"學習完成，共提取 {len(final_records)} 條標準知識。")
        
        if final_records:
            df_knowledge = pd.DataFrame(final_records)
            
            with engine.begin() as conn:
                # [新增] 自動備份機制
                # 檢查目前標準庫是否有資料
                check_sql = text("SELECT COUNT(*) FROM standard_knowledge_base")
                row_count = conn.execute(check_sql).scalar()
                
                if row_count > 0:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    backup_table = f"standard_knowledge_base_backup_{timestamp}"
                    logging.info(f"偵測到舊資料 ({row_count} 筆)，正在備份至 {backup_table} ...")
                    
                    # 執行備份 (Create Table As Select)
                    backup_sql = text(f"CREATE TABLE {backup_table} AS SELECT * FROM standard_knowledge_base")
                    conn.execute(backup_sql)
                    logging.info("備份完成。")

                # 清空舊資料
                logging.info("正在清空標準知識庫 (TRUNCATE)...")
                conn.execute(text("TRUNCATE TABLE standard_knowledge_base"))
                
                # 寫入新資料
                logging.info("正在寫入新訓練資料...")
                df_knowledge.to_sql('standard_knowledge_base', conn, if_exists='append', index=False)
            
            logging.info("✅ 標準知識庫已更新完畢！")

    except Exception as e:
        logging.error(f"訓練過程發生錯誤: {e}", exc_info=True)

if __name__ == "__main__":
    train_model()