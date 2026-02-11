import os
import logging
import unicodedata
import pandas as pd
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
    1. 去除前後空白 (trim)
    2. 全形轉半形 (NFKC normalization)
    """
    if not text_str:
        return ""
    # NFKC 可以將全形英文/數字/空白轉為半形
    return unicodedata.normalize('NFKC', str(text_str)).strip()

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
        # 這裡利用 Pandas 的向量運算快速比對
        matched_counts_mask = (count_a[valid_keys] == count_b[valid_keys])
        final_valid_keys = valid_keys[matched_counts_mask]

        logging.info(f"總分提單數: A={len(count_a)}, B={len(count_b)}")
        logging.info(f"符合「項次數量一致(1對1)」的有效訓練單數: {len(final_valid_keys)}")

        if len(final_valid_keys) == 0:
            logging.warning("沒有符合訓練條件的資料。請確認 Table A 與 B 是否有成對的主/分提單號。")
            return

        # 3. 建立訓練集 (Linking)
        # 只保留有效的資料
        df_a_clean = df_a[df_a['link_key'].isin(final_valid_keys)].copy()
        df_b_clean = df_b[df_b['link_key'].isin(final_valid_keys)].copy()

        # 排序：確保按照 item_no / item_sequence 順序排列，以便依序配對
        df_a_clean.sort_values(by=['link_key', 'item_no'], inplace=True)
        df_b_clean.sort_values(by=['link_key', 'item_sequence'], inplace=True)

        # 重置索引，利用位置 (Reset Index) 來強制對齊
        # 因為已知每個 Key 裡的數量一樣，排序後第 1 筆 A 必定對應第 1 筆 B
        # 這裡我們使用一個技巧：直接把兩個 DF 的內容合併
        
        # 提取需要的欄位列表
        train_source = df_a_clean['description_original'].apply(normalize_text).tolist()
        train_target_desc = df_b_clean['description_official'].tolist()
        train_target_ccc = df_b_clean['ccc_code'].tolist()

        # 4. 多數決投票 (Majority Vote)
        logging.info("正在進行知識萃取與多數決投票...")
        
        # 結構: { 原始品名: Counter( (標準品名, 稅號) ) }
        knowledge_map = {}

        for src, tgt_desc, tgt_ccc in zip(train_source, train_target_desc, train_target_ccc):
            if not src: continue
            
            if src not in knowledge_map:
                knowledge_map[src] = Counter()
            
            # 投票：這組對應出現一次，就加一票
            knowledge_map[src][(tgt_desc, tgt_ccc)] += 1

        # 5. 產生最終知識庫 (Winner Takes All)
        final_records = []
        for src_desc, counter in knowledge_map.items():
            # 取得票數最高的組合 (most_common(1) 回傳 [((desc, ccc), count)])
            winner, count = counter.most_common(1)[0]
            official_desc, ccc = winner
            
            final_records.append({
                'original_description': src_desc,
                'official_description': official_desc,
                'ccc_code': ccc,
                'frequency': count
            })

        # 6. 寫入資料庫 (Update Database)
        logging.info(f"學習完成，共提取 {len(final_records)} 條標準知識。準備寫入...")
        
        if final_records:
            df_knowledge = pd.DataFrame(final_records)
            
            # 使用 temp table 策略進行 Upsert (更新或插入)
            # 因為 Pandas to_sql 預設只有 fail, replace, append
            # 我們希望保留舊資料但更新頻率 -> 其實最簡單是 Truncate 重建 (因為是 Batch Train)
            # 根據您的指示 "一次性跑完... 系統第一天就很聰明"，清空重建是最乾淨的
            
            with engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE standard_knowledge_base"))
                df_knowledge.to_sql('standard_knowledge_base', conn, if_exists='append', index=False)
            
            logging.info("✅ 標準知識庫已更新完畢！")

    except Exception as e:
        logging.error(f"訓練過程發生錯誤: {e}", exc_info=True)

if __name__ == "__main__":
    train_model()