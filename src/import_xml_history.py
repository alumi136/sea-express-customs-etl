import os
import shutil
import logging
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
from collections import defaultdict
from database import get_db_engine

# --- 設定 Log 紀錄 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("import_xml.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- 路徑設定 ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XML_SOURCE_DIR = os.path.join(BASE_DIR, os.getenv('XML_SOURCE_DIR', 'uploads/xml_history'))
XML_PROCESSED_DIR = os.path.join(BASE_DIR, os.getenv('XML_PROCESSED_DIR', 'uploads/xml_history/processed'))

def clean_doc_no(raw_doc_no):
    """
    清洗報單號碼: 移除空格、換行與斜線
    原始: BY/ /14/440 /JM0H3 -> 目標: BY14440JM0H3
    """
    if not raw_doc_no:
        return None
    return raw_doc_no.replace(' ', '').replace('\n', '').replace('/', '').strip()

def parse_xml_file(filepath, filename):
    """
    解析單一 XML 檔案，回傳 DataFrame 所需的字典列表
    """
    data_list = []
    
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        
        # 追蹤每個 HAWB 的項次 (Item Sequence)
        hawb_item_counters = defaultdict(int)

        # 搜尋所有 <BID_HEAD> 標籤 (使用 .// 遞迴搜尋以防結構差異)
        # 根據您的說明，每一個 BID_HEAD 代表一個品項
        for bid_head in root.findall('.//BID_HEAD'):
            row = {}
            
            # 1. 基礎欄位讀取
            hawb_no = bid_head.findtext('HAWB_NO', '').strip()
            if not hawb_no:
                continue # 若無分提單號則跳過

            # 產生項次 (Item Sequence)
            hawb_item_counters[hawb_no] += 1
            
            # 2. 資料提取與清洗
            row['data_source_file'] = filename
            row['dcl_doc_no'] = clean_doc_no(bid_head.findtext('DCL_DOC_NO'))
            row['mawb_no'] = bid_head.findtext('MAWB')
            row['hawb_no'] = hawb_no
            row['flight_no'] = bid_head.findtext('FLY_NO') # XML標記為 FLY_NO
            
            # 日期處理 (嘗試解析，若失敗則留空)
            import_date_str = bid_head.findtext('IMPORT_DATE')
            try:
                # 假設 XML 日期格式包含 T (如 2025-01-01T...)
                if import_date_str:
                    row['import_date'] = import_date_str.split('T')[0]
            except:
                row['import_date'] = None

            row['item_sequence'] = hawb_item_counters[hawb_no]
            row['description_official'] = bid_head.findtext('DESCRIPTION')
            row['ccc_code'] = bid_head.findtext('CLASSIFY_NO')
            
            # 3. 數值運算與邏輯
            # QTY
            try:
                qty = float(bid_head.findtext('QTY', 0))
                row['qty'] = qty
            except ValueError:
                row['qty'] = 0
            
            row['qty_unit'] = bid_head.findtext('QTY_UM')
            
            # 金額處理
            # PAY_TAX_AMT = 品項總價
            # FOB_AMT_TWD = 分提單總價 (HAWB Total)
            try:
                item_total = float(bid_head.findtext('PAY_TAX_AMT', 0))
                hawb_total = float(bid_head.findtext('FOB_AMT_TWD', 0))
                
                row['item_total_amount'] = item_total
                row['hawb_total_amount'] = hawb_total
                
                # 計算單價 = 品項總價 / 數量
                if qty > 0:
                    row['unit_price_calculated'] = round(item_total / qty, 4)
                else:
                    row['unit_price_calculated'] = 0
            except ValueError:
                row['item_total_amount'] = 0
                row['hawb_total_amount'] = 0
                row['unit_price_calculated'] = 0

            row['duty_rate'] = bid_head.findtext('IMPORT_DUTY_RATE')
            
            # 4. 關係人資訊
            row['consignee_id'] = bid_head.findtext('CNEE_BAN_ID')
            row['consignee_name'] = bid_head.findtext('CNEE_E_NAME')
            row['consignee_phone'] = bid_head.findtext('OTHER_ITEN_2') # 根據您的指示，手機在 OTHER_ITEN_2
            row['shipper_name'] = bid_head.findtext('SHPR_E_NAME')
            row['export_port'] = bid_head.findtext('FROM_CODE')
            
            data_list.append(row)
            
    except ET.ParseError as e:
        logging.error(f"XML 解析失敗 {filename}: {e}")
    except Exception as e:
        logging.error(f"處理檔案時發生未預期錯誤 {filename}: {e}")

    return data_list

def main():
    # 確保處理後資料夾存在
    if not os.path.exists(XML_PROCESSED_DIR):
        os.makedirs(XML_PROCESSED_DIR)
        
    engine = get_db_engine()
    if not engine:
        logging.error("無法取得資料庫連線，程式終止。")
        return

    # 取得所有 XML 檔案
    if not os.path.exists(XML_SOURCE_DIR):
        logging.error(f"找不到來源資料夾: {XML_SOURCE_DIR}")
        return

    files = [f for f in os.listdir(XML_SOURCE_DIR) if f.lower().endswith('.xml')]
    logging.info(f"掃描到 {len(files)} 個 XML 檔案準備處理。")

    total_inserted = 0

    for filename in files:
        filepath = os.path.join(XML_SOURCE_DIR, filename)
        logging.info(f"正在處理: {filename} ...")
        
        # 1. 解析
        parsed_data = parse_xml_file(filepath, filename)
        
        if parsed_data:
            # 2. 轉換為 DataFrame
            df = pd.DataFrame(parsed_data)
            
            # 3. 寫入資料庫
            try:
                # 使用 append 模式，若表格存在則附加資料
                df.to_sql('table_b_history', con=engine, if_exists='append', index=False)
                count = len(df)
                total_inserted += count
                logging.info(f"  -> 成功匯入 {count} 筆資料。")
                
                # 4. 移動檔案至 processed 資料夾
                shutil.move(filepath, os.path.join(XML_PROCESSED_DIR, filename))
                
            except Exception as e:
                logging.error(f"  -> 資料庫寫入失敗: {e}")
        else:
            logging.warning(f"  -> 檔案 {filename} 未提取到任何資料或格式不符。")

    logging.info(f"作業結束。總共匯入 {total_inserted} 筆歷史資料。")

if __name__ == "__main__":
    main()