import os
import shutil
import logging
import xml.etree.ElementTree as ET
import pandas as pd
import zipfile
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

def extract_data_from_root(root, source_filename):
    """
    核心邏輯：從 XML 的 Root 節點提取資料
    (抽離出來以供 單檔XML 與 Zip內XML 共用)
    """
    data_list = []
    
    # 追蹤每個 HAWB 的項次 (Item Sequence)
    # 注意：若是 Zip 檔，這裡的計數器範圍僅限於該單一 XML 檔案內
    hawb_item_counters = defaultdict(int)

    # 搜尋所有 <BID_HEAD> 標籤 (使用 .// 遞迴搜尋以防結構差異)
    for bid_head in root.findall('.//BID_HEAD'):
        row = {}
        
        # 1. 基礎欄位讀取
        hawb_no = bid_head.findtext('HAWB_NO', '').strip()
        if not hawb_no:
            continue # 若無分提單號則跳過

        # 產生項次 (Item Sequence)
        hawb_item_counters[hawb_no] += 1
        
        # 2. 資料提取與清洗
        row['data_source_file'] = source_filename
        row['dcl_doc_no'] = clean_doc_no(bid_head.findtext('DCL_DOC_NO'))
        row['mawb_no'] = bid_head.findtext('MAWB')
        row['hawb_no'] = hawb_no
        row['flight_no'] = bid_head.findtext('FLY_NO')
        
        # 日期處理
        import_date_str = bid_head.findtext('IMPORT_DATE')
        try:
            if import_date_str:
                row['import_date'] = import_date_str.split('T')[0]
        except:
            row['import_date'] = None

        row['item_sequence'] = hawb_item_counters[hawb_no]
        row['description_official'] = bid_head.findtext('DESCRIPTION')
        row['ccc_code'] = bid_head.findtext('CLASSIFY_NO')
        
        # 3. 數值運算與邏輯
        try:
            qty = float(bid_head.findtext('QTY', 0))
            row['qty'] = qty
        except ValueError:
            row['qty'] = 0
        
        row['qty_unit'] = bid_head.findtext('QTY_UM')
        
        # 金額處理
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
        row['consignee_phone'] = bid_head.findtext('OTHER_ITEN_2')
        row['shipper_name'] = bid_head.findtext('SHPR_E_NAME')
        row['export_port'] = bid_head.findtext('FROM_CODE')
        
        data_list.append(row)
        
    return data_list

def parse_xml_file(filepath, filename):
    """
    解析單一 XML 檔案
    """
    try:
        tree = ET.parse(filepath)
        return extract_data_from_root(tree.getroot(), filename)
    except ET.ParseError as e:
        logging.error(f"XML 解析失敗 {filename}: {e}")
    except Exception as e:
        logging.error(f"處理檔案時發生未預期錯誤 {filename}: {e}")
    return []

def parse_zip_file(filepath, zip_filename):
    """
    解析 ZIP 壓縮檔內的 XML 檔案
    """
    all_data = []
    try:
        if not zipfile.is_zipfile(filepath):
            logging.error(f"檔案 {zip_filename} 不是有效的 ZIP 格式")
            return []

        with zipfile.ZipFile(filepath, 'r') as zf:
            # 取得壓縮檔內所有檔案清單
            file_list = zf.namelist()
            # 過濾出 .xml 檔案，並排除 __MACOSX 等系統隱藏檔
            xml_files = [f for f in file_list if f.lower().endswith('.xml') and not f.startswith('__')]
            
            logging.info(f"壓縮檔 {zip_filename} 內含 {len(xml_files)} 個 XML 檔案。")
            
            for member_name in xml_files:
                try:
                    # 使用 zf.open 直接讀取檔案串流，無需解壓縮到硬碟
                    with zf.open(member_name) as xml_file:
                        tree = ET.parse(xml_file)
                        # 標註來源為 "Zip檔名::內含XML檔名" 以便追溯
                        source_name = f"{zip_filename}::{member_name}"
                        data = extract_data_from_root(tree.getroot(), source_name)
                        all_data.extend(data)
                except Exception as e:
                    logging.warning(f"  -> 讀取壓縮檔內 {member_name} 失敗: {e}")
                    
    except Exception as e:
        logging.error(f"處理壓縮檔 {zip_filename} 時發生錯誤: {e}")
        
    return all_data

def main():
    # 確保處理後資料夾存在
    if not os.path.exists(XML_PROCESSED_DIR):
        os.makedirs(XML_PROCESSED_DIR)
        
    engine = get_db_engine()
    if not engine:
        logging.error("無法取得資料庫連線，程式終止。")
        return

    # 取得所有 XML 或 ZIP 檔案
    if not os.path.exists(XML_SOURCE_DIR):
        logging.error(f"找不到來源資料夾: {XML_SOURCE_DIR}")
        return

    # 掃描 .xml 和 .zip
    files = [f for f in os.listdir(XML_SOURCE_DIR) if f.lower().endswith(('.xml', '.zip'))]
    logging.info(f"掃描到 {len(files)} 個檔案 (XML/ZIP) 準備處理。")

    total_inserted = 0

    for filename in files:
        filepath = os.path.join(XML_SOURCE_DIR, filename)
        logging.info(f"正在處理: {filename} ...")
        
        parsed_data = []
        
        # 依副檔名決定處理方式
        if filename.lower().endswith('.zip'):
            parsed_data = parse_zip_file(filepath, filename)
        else:
            parsed_data = parse_xml_file(filepath, filename)
        
        if parsed_data:
            # 轉換為 DataFrame
            df = pd.DataFrame(parsed_data)
            
            # 寫入資料庫
            try:
                # 使用 append 模式
                df.to_sql('table_b_history', con=engine, if_exists='append', index=False)
                count = len(df)
                total_inserted += count
                logging.info(f"  -> 成功匯入 {count} 筆資料。")
                
                # 移動檔案至 processed 資料夾
                shutil.move(filepath, os.path.join(XML_PROCESSED_DIR, filename))
                
            except Exception as e:
                logging.error(f"  -> 資料庫寫入失敗: {e}")
        else:
            logging.warning(f"  -> 檔案 {filename} 未提取到任何有效資料。")

    logging.info(f"作業結束。總共匯入 {total_inserted} 筆歷史資料。")

if __name__ == "__main__":
    main()