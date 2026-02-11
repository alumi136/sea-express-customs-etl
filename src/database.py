import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 載入 .env 環境變數
# 確保能從專案根目錄找到 .env 檔
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
dotenv_path = os.path.join(project_root, '.env')

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    print(f"警告: 找不到 .env 設定檔於 {dotenv_path}")

def get_db_engine():
    """
    建立並回傳 SQLAlchemy 的資料庫引擎 (Engine)
    """
    try:
        user = os.getenv('DB_USER', 'root')
        password = os.getenv('DB_PASSWORD', '')
        host = os.getenv('DB_HOST', '127.0.0.1')
        port = os.getenv('DB_PORT', '3306')
        dbname = os.getenv('DB_NAME', 'sea_express')

        # 建立連線字串 (Connection String)
        # 格式: mysql+pymysql://user:password@host:port/dbname?charset=utf8mb4
        connection_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}?charset=utf8mb4"
        
        # 建立引擎 (echo=False 表示不印出所有 SQL 語法，除錯時可改為 True)
        engine = create_engine(connection_str, echo=False)
        return engine
    
    except Exception as e:
        print(f"資料庫連線設定錯誤: {e}")
        return None

# 簡單測試連線用
if __name__ == "__main__":
    engine = get_db_engine()
    if engine:
        try:
            with engine.connect() as connection:
                print("成功連線至 MySQL 資料庫！")
        except Exception as e:
            print(f"連線失敗: {e}")