import os
import logging
import streamlit as st
from src.visualization.dashboard import Dashboard

def setup_logging():
    """
    ロギングの設定
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("app.log")
        ]
    )

def main():
    """
    アプリケーションのメインエントリポイント
    """
    # ロギングの設定
    setup_logging()
    
    # 環境変数からDB_PATHを取得、なければデフォルト値を使用
    db_path = os.environ.get("DB_PATH", "data/mlb_pitchers.db")
    
    # ダッシュボードの作成と実行
    dashboard = Dashboard(db_path)
    dashboard.run()

if __name__ == "__main__":
    main()