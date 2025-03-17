# tests/data_storage/test_data_manager.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.data_storage.data_manager import DataManager
from src.data_storage.database import Database

class TestDataManager:
    
    def test_init(self, test_db):
        """DataManagerの初期化テスト"""
        manager = DataManager(test_db)
        assert manager is not None
        assert manager.db is test_db
        
    def test_initialize_pitch_types(self, test_db):
        """球種初期化テスト"""
        # テスト実行
        manager = DataManager(test_db)
        
        # 検証
        conn = test_db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM pitch_types")
        result = cursor.fetchone()
        count = result['count']
        conn.close()
        
        assert count >= 10  # 複数の球種が登録されていることを確認
        
    def test_process_statcast_data(self, test_db, sample_pitch_data):
        """Statcastデータ処理テスト"""
        # DataManagerのインスタンス化
        manager = DataManager(test_db)
        
        # テスト実行
        manager.process_statcast_data(1, 123456, 'Test Pitcher', sample_pitch_data)
        
        # 検証
        conn = test_db._get_connection()
        cursor = conn.cursor()
        
        # 投球データが挿入されたか確認
        cursor.execute("SELECT COUNT(*) as count FROM pitches")
        result = cursor.fetchone()
        pitches_count = result['count']
        
        # 球種使用割合が計算されたか確認
        cursor.execute("SELECT COUNT(*) as count FROM pitch_usage")
        result = cursor.fetchone()
        usage_count = result['count']
        
        # 成績指標が計算されたか確認
        cursor.execute("SELECT COUNT(*) as count FROM pitcher_metrics")
        result = cursor.fetchone()
        metrics_count = result['count']
        
        conn.close()
        
        assert pitches_count > 0
        assert usage_count > 0
        assert metrics_count > 0