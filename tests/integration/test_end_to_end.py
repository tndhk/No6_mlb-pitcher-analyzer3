# tests/integration/test_end_to_end.py
import pytest
import pandas as pd
import os
import sqlite3
from unittest.mock import patch, MagicMock

from src.data_acquisition.statcast_client import StatcastClient
from src.data_storage.database import Database
from src.data_storage.data_manager import DataManager
from src.data_analysis.pitcher_analyzer import PitcherAnalyzer

class TestEndToEnd:
    
    @pytest.mark.skip(reason="This is a slow integration test that requires internet connection")
    def test_data_pipeline(self):
        """データパイプラインの統合テスト（実際のAPI呼び出しは避ける）"""
        # テスト用の一時DBファイルパス
        db_path = "test_integration.db"
        
        try:
            # 既存のテストDBがあれば削除
            if os.path.exists(db_path):
                os.remove(db_path)
            
            # 1. DB初期化
            db = Database(db_path)
            
            # 2. DataManagerの初期化
            data_manager = DataManager(db)
            
            # 3. StatcastClientの初期化
            client = StatcastClient()
            
            # 4. テスト用の投手IDを取得（Gerrit Cole）
            pitcher_id = client.get_pitcher_id_by_name("Gerrit", "Cole")
            assert pitcher_id is not None
            
            # 5. 特定期間のデータを取得（短い期間のみ、APIレート制限を考慮）
            data = client.get_pitcher_data(pitcher_id, "2023-04-01", "2023-04-15")
            assert not data.empty
            
            # 6. データを変換
            transformed_data = client.transform_pitcher_data(data)
            assert not transformed_data.empty
            
            # 7. データをDBに保存
            db_pitcher_id = db.insert_pitcher(pitcher_id, "Gerrit Cole", "NYY")
            data_manager.process_statcast_data(db_pitcher_id, pitcher_id, "Gerrit Cole", transformed_data, "NYY")
            
            # 8. 分析クラスの初期化
            analyzer = PitcherAnalyzer(db)
            
            # 9. 投手サマリーの取得と検証
            summary = analyzer.get_pitcher_summary(db_pitcher_id)
            assert summary is not None
            assert summary['name'] == "Gerrit Cole"
            assert summary['team'] == "NYY"
            assert len(summary['pitch_types']) > 0
            
            # 成功
            print("End-to-end test successful!")
            
        finally:
            # テスト後のクリーンアップ
            if os.path.exists(db_path):
                os.remove(db_path)
    
    @pytest.mark.integration
    def test_mock_end_to_end(self, sample_pitch_data):
        """モックデータを使用した統合テスト"""
        # テスト用の一時DBファイルパス
        db_path = "test_mock_integration.db"
        
        try:
            # 既存のテストDBがあれば削除
            if os.path.exists(db_path):
                os.remove(db_path)
            
            # モックのStatcastClientを作成
            mock_client = MagicMock()
            mock_client.get_pitcher_id_by_name.return_value = 123456
            mock_client.get_pitcher_data.return_value = sample_pitch_data
            mock_client.transform_pitcher_data.return_value = sample_pitch_data
            
            # 実際のDBとDataManagerを使用
            db = Database(db_path)
            data_manager = DataManager(db)
            
            # 投手情報を登録
            pitcher_name = "Test Pitcher"
            team = "NYY"
            mlb_id = 123456
            
            # DBに投手情報を登録
            db_pitcher_id = db.insert_pitcher(mlb_id, pitcher_name, team)
            
            # データを処理
            data_manager.process_statcast_data(db_pitcher_id, mlb_id, pitcher_name, sample_pitch_data, team)
            
            # 分析クラスを使用
            analyzer = PitcherAnalyzer(db)
            
            # 投手サマリーを取得
            summary = analyzer.get_pitcher_summary(db_pitcher_id)
            
            # 検証
            assert summary is not None
            assert summary['name'] == pitcher_name
            assert summary['team'] == team
            assert len(summary['pitch_types']) > 0
            
            # 球種割合の検証
            pitch_types = summary['pitch_types']
            assert len(pitch_types) == 3  # FF, SL, CH
            
            # フォーシームが最も多く使われている
            assert pitch_types[0]['code'] == 'FF'
            assert pitch_types[0]['usage_pct'] == 50.0
            
            # スライダーが2番目
            assert pitch_types[1]['code'] == 'SL'
            assert pitch_types[1]['usage_pct'] == 30.0
            
            # チェンジアップが3番目
            assert pitch_types[2]['code'] == 'CH'
            assert pitch_types[2]['usage_pct'] == 20.0
            
        finally:
            # テスト後のクリーンアップ
            if os.path.exists(db_path):
                os.remove(db_path)