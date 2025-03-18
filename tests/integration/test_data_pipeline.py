# tests/integration/test_data_pipeline.py
import pytest
import pandas as pd
import os
import sqlite3
from unittest.mock import patch, MagicMock

from src.data_acquisition.statcast_client import StatcastClient
from src.data_acquisition.batch_processor import BatchProcessor
from src.data_storage.database import Database
from src.data_storage.data_manager import DataManager
from src.data_analysis.pitcher_analyzer import PitcherAnalyzer

class TestDataPipeline:
    """
    データ取得から分析までのパイプラインの結合テスト
    """
    
    @pytest.fixture
    def mock_statcast_response(self):
        """モックのStatcastレスポンスデータを提供するフィクスチャ"""
        # サンプルデータの作成（実際のAPI応答を模倣）
        data = {
            'game_date': pd.date_range(start='2023-04-01', periods=50, freq='D'),
            'player_name': ['Gerrit Cole'] * 50,
            'pitcher': [543037] * 50,  # Gerrit ColeのMLB ID
            'pitch_type': ['FF'] * 25 + ['SL'] * 15 + ['CU'] * 10,
            'release_speed': [95.5 + i * 0.1 for i in range(25)] + 
                           [87.5 + i * 0.1 for i in range(15)] + 
                           [83.5 + i * 0.1 for i in range(10)],
            'release_spin_rate': [2400 + i * 5 for i in range(25)] + 
                                [2600 + i * 5 for i in range(15)] + 
                                [2800 + i * 5 for i in range(10)],
            'pfx_x': [1.5 + i * 0.05 for i in range(25)] + 
                    [-3.5 - i * 0.05 for i in range(15)] + 
                    [10.0 + i * 0.05 for i in range(10)],
            'pfx_z': [9.0 + i * 0.05 for i in range(25)] + 
                    [2.0 + i * 0.05 for i in range(15)] + 
                    [-8.0 - i * 0.05 for i in range(10)],
            'plate_x': [0.0 + i * 0.1 for i in range(50)],
            'plate_z': [2.5 + i * 0.05 for i in range(50)],
            'description': ['swinging_strike'] * 15 + ['called_strike'] * 10 + 
                          ['ball'] * 15 + ['foul'] * 5 + ['hit_into_play'] * 5,
            'zone': [5] * 10 + [1] * 5 + [2] * 5 + [3] * 5 + [4] * 5 + 
                   [6] * 5 + [7] * 5 + [8] * 5 + [9] * 5,
            'type': ['S'] * 30 + ['B'] * 15 + ['X'] * 5,
            'launch_speed': [90.0 + i for i in range(5)] + [None] * 45,
            'launch_angle': [20.0 + i for i in range(5)] + [None] * 45,
            'home_team': ['NYY'] * 25 + ['BOS'] * 25,
            'away_team': ['BOS'] * 25 + ['NYY'] * 25
        }
        
        return pd.DataFrame(data)
    
    @pytest.mark.integration
    def test_data_acquisition_to_storage(self, mock_statcast_response):
        """データ取得からストレージへの統合テスト"""
        # テスト用の一時DBファイルパス
        db_path = "test_integration_pipeline.db"
        
        try:
            # 既存のテストDBがあれば削除
            if os.path.exists(db_path):
                os.remove(db_path)
            
            # モックの設定
            with patch('src.data_acquisition.statcast_client.statcast_pitcher') as mock_statcast:
                # StatcastのAPIレスポンスをモック
                mock_statcast.return_value = mock_statcast_response
                
                # 実際のコンポーネントの初期化（StatcastClientのみモック）
                client = StatcastClient()
                db = Database(db_path)
                data_manager = DataManager(db)
                
                # Gerrit ColeのIDを設定
                pitcher_mlb_id = 543037
                pitcher_name = "Gerrit Cole"
                team = "NYY"
                
                # データ取得
                data = client.get_pitcher_data(pitcher_mlb_id, '2023-04-01', '2023-04-30')
                assert not data.empty, "データが空です"
                
                # データ変換
                transformed_data = client.transform_pitcher_data(data)
                assert not transformed_data.empty, "変換後のデータが空です"
                
                # データベースに投手情報を登録
                db_pitcher_id = db.insert_pitcher(pitcher_mlb_id, pitcher_name, team)
                
                # データ処理と保存
                data_manager.process_statcast_data(db_pitcher_id, pitcher_mlb_id, pitcher_name, transformed_data, team)
                
                # データがDBに正しく保存されたかチェック
                conn = db._get_connection()
                cursor = conn.cursor()
                
                # 投手情報のチェック
                cursor.execute("SELECT * FROM pitchers WHERE mlb_id = ?", (pitcher_mlb_id,))
                pitcher_data = cursor.fetchone()
                assert pitcher_data is not None, "投手データが保存されていません"
                assert pitcher_data['name'] == pitcher_name, "投手名が正しくありません"
                
                # 投球データのチェック
                cursor.execute("SELECT COUNT(*) as count FROM pitches WHERE pitcher_id = ?", (db_pitcher_id,))
                pitches_count = cursor.fetchone()['count']
                assert pitches_count > 0, "投球データが保存されていません"
                
                # 球種使用割合のチェック
                cursor.execute("SELECT COUNT(*) as count FROM pitch_usage WHERE pitcher_id = ?", (db_pitcher_id,))
                usage_count = cursor.fetchone()['count']
                assert usage_count > 0, "球種使用割合データが保存されていません"
                
                # 成績指標のチェック
                cursor.execute("SELECT COUNT(*) as count FROM pitcher_metrics WHERE pitcher_id = ?", (db_pitcher_id,))
                metrics_count = cursor.fetchone()['count']
                assert metrics_count > 0, "成績指標データが保存されていません"
                
                conn.close()
                
                # 期待通りの結果を返すことを確認
                mock_statcast.assert_called_once()
                
        finally:
            # テスト後のクリーンアップ
            if os.path.exists(db_path):
                os.remove(db_path)
                
    @pytest.mark.integration
    def test_storage_to_analysis(self, mock_statcast_response):
        """データストレージから分析までの統合テスト"""
        # テスト用の一時DBファイルパス
        db_path = "test_integration_analysis.db"
        
        try:
            # 既存のテストDBがあれば削除
            if os.path.exists(db_path):
                os.remove(db_path)
            
            # DBと必要なコンポーネントを初期化
            db = Database(db_path)
            data_manager = DataManager(db)
            analyzer = PitcherAnalyzer(db)
            
            # テストデータの準備
            pitcher_mlb_id = 543037
            pitcher_name = "Gerrit Cole"
            team = "NYY"
            
            # 投手情報をDBに登録
            db_pitcher_id = db.insert_pitcher(pitcher_mlb_id, pitcher_name, team)
            
            # サンプルデータをDBに保存
            transformed_data = pd.DataFrame(mock_statcast_response)
            data_manager.process_statcast_data(db_pitcher_id, pitcher_mlb_id, pitcher_name, transformed_data, team)
            
            # 分析の実行
            # 1. 投手サマリーの取得
            summary = analyzer.get_pitcher_summary(db_pitcher_id)
            
            # 検証
            assert summary is not None, "投手サマリーがありません"
            assert summary['name'] == pitcher_name, "投手名が正しくありません"
            assert summary['team'] == team, "チームが正しくありません"
            assert 'metrics' in summary, "指標データがありません"
            assert 'pitch_types' in summary, "球種データがありません"
            assert len(summary['pitch_types']) > 0, "球種データが空です"
            
            # 2. 球種分析
            pitch_types = summary['pitch_types']
            ff_data = next((p for p in pitch_types if p['code'] == 'FF'), None)
            sl_data = next((p for p in pitch_types if p['code'] == 'SL'), None)
            
            assert ff_data is not None, "フォーシームのデータがありません"
            assert sl_data is not None, "スライダーのデータがありません"
            
            # 3. 球種使用割合のチェック
            assert ff_data['usage_pct'] > 0, "フォーシームの使用割合が0です"
            assert sl_data['usage_pct'] > 0, "スライダーの使用割合が0です"
            
            # 合計が100%に近いことを確認
            total_usage = sum(p['usage_pct'] for p in pitch_types)
            assert abs(total_usage - 100.0) < 1.0, f"球種使用割合の合計が100%ではありません: {total_usage}"
            
        finally:
            # テスト後のクリーンアップ
            if os.path.exists(db_path):
                os.remove(db_path)
                
    @pytest.mark.integration
    def test_batch_processing(self):
        """バッチ処理の統合テスト"""
        # テスト用の一時DBファイルパス
        db_path = "test_integration_batch.db"
        
        try:
            # 既存のテストDBがあれば削除
            if os.path.exists(db_path):
                os.remove(db_path)
            
            # コンポーネントをモックで初期化
            mock_client = MagicMock()
            mock_client.get_last_n_years_data.return_value = pd.DataFrame({
                'game_date': pd.date_range(start='2023-04-01', periods=10),
                'player_name': ['Pitcher 1'] * 10,
                'pitch_type': ['FF'] * 5 + ['SL'] * 5,
                'release_speed': [95.0] * 5 + [85.0] * 5
            })
            mock_client.transform_pitcher_data.return_value = mock_client.get_last_n_years_data.return_value
            
            # バッチプロセッサの初期化
            batch_processor = BatchProcessor(mock_client, max_workers=2, rate_limit_pause=0.1)
            
            # テスト用の投手IDリスト
            pitcher_ids = [123456, 234567, 345678]
            
            # バッチ処理の実行
            results = batch_processor.process_pitcher_list(pitcher_ids, years=1)
            
            # 検証
            assert isinstance(results, dict), "結果が辞書ではありません"
            assert len(results) == len(pitcher_ids), f"処理された投手数が異なります: expected={len(pitcher_ids)}, actual={len(results)}"
            
            # 各投手のデータが取得されたことを確認
            for pitcher_id in pitcher_ids:
                assert pitcher_id in results, f"投手ID {pitcher_id} のデータがありません"
                assert not results[pitcher_id].empty, f"投手ID {pitcher_id} のデータが空です"
            
            # モックが各投手に対して呼び出されたことを確認
            assert mock_client.get_last_n_years_data.call_count == len(pitcher_ids)
            assert mock_client.transform_pitcher_data.call_count == len(pitcher_ids)
            
        finally:
            # テスト後のクリーンアップ
            if os.path.exists(db_path):
                os.remove(db_path)