# tests/data_storage/test_data_manager.py
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from src.data_storage.data_manager import DataManager

class TestDataManager:
    
    def test_init(self):
        """DataManagerの初期化テスト"""
        # データベースをモック
        mock_db = MagicMock()
        
        # モックの振る舞いを設定
        mock_db.insert_pitch_types = MagicMock()
        
        # テスト実行
        manager = DataManager(mock_db)
        
        # 検証
        assert manager is not None
        assert manager.db is mock_db
        # insert_pitch_typesが呼ばれたことを確認
        mock_db.insert_pitch_types.assert_called_once()
        
    def test_initialize_pitch_types(self):
        """球種初期化テスト"""
        # データベースをモック
        mock_db = MagicMock()
        
        # テスト実行
        manager = DataManager(mock_db)
        
        # 検証
        # insert_pitch_typesが呼ばれ、正しいデータが渡されたことを確認
        mock_db.insert_pitch_types.assert_called_once()
        
        # 呼び出し時の引数を取得
        args, _ = mock_db.insert_pitch_types.call_args
        pitch_types = args[0]
        
        # 球種データが含まれていることを確認
        assert len(pitch_types) > 0
        assert any(pt['code'] == 'FF' for pt in pitch_types)
        assert any(pt['code'] == 'SL' for pt in pitch_types)
        assert any(pt['code'] == 'CU' for pt in pitch_types)
        
    def test_process_statcast_data(self):
        """Statcastデータ処理テスト"""
        # データベースをモック
        mock_db = MagicMock()
        
        # サンプル投球データ
        sample_data = pd.DataFrame({
            'game_date': pd.date_range(start='2023-04-01', periods=10),
            'player_name': ['Test Pitcher'] * 10,
            'pitcher': [123456] * 10,
            'pitch_type': ['FF'] * 5 + ['SL'] * 5,
            'release_speed': [95.0] * 5 + [85.0] * 5,
            'release_spin_rate': [2400] * 5 + [2600] * 5,
            'description': ['swinging_strike'] * 3 + ['called_strike'] * 3 + ['ball'] * 4,
            'zone': [5] * 6 + [12] * 4
        })
        
        # モックの振る舞いを設定
        mock_db.get_pitcher_id.return_value = 1
        mock_db.get_pitch_type_id.side_effect = lambda code: 1 if code == 'FF' else 2
        mock_db.insert_game.return_value = 1
        
        # DataManagerのインスタンス化
        manager = DataManager(mock_db)
        
        # テスト実行
        manager.process_statcast_data(1, 123456, 'Test Pitcher', sample_data, 'NYY')
        
        # 検証
        # insert_pitchesが呼ばれたことを確認
        mock_db.insert_pitches.assert_called_once()
        
        # 少なくとも1回 update_pitcher_metrics が呼ばれたことを確認
        assert mock_db.update_pitcher_metrics.call_count >= 1
        
        # 少なくとも1回 update_pitch_usage が呼ばれたことを確認
        assert mock_db.update_pitch_usage.call_count >= 1