# tests/data_acquisition/test_statcast_client.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.data_acquisition.statcast_client import StatcastClient

class TestStatcastClient:
    
    def test_init(self):
        """StatcastClientの初期化テスト"""
        client = StatcastClient()
        assert client is not None
        
    @patch('src.data_acquisition.statcast_client.statcast_pitcher')
    def test_get_pitcher_data(self, mock_statcast_pitcher):
        """get_pitcher_dataメソッドのテスト"""
        # モックの設定
        mock_data = pd.DataFrame({
            'pitch_type': ['FF', 'SL'],
            'release_speed': [95.0, 85.0]
        })
        mock_statcast_pitcher.return_value = mock_data
        
        # テスト
        client = StatcastClient()
        result = client.get_pitcher_data(123456, '2023-04-01', '2023-04-30')
        
        # 検証
        mock_statcast_pitcher.assert_called_once_with(
            start_dt='2023-04-01', 
            end_dt='2023-04-30', 
            player_id=123456
        )
        assert result.equals(mock_data)
        
    @patch('src.data_acquisition.statcast_client.playerid_lookup')
    def test_get_pitcher_id_by_name(self, mock_playerid_lookup):
        """get_pitcher_id_by_nameメソッドのテスト"""
        # モックの設定
        mock_data = pd.DataFrame({
            'mlbam': [123456],
            'name_first': ['Test'],
            'name_last': ['Pitcher']
        })
        mock_playerid_lookup.return_value = mock_data
        
        # テスト
        client = StatcastClient()
        result = client.get_pitcher_id_by_name('Test', 'Pitcher')
        
        # 検証
        mock_playerid_lookup.assert_called_once_with('Pitcher', 'Test')
        assert result == 123456
        
    @patch('src.data_acquisition.statcast_client.StatcastClient.get_pitcher_data')
    def test_get_last_n_years_data(self, mock_get_pitcher_data):
        """get_last_n_years_dataメソッドのテスト"""
        # モックの設定
        mock_data = pd.DataFrame({
            'pitch_type': ['FF', 'SL'],
            'release_speed': [95.0, 85.0]
        })
        mock_get_pitcher_data.return_value = mock_data
        
        # テスト
        client = StatcastClient()
        result = client.get_last_n_years_data(123456, 3)
        
        # 検証
        mock_get_pitcher_data.assert_called_once()
        assert result.equals(mock_data)
        
    def test_transform_pitcher_data(self):
        """transform_pitcher_dataメソッドのテスト"""
        # テストデータ
        input_data = pd.DataFrame({
            'game_date': ['2023-04-01', '2023-04-02'],
            'player_name': ['Test Pitcher', 'Test Pitcher'],
            'pitcher': [123456, 123456],
            'pitch_type': ['FF', 'SL'],
            'release_speed': [95.0, 85.0],
            'release_spin_rate': [2400, 2600],
            'pfx_x': [2.0, -2.0],
            'pfx_z': [8.0, 2.0],
            'description': ['swinging_strike', 'called_strike']
        })
        
        # テスト
        client = StatcastClient()
        result = client.transform_pitcher_data(input_data)
        
        # 検証
        assert 'game_date' in result.columns
        assert 'pitch_type' in result.columns
        assert 'release_speed' in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result['game_date'])
