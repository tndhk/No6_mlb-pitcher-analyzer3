# tests/data_acquisition/test_team_processor.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.data_acquisition.team_processor import TeamProcessor

class TestTeamProcessor:
    
    def test_init(self):
        """TeamProcessorの初期化テスト"""
        processor = TeamProcessor()
        assert processor is not None
        
    @patch('src.data_acquisition.team_processor.pitching_stats')
    @patch('src.data_acquisition.team_processor.playerid_lookup')
    def test_get_team_pitchers(self, mock_playerid_lookup, mock_pitching_stats):
        """get_team_pitchersメソッドのテスト"""
        # モック設定
        mock_stats = pd.DataFrame({
            'Name': ['Test Pitcher', 'Another Pitcher'],
            'Team': ['NYY', 'NYY'],
            'W': [10, 8],
            'ERA': [3.45, 3.78]
        })
        mock_pitching_stats.return_value = mock_stats
        
        # playerid_lookupのモック設定
        mock_player_info = pd.DataFrame({
            'key_mlbam': [123456, 234567],
            'name_first': ['Test', 'Another'],
            'name_last': ['Pitcher', 'Pitcher']
        })
        mock_playerid_lookup.return_value = mock_player_info
        
        # テスト
        processor = TeamProcessor()
        result = processor.get_team_pitchers('NYY', 2023)
        
        # 検証
        mock_pitching_stats.assert_called_once_with(2023, team='NYY')
        assert len(result) == 2
        assert result[0]['name'] == 'Test Pitcher'
        assert result[0]['mlbam_id'] == 123456
        
    @patch('src.data_acquisition.team_processor.team_pitching')
    def test_get_team_pitching_stats(self, mock_team_pitching):
        """get_team_pitching_statsメソッドのテスト"""
        # モック設定
        mock_data = pd.DataFrame({
            'Team': ['NYY', 'NYY'],
            'Season': [2022, 2023],
            'ERA': [3.45, 3.56]
        })
        mock_team_pitching.return_value = mock_data
        
        # テスト
        processor = TeamProcessor()
        result = processor.get_team_pitching_stats('NYY', 2022, 2023)
        
        # 検証
        mock_team_pitching.assert_called_once_with(2022, 2023, team='NYY')
        assert result.equals(mock_data)
        
    def test_get_all_mlb_teams(self):
        """get_all_mlb_teamsメソッドのテスト"""
        processor = TeamProcessor()
        teams = processor.get_all_mlb_teams()
        
        assert len(teams) == 30
        assert 'NYY' in teams
        assert 'LAD' in teams