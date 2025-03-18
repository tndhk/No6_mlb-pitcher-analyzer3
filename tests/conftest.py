# tests/conftest.py
import os
import sys
import pytest
import sqlite3
import pandas as pd
from unittest.mock import patch, MagicMock

# プロジェクトのルートパスをPYTHONPATHに追加
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data_storage.database import Database
from src.data_analysis.pitcher_analyzer import PitcherAnalyzer
from src.data_acquisition.statcast_client import StatcastClient
from src.data_acquisition.batch_processor import BatchProcessor
from src.data_acquisition.team_processor import TeamProcessor

# 先ほど定義した MockDatabase クラスをインポート
from .mock_database import MockDatabase

@pytest.fixture
def test_db():
    """テスト用のモックデータベースを提供するフィクスチャ"""
    return MockDatabase()

@pytest.fixture
def sample_pitch_data():
    """サンプル投球データを提供するフィクスチャ"""
    # サンプルデータの作成
    data = {
        'game_date': pd.date_range(start='2023-04-01', periods=100, freq='D'),
        'player_name': ['Test Pitcher'] * 100,
        'pitcher': [123456] * 100,
        'pitch_type': ['FF'] * 50 + ['SL'] * 30 + ['CH'] * 20,
        'release_speed': [95.0 + i * 0.1 for i in range(50)] + [85.0 + i * 0.1 for i in range(30)] + [83.0 + i * 0.1 for i in range(20)],
        'release_spin_rate': [2400 + i for i in range(50)] + [2600 + i for i in range(30)] + [1800 + i for i in range(20)],
        'pfx_x': [2.0 + i * 0.1 for i in range(50)] + [-2.0 - i * 0.1 for i in range(30)] + [8.0 + i * 0.1 for i in range(20)],
        'pfx_z': [8.0 + i * 0.1 for i in range(50)] + [2.0 + i * 0.1 for i in range(30)] + [4.0 + i * 0.1 for i in range(20)],
        'plate_x': [0.0 + i * 0.1 for i in range(100)],
        'plate_z': [2.5 + i * 0.02 for i in range(100)],
        'description': ['swinging_strike'] * 30 + ['called_strike'] * 20 + ['ball'] * 30 + ['foul'] * 10 + ['hit_into_play'] * 10,
        'zone': [5] * 20 + [1] * 10 + [2] * 10 + [3] * 10 + [4] * 10 + [6] * 10 + [7] * 10 + [8] * 10 + [9] * 10,
        'type': ['S'] * 60 + ['B'] * 30 + ['X'] * 10,
        'launch_speed': [90.0 + i for i in range(10)] + [0.0] * 90,
        'launch_angle': [15.0 + i for i in range(10)] + [0.0] * 90
    }
    
    return pd.DataFrame(data)

@pytest.fixture
def mock_statcast_client():
    """モックされたStatcastクライアントを提供するフィクスチャ"""
    with patch('src.data_acquisition.statcast_client.StatcastClient') as mock:
        client = mock.return_value
        client.get_pitcher_data.return_value = pd.DataFrame()
        client.get_pitcher_id_by_name.return_value = 123456
        client.get_last_n_years_data.return_value = pd.DataFrame()
        client.transform_pitcher_data.return_value = pd.DataFrame()
        yield client

@pytest.fixture
def mock_batch_processor(mock_statcast_client):
    """モックされたBatchProcessorを提供するフィクスチャ"""
    with patch('src.data_acquisition.batch_processor.BatchProcessor') as mock:
        processor = mock.return_value
        processor.client = mock_statcast_client
        processor.process_pitcher_list.return_value = {}
        processor._get_pitcher_data_with_retry.return_value = pd.DataFrame()
        yield processor

@pytest.fixture
def mock_team_processor():
    """モックされたTeamProcessorを提供するフィクスチャ"""
    with patch('src.data_acquisition.team_processor.TeamProcessor') as mock:
        processor = mock.return_value
        processor.get_team_pitchers.return_value = []
        processor.get_team_pitching_stats.return_value = pd.DataFrame()
        processor.get_all_mlb_teams.return_value = [
            "ARI", "ATL", "BAL", "BOS", "CHC", "CHW", "CIN", "CLE", 
            "COL", "DET", "HOU", "KC", "LAA", "LAD", "MIA", "MIL", 
            "MIN", "NYM", "NYY", "OAK", "PHI", "PIT", "SD", "SEA", 
            "SF", "STL", "TB", "TEX", "TOR", "WSH"
        ]
        yield processor

@pytest.fixture
def populated_test_db(test_db, sample_pitch_data):
    """サンプルデータが入力されたテスト用データベースを提供するフィクスチャ"""
    # 球種データの初期化
    pitch_types = [
        {'code': 'FF', 'name': 'Four-Seam Fastball', 'description': 'Standard four-seam fastball'},
        {'code': 'SL', 'name': 'Slider', 'description': 'Breaking ball with lateral and downward movement'},
        {'code': 'CH', 'name': 'Changeup', 'description': 'Off-speed pitch that mimics fastball arm action'}
    ]
    test_db.insert_pitch_types(pitch_types)
    
    # ピッチャーデータの挿入
    pitcher_id = test_db.insert_pitcher(123456, 'Test Pitcher', 'NYY')
    
    # 試合データの挿入
    game_id = test_db.insert_game('2023-04-01', 'NYY', 'BOS', 2023)
    
    # 投球データの変換と挿入
    pitches = []
    for _, row in sample_pitch_data.iterrows():
        pitch_type_id = test_db.get_pitch_type_id(row['pitch_type'])
        
        is_strike = 'strike' in row['description'] or row['description'] in ['swinging_strike', 'called_strike', 'foul', 'foul_tip']
        is_swing = row['description'] in ['swinging_strike', 'foul', 'foul_tip', 'hit_into_play']
        is_whiff = row['description'] == 'swinging_strike'
        is_in_zone = 1 <= row['zone'] <= 9
        
        pitches.append({
            'pitcher_id': pitcher_id,
            'game_id': game_id,
            'pitch_type_id': pitch_type_id,
            'release_speed': row['release_speed'],
            'release_spin_rate': row['release_spin_rate'],
            'pfx_x': row['pfx_x'],
            'pfx_z': row['pfx_z'],
            'plate_x': row['plate_x'],
            'plate_z': row['plate_z'],
            'description': row['description'],
            'zone': row['zone'],
            'type': row['type'],
            'launch_speed': row['launch_speed'],
            'launch_angle': row['launch_angle'],
            'is_strike': is_strike,
            'is_swing': is_swing,
            'is_whiff': is_whiff,
            'is_in_zone': is_in_zone
        })
    
    test_db.insert_pitches(pitches)
    
    # パフォーマンス指標の追加
    metrics = {
        'pitcher_id': pitcher_id,
        'season': 2023,
        'era': 3.45,
        'fip': 3.56,
        'whip': 1.21,
        'k_per_9': 9.5,
        'bb_per_9': 2.8,
        'hr_per_9': 1.2,
        'swstr_pct': 11.5,
        'csw_pct': 30.2,
        'o_swing_pct': 32.5,
        'z_contact_pct': 85.3,
        'innings_pitched': 180.2,
        'games': 30,
        'strikeouts': 182,
        'walks': 55,
        'home_runs': 22,
        'hits': 165,
        'earned_runs': 69
    }
    test_db.update_pitcher_metrics(metrics)
    
    # 球種使用割合の追加
    for pitch_type, code, usage_pct, velo, spin in [
        ('Four-Seam Fastball', 'FF', 50.0, 95.5, 2425),
        ('Slider', 'SL', 30.0, 85.5, 2615),
        ('Changeup', 'CH', 20.0, 84.0, 1810)
    ]:
        pitch_type_id = test_db.get_pitch_type_id(code)
        
        usage_data = {
            'pitcher_id': pitcher_id,
            'pitch_type_id': pitch_type_id,
            'season': 2023,
            'usage_pct': usage_pct,
            'avg_velocity': velo,
            'avg_spin_rate': spin,
            'avg_pfx_x': 5.0 if code == 'CH' else (2.5 if code == 'FF' else -2.5),
            'avg_pfx_z': 8.5 if code == 'FF' else (2.5 if code == 'SL' else 5.0),
            'whiff_pct': 25.0 if code == 'SL' else (15.0 if code == 'CH' else 10.0)
        }
        
        test_db.update_pitch_usage(usage_data)
    
    return test_db

@pytest.fixture
def pitcher_analyzer(populated_test_db):
    """投手分析クラスのインスタンスを提供するフィクスチャ"""
    return PitcherAnalyzer(populated_test_db)