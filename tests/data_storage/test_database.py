# tests/data_storage/test_database.py
import pytest
import sqlite3
from unittest.mock import patch, MagicMock

class TestDatabase:
    
    def test_init(self, test_db):
        """データベースの初期化テスト"""
        assert test_db is not None
        
    def test_insert_pitch_types(self, test_db):
        """球種の挿入テスト"""
        # テストデータ
        pitch_types = [
            {'code': 'CU', 'name': 'Curveball', 'description': 'Breaking ball with significant downward movement'}
        ]
        
        # テスト実行
        test_db.insert_pitch_types(pitch_types)
        
        # 検証 - SQLクエリではなく直接モックデータにアクセス
        pitch_id = test_db.get_pitch_type_id('CU')
        assert pitch_id is not None
        assert test_db.pitch_types[pitch_id]['name'] == 'Curveball'
        
    def test_get_pitch_type_id(self, test_db):
        """球種IDの取得テスト"""
        # テストデータの挿入
        pitch_types = [
            {'code': 'SV', 'name': 'Slurve', 'description': 'Combination slider/curveball'}
        ]
        test_db.insert_pitch_types(pitch_types)
        
        # テスト実行
        pitch_id = test_db.get_pitch_type_id('SV')
        
        # 検証
        assert pitch_id is not None
        assert 'SV' == test_db.pitch_types[pitch_id]['code']
        
    def test_insert_pitcher(self, test_db):
        """投手情報の挿入テスト"""
        # テスト実行
        pitcher_id = test_db.insert_pitcher(123789, 'Test Pitcher', 'LAD')
        
        # 検証 - SQLクエリではなく直接モックデータにアクセス
        assert pitcher_id in test_db.pitchers
        assert test_db.pitchers[pitcher_id]['name'] == 'Test Pitcher'
        assert test_db.pitchers[pitcher_id]['team'] == 'LAD'
        assert test_db.pitchers[pitcher_id]['mlb_id'] == 123789
        
    def test_get_pitcher_id(self, test_db):
        """投手IDの取得テスト"""
        # テストデータの挿入
        test_db.insert_pitcher(123789, 'Test Pitcher', 'LAD')
        
        # テスト実行
        pitcher_id = test_db.get_pitcher_id(123789)
        
        # 検証
        assert pitcher_id is not None
        assert test_db.pitchers[pitcher_id]['mlb_id'] == 123789
        
    def test_insert_game(self, test_db):
        """試合情報の挿入テスト"""
        # テスト実行
        game_id = test_db.insert_game('2023-05-01', 'LAD', 'SFG', 2023)
        
        # 検証 - SQLクエリではなく直接モックデータにアクセス
        assert game_id in test_db.games
        assert test_db.games[game_id]['home_team'] == 'LAD'
        assert test_db.games[game_id]['away_team'] == 'SFG'
        assert test_db.games[game_id]['season'] == 2023
        
    def test_insert_pitches(self, test_db):
        """投球データの挿入テスト"""
        # 前提データの挿入
        pitcher_id = test_db.insert_pitcher(123789, 'Test Pitcher', 'LAD')
        game_id = test_db.insert_game('2023-05-01', 'LAD', 'SFG', 2023)
        pitch_types = [{'code': 'FF', 'name': 'Four-Seam Fastball', 'description': 'Standard fastball'}]
        test_db.insert_pitch_types(pitch_types)
        pitch_type_id = test_db.get_pitch_type_id('FF')
        
        # テストデータ
        pitches = [
            {
                'pitcher_id': pitcher_id,
                'game_id': game_id,
                'pitch_type_id': pitch_type_id,
                'release_speed': 95.0,
                'release_spin_rate': 2400,
                'pfx_x': 2.0,
                'pfx_z': 8.0,
                'plate_x': 0.0,
                'plate_z': 2.5,
                'description': 'swinging_strike',
                'zone': 5,
                'type': 'S',
                'launch_speed': None,
                'launch_angle': None,
                'is_strike': True,
                'is_swing': True,
                'is_whiff': True,
                'is_in_zone': True
            }
        ]
        
        # テスト実行
        test_db.insert_pitches(pitches)
        
        # 検証 - SQLクエリではなく直接モックデータにアクセス
        # 投球データは挿入された順にモックデータに格納されるので、最初のIDを探す
        pitch_id = 1  # モックデータベースでは最初のIDは1になるはず
        assert pitch_id in test_db.pitches
        pitch_data = test_db.pitches[pitch_id]
        assert pitch_data['release_speed'] == pytest.approx(95.0)  # 浮動小数点数なので許容誤差を設定
        assert pitch_data['is_strike'] is True
        
    def test_update_pitcher_metrics(self, test_db):
        """投手成績指標の更新テスト"""
        # 前提データの挿入
        pitcher_id = test_db.insert_pitcher(123789, 'Test Pitcher', 'LAD')
        
        # テストデータ
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
        
        # テスト実行
        test_db.update_pitcher_metrics(metrics)
        
        # 検証 - SQLクエリではなく直接モックデータにアクセス
        key = (pitcher_id, 2023)
        assert key in test_db.pitcher_metrics
        saved_metrics = test_db.pitcher_metrics[key]
        assert saved_metrics['era'] == pytest.approx(3.45)  # 浮動小数点数なので許容誤差を設定
        assert saved_metrics['whip'] == pytest.approx(1.21)
        assert saved_metrics['k_per_9'] == pytest.approx(9.5)
        
    def test_update_pitch_usage(self, test_db):
        """球種使用割合の更新テスト"""
        # 前提データの挿入
        pitcher_id = test_db.insert_pitcher(123789, 'Test Pitcher', 'LAD')
        pitch_types = [{'code': 'FF', 'name': 'Four-Seam Fastball', 'description': 'Standard fastball'}]
        test_db.insert_pitch_types(pitch_types)
        pitch_type_id = test_db.get_pitch_type_id('FF')
        
        # テストデータ
        usage_data = {
            'pitcher_id': pitcher_id,
            'pitch_type_id': pitch_type_id,
            'season': 2023,
            'usage_pct': 60.5,
            'avg_velocity': 95.5,
            'avg_spin_rate': 2425,
            'avg_pfx_x': 2.5,
            'avg_pfx_z': 8.5,
            'whiff_pct': 10.2
        }
        
        # テスト実行
        test_db.update_pitch_usage(usage_data)
        
        # 検証 - SQLクエリではなく直接モックデータにアクセス
        key = (pitcher_id, pitch_type_id, 2023)
        assert key in test_db.pitch_usage
        usage = test_db.pitch_usage[key]
        assert usage['usage_pct'] == pytest.approx(60.5)  # 浮動小数点数なので許容誤差を設定
        assert usage['avg_velocity'] == pytest.approx(95.5)
        assert usage['whiff_pct'] == pytest.approx(10.2)
        
    def test_search_pitchers(self, test_db):
        """投手検索テスト"""
        # テストデータの挿入
        test_db.insert_pitcher(123789, 'John Smith', 'LAD')
        test_db.insert_pitcher(234567, 'Jane Smith', 'NYY')
        test_db.insert_pitcher(345678, 'Bob Johnson', 'BOS')
        
        # テスト実行
        results = test_db.search_pitchers('Smith')
        
        # 検証
        assert len(results) == 2
        names = [r['name'] for r in results]
        assert 'John Smith' in names
        assert 'Jane Smith' in names
        
    def test_get_all_teams(self, test_db):
        """全チーム取得テスト"""
        # テストデータの挿入
        test_db.insert_pitcher(123789, 'Player1', 'LAD')
        test_db.insert_pitcher(234567, 'Player2', 'NYY')
        test_db.insert_pitcher(345678, 'Player3', 'BOS')
        
        # テスト実行
        teams = test_db.get_all_teams()
        
        # 検証
        assert len(teams) == 3
        assert 'LAD' in teams
        assert 'NYY' in teams
        assert 'BOS' in teams