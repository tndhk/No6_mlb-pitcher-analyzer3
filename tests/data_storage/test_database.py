# tests/data_storage/test_database.py
import pytest
import sqlite3
from unittest.mock import patch, MagicMock

from src.data_storage.database import Database

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
        
        # 検証
        conn = test_db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pitch_types WHERE code = 'CU'")
        result = cursor.fetchone()
        conn.close()
        
        assert result is not None
        assert result['name'] == 'Curveball'
        
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
        
    def test_insert_pitcher(self, test_db):
        """投手情報の挿入テスト"""
        # テスト実行
        pitcher_id = test_db.insert_pitcher(123789, 'Test Pitcher', 'LAD')
        
        # 検証
        conn = test_db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pitchers WHERE mlb_id = 123789")
        result = cursor.fetchone()
        conn.close()
        
        assert result is not None
        assert result['name'] == 'Test Pitcher'
        assert result['team'] == 'LAD'
        assert pitcher_id == result['id']
        
    def test_get_pitcher_id(self, test_db):
        """投手IDの取得テスト"""
        # テストデータの挿入
        test_db.insert_pitcher(123789, 'Test Pitcher', 'LAD')
        
        # テスト実行
        pitcher_id = test_db.get_pitcher_id(123789)
        
        # 検証
        assert pitcher_id is not None
        
    def test_insert_game(self, test_db):
        """試合情報の挿入テスト"""
        # テスト実行
        game_id = test_db.insert_game('2023-05-01', 'LAD', 'SFG', 2023)
        
        # 検証
        conn = test_db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM games WHERE game_date = '2023-05-01'")
        result = cursor.fetchone()
        conn.close()
        
        assert result is not None
        assert result['home_team'] == 'LAD'
        assert result['away_team'] == 'SFG'
        assert result['season'] == 2023
        assert game_id == result['id']
        
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
        
        # 検証
        conn = test_db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pitches WHERE pitcher_id = ?", (pitcher_id,))
        result = cursor.fetchone()
        conn.close()
        
        assert result is not None
        assert result['release_speed'] == 95.0
        assert result['is_strike'] == 1  # SQLiteでは真偽値は0/1で保存
        
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
        
        # 検証
        conn = test_db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pitcher_metrics WHERE pitcher_id = ? AND season = ?", 
                      (pitcher_id, 2023))
        result = cursor.fetchone()
        conn.close()
        
        assert result is not None
        assert result['era'] == 3.45
        assert result['whip'] == 1.21
        assert result['k_per_9'] == 9.5
        
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
        
        # 検証
        conn = test_db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pitch_usage WHERE pitcher_id = ? AND pitch_type_id = ? AND season = ?", 
                      (pitcher_id, pitch_type_id, 2023))
        result = cursor.fetchone()
        conn.close()
        
        assert result is not None
        assert result['usage_pct'] == 60.5
        assert result['avg_velocity'] == 95.5
        assert result['whiff_pct'] == 10.2
        
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