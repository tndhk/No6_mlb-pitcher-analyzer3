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

# tests/conftest.py の test_db フィクスチャを完全に書き換え
@pytest.fixture
def test_db():
    """テスト用のインメモリデータベースを提供するフィクスチャ"""
    # 完全に独立したテスト用データベースクラス
    class TestDatabase:
        def __init__(self):
            self.db_path = ":memory:"
            self.logger = MagicMock()
            self._create_tables()
            
        def _get_connection(self):
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn
            
        def _create_tables(self):
            """テスト用のテーブルを作成"""
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # テーブル作成
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS pitchers (
                id INTEGER PRIMARY KEY,
                mlb_id INTEGER UNIQUE NOT NULL,
                name TEXT NOT NULL,
                team TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS pitch_types (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT
            )
            ''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY,
                game_date DATE NOT NULL,
                home_team TEXT,
                away_team TEXT,
                season INTEGER,
                UNIQUE(game_date, home_team, away_team)
            )
            ''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS pitches (
                id INTEGER PRIMARY KEY,
                pitcher_id INTEGER NOT NULL,
                game_id INTEGER,
                pitch_type_id INTEGER,
                release_speed REAL,
                release_spin_rate REAL,
                pfx_x REAL,
                pfx_z REAL,
                plate_x REAL,
                plate_z REAL,
                description TEXT,
                zone INTEGER,
                type TEXT,
                launch_speed REAL,
                launch_angle REAL,
                is_strike BOOLEAN,
                is_swing BOOLEAN,
                is_whiff BOOLEAN,
                is_in_zone BOOLEAN,
                FOREIGN KEY (pitcher_id) REFERENCES pitchers(id),
                FOREIGN KEY (game_id) REFERENCES games(id),
                FOREIGN KEY (pitch_type_id) REFERENCES pitch_types(id)
            )
            ''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS pitcher_metrics (
                id INTEGER PRIMARY KEY,
                pitcher_id INTEGER NOT NULL,
                season INTEGER NOT NULL,
                era REAL,
                fip REAL,
                whip REAL,
                k_per_9 REAL,
                bb_per_9 REAL,
                hr_per_9 REAL,
                swstr_pct REAL,
                csw_pct REAL,
                o_swing_pct REAL,
                z_contact_pct REAL,
                innings_pitched REAL,
                games INTEGER,
                strikeouts INTEGER,
                walks INTEGER,
                home_runs INTEGER,
                hits INTEGER,
                earned_runs INTEGER,
                FOREIGN KEY (pitcher_id) REFERENCES pitchers(id),
                UNIQUE(pitcher_id, season)
            )
            ''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS pitch_usage (
                id INTEGER PRIMARY KEY,
                pitcher_id INTEGER NOT NULL,
                pitch_type_id INTEGER NOT NULL,
                season INTEGER NOT NULL,
                usage_pct REAL,
                avg_velocity REAL,
                avg_spin_rate REAL,
                avg_pfx_x REAL,
                avg_pfx_z REAL,
                whiff_pct REAL,
                FOREIGN KEY (pitcher_id) REFERENCES pitchers(id),
                FOREIGN KEY (pitch_type_id) REFERENCES pitch_types(id),
                UNIQUE(pitcher_id, pitch_type_id, season)
            )
            ''')
            
            conn.commit()
            conn.close()
            
        # 必要なメソッドを実装
        def insert_pitch_types(self, pitch_types):
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for pitch_type in pitch_types:
                cursor.execute('''
                INSERT OR IGNORE INTO pitch_types (code, name, description)
                VALUES (?, ?, ?)
                ''', (pitch_type['code'], pitch_type['name'], pitch_type.get('description', '')))
            
            conn.commit()
            conn.close()
            
        def get_pitch_type_id(self, pitch_code):
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT id FROM pitch_types WHERE code = ?', (pitch_code,))
            result = cursor.fetchone()
            
            conn.close()
            return result['id'] if result else None
            
        def insert_pitcher(self, mlb_id, name, team=None):
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT OR IGNORE INTO pitchers (mlb_id, name, team)
            VALUES (?, ?, ?)
            ''', (mlb_id, name, team))
            
            cursor.execute('SELECT id FROM pitchers WHERE mlb_id = ?', (mlb_id,))
            pitcher_id = cursor.fetchone()['id']
            
            conn.commit()
            conn.close()
            return pitcher_id
            
        def insert_game(self, game_date, home_team, away_team, season):
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT OR IGNORE INTO games (game_date, home_team, away_team, season)
            VALUES (?, ?, ?, ?)
            ''', (game_date, home_team, away_team, season))
            
            cursor.execute('''
            SELECT id FROM games 
            WHERE game_date = ? AND home_team = ? AND away_team = ?
            ''', (game_date, home_team, away_team))
            
            game_id = cursor.fetchone()['id']
            
            conn.commit()
            conn.close()
            return game_id
            
        def insert_pitches(self, pitches_data):
            if not pitches_data:
                return
                
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for pitch in pitches_data:
                cursor.execute('''
                INSERT INTO pitches (
                    pitcher_id, game_id, pitch_type_id, release_speed, release_spin_rate,
                    pfx_x, pfx_z, plate_x, plate_z, description, zone, type,
                    launch_speed, launch_angle, is_strike, is_swing, is_whiff, is_in_zone
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    pitch['pitcher_id'],
                    pitch.get('game_id'),
                    pitch.get('pitch_type_id'),
                    pitch.get('release_speed'),
                    pitch.get('release_spin_rate'),
                    pitch.get('pfx_x'),
                    pitch.get('pfx_z'),
                    pitch.get('plate_x'),
                    pitch.get('plate_z'),
                    pitch.get('description'),
                    pitch.get('zone'),
                    pitch.get('type'),
                    pitch.get('launch_speed'),
                    pitch.get('launch_angle'),
                    pitch.get('is_strike'),
                    pitch.get('is_swing'),
                    pitch.get('is_whiff'),
                    pitch.get('is_in_zone')
                ))
            
            conn.commit()
            conn.close()
            
        def update_pitcher_metrics(self, metrics):
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT OR REPLACE INTO pitcher_metrics (
                pitcher_id, season, era, fip, whip, k_per_9, bb_per_9, hr_per_9,
                swstr_pct, csw_pct, o_swing_pct, z_contact_pct, innings_pitched,
                games, strikeouts, walks, home_runs, hits, earned_runs
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                metrics['pitcher_id'],
                metrics['season'],
                metrics.get('era'),
                metrics.get('fip'),
                metrics.get('whip'),
                metrics.get('k_per_9'),
                metrics.get('bb_per_9'),
                metrics.get('hr_per_9'),
                metrics.get('swstr_pct'),
                metrics.get('csw_pct'),
                metrics.get('o_swing_pct'),
                metrics.get('z_contact_pct'),
                metrics.get('innings_pitched'),
                metrics.get('games'),
                metrics.get('strikeouts'),
                metrics.get('walks'),
                metrics.get('home_runs'),
                metrics.get('hits'),
                metrics.get('earned_runs')
            ))
            
            conn.commit()
            conn.close()
            
        def update_pitch_usage(self, usage_data):
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT OR REPLACE INTO pitch_usage (
                pitcher_id, pitch_type_id, season, usage_pct, avg_velocity,
                avg_spin_rate, avg_pfx_x, avg_pfx_z, whiff_pct
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                usage_data['pitcher_id'],
                usage_data['pitch_type_id'],
                usage_data['season'],
                usage_data.get('usage_pct'),
                usage_data.get('avg_velocity'),
                usage_data.get('avg_spin_rate'),
                usage_data.get('avg_pfx_x'),
                usage_data.get('avg_pfx_z'),
                usage_data.get('whiff_pct')
            ))
            
            conn.commit()
            conn.close()
            
        def get_pitcher_data(self, pitcher_id):
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM pitchers WHERE id = ?', (pitcher_id,))
            result = cursor.fetchone()
            
            conn.close()
            return dict(result) if result else {}
            
        def get_pitcher_metrics(self, pitcher_id, season=None):
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if season:
                cursor.execute('''
                SELECT * FROM pitcher_metrics 
                WHERE pitcher_id = ? AND season = ?
                ''', (pitcher_id, season))
            else:
                cursor.execute('''
                SELECT * FROM pitcher_metrics 
                WHERE pitcher_id = ?
                ''', (pitcher_id,))
                
            results = cursor.fetchall()
            conn.close()
            
            return [dict(row) for row in results]
            
        def get_pitch_usage_data(self, pitcher_id, season=None):
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if season:
                cursor.execute('''
                SELECT pu.*, pt.code, pt.name
                FROM pitch_usage pu
                JOIN pitch_types pt ON pu.pitch_type_id = pt.id
                WHERE pu.pitcher_id = ? AND pu.season = ?
                ''', (pitcher_id, season))
            else:
                cursor.execute('''
                SELECT pu.*, pt.code, pt.name
                FROM pitch_usage pu
                JOIN pitch_types pt ON pu.pitch_type_id = pt.id
                WHERE pu.pitcher_id = ?
                ''', (pitcher_id,))
                
            results = cursor.fetchall()
            conn.close()
            
            return [dict(row) for row in results]
    
    # 新しいテストデータベースインスタンスを返す
    return TestDatabase()

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