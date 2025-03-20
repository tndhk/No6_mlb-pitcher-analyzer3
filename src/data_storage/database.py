# src/data_storage/database.py
import logging
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple

import pandas as pd

class Database:
    """
    SQLiteデータベースを管理するクラス
    """
    
    def __init__(self, db_path: str, logging_level=logging.INFO):
        """
        Databaseの初期化
        
        Args:
            db_path: SQLiteデータベースファイルのパス
            logging_level: ロギングレベル
        """
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)
        
        # データベースディレクトリが存在しない場合は作成
        db_dir = Path(db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        
        # データベース初期化
        self._initialize_db()
        
    def _get_connection(self) -> sqlite3.Connection:
        """
        データベース接続を取得
        
        Returns:
            Connection: SQLite接続オブジェクト
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # 辞書形式で結果を取得
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database: {str(e)}")
            raise
            
    def _initialize_db(self):
        """
        データベーススキーマの初期化
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # ピッチャーテーブル
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS pitchers (
                id INTEGER PRIMARY KEY,
                mlb_id INTEGER UNIQUE NOT NULL,
                name TEXT NOT NULL,
                team TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            # 球種テーブル
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS pitch_types (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT
            )
            ''')
            
            # 試合テーブル
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
            
            # 投球データテーブル
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
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pitcher_id) REFERENCES pitchers(id),
                FOREIGN KEY (game_id) REFERENCES games(id),
                FOREIGN KEY (pitch_type_id) REFERENCES pitch_types(id)
            )
            ''')
            
            # インデックス作成
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_pitches_pitcher_id ON pitches(pitcher_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_pitches_game_id ON pitches(game_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_pitches_pitch_type_id ON pitches(pitch_type_id)')
            
            # 集計・パフォーマンス指標テーブル
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
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pitcher_id) REFERENCES pitchers(id),
                UNIQUE(pitcher_id, season)
            )
            ''')
            
            # 球種使用割合テーブル
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
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pitcher_id) REFERENCES pitchers(id),
                FOREIGN KEY (pitch_type_id) REFERENCES pitch_types(id),
                UNIQUE(pitcher_id, pitch_type_id, season)
            )
            ''')
            
            conn.commit()
            self.logger.info("Database schema initialized successfully")
            
        except sqlite3.Error as e:
            self.logger.error(f"Error initializing database: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def insert_pitch_types(self, pitch_types: List[Dict[str, str]]):
        """
        球種データをデータベースに挿入
        
        Args:
            pitch_types: 球種情報のリスト [{'code': 'FF', 'name': 'Four-Seam Fastball', 'description': '...'}]
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for pitch_type in pitch_types:
                cursor.execute('''
                INSERT OR IGNORE INTO pitch_types (code, name, description)
                VALUES (?, ?, ?)
                ''', (pitch_type['code'], pitch_type['name'], pitch_type.get('description', '')))
                
            conn.commit()
            self.logger.info(f"Inserted {len(pitch_types)} pitch types")
            
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting pitch types: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def get_pitch_type_id(self, pitch_code: str) -> Optional[int]:
        """
        球種コードからIDを取得
        
        Args:
            pitch_code: 球種コード (FF, SL, CHなど)
            
        Returns:
            int: 球種ID（存在しない場合はNone）
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT id FROM pitch_types WHERE code = ?', (pitch_code,))
            result = cursor.fetchone()
            
            return result['id'] if result else None
            
        except sqlite3.Error as e:
            self.logger.error(f"Error getting pitch type ID: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def insert_pitcher(self, mlb_id: int, name: str, team: Optional[str] = None) -> int:
        """
        ピッチャー情報をデータベースに挿入
        
        Args:
            mlb_id: MLB選手ID
            name: ピッチャー名
            team: チーム略称（オプション）
            
        Returns:
            int: データベース内のピッチャーID
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT OR IGNORE INTO pitchers (mlb_id, name, team)
            VALUES (?, ?, ?)
            ''', (mlb_id, name, team))
            
            # 既存のレコードがある場合はIDを取得
            cursor.execute('SELECT id FROM pitchers WHERE mlb_id = ?', (mlb_id,))
            pitcher_id = cursor.fetchone()['id']
            
            conn.commit()
            return pitcher_id
            
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting pitcher: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def get_pitcher_id(self, mlb_id: int) -> Optional[int]:
        """
        MLB選手IDからデータベース内のピッチャーIDを取得
        
        Args:
            mlb_id: MLB選手ID
            
        Returns:
            int: データベース内のピッチャーID（存在しない場合はNone）
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT id FROM pitchers WHERE mlb_id = ?', (mlb_id,))
            result = cursor.fetchone()
            
            return result['id'] if result else None
            
        except sqlite3.Error as e:
            self.logger.error(f"Error getting pitcher ID: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def insert_game(self, game_date: str, home_team: str, away_team: str, season: int) -> int:
        """
        試合情報をデータベースに挿入
        
        Args:
            game_date: 試合日（YYYY-MM-DD）
            home_team: ホームチーム略称
            away_team: アウェイチーム略称
            season: シーズン年
            
        Returns:
            int: データベース内の試合ID
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT OR IGNORE INTO games (game_date, home_team, away_team, season)
            VALUES (?, ?, ?, ?)
            ''', (game_date, home_team, away_team, season))
            
            # 既存のレコードがある場合はIDを取得
            cursor.execute('''
            SELECT id FROM games 
            WHERE game_date = ? AND home_team = ? AND away_team = ?
            ''', (game_date, home_team, away_team))
            
            game_id = cursor.fetchone()['id']
            
            conn.commit()
            return game_id
            
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting game: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def insert_pitches(self, pitches_data: List[Dict[str, Any]]):
        """
        複数の投球データをバッチで挿入
        
        Args:
            pitches_data: 投球データのリスト
        """
        if not pitches_data:
            self.logger.warning("No pitch data to insert")
            return
            
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # バッチ処理用のパラメータリスト
            values = []
            for pitch in pitches_data:
                values.append((
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
            
            cursor.executemany('''
            INSERT INTO pitches (
                pitcher_id, game_id, pitch_type_id, release_speed, release_spin_rate,
                pfx_x, pfx_z, plate_x, plate_z, description, zone, type,
                launch_speed, launch_angle, is_strike, is_swing, is_whiff, is_in_zone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', values)
            
            conn.commit()
            self.logger.info(f"Inserted {len(pitches_data)} pitches")
            
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting pitches: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def update_pitcher_metrics(self, metrics: Dict[str, Any]):
        """
        投手の成績指標を更新
        
        Args:
            metrics: 成績指標データ
        """
        try:
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
            self.logger.info(f"Updated metrics for pitcher {metrics['pitcher_id']} in season {metrics['season']}")
            
        except sqlite3.Error as e:
            self.logger.error(f"Error updating pitcher metrics: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def update_pitch_usage(self, usage_data: Dict[str, Any]):
        """
        球種使用割合データを更新
        
        Args:
            usage_data: 球種使用割合データ
        """
        try:
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
            self.logger.info(
                f"Updated pitch usage for pitcher {usage_data['pitcher_id']}, "
                f"pitch type {usage_data['pitch_type_id']} in season {usage_data['season']}"
            )
            
        except sqlite3.Error as e:
            self.logger.error(f"Error updating pitch usage: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def get_pitcher_data(self, pitcher_id: int) -> Dict[str, Any]:
        """
        ピッチャーの基本情報を取得
        
        Args:
            pitcher_id: ピッチャーID
            
        Returns:
            Dict: ピッチャー情報
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT * FROM pitchers WHERE id = ?
            ''', (pitcher_id,))
            
            result = cursor.fetchone()
            if not result:
                return {}
                
            return dict(result)
            
        except sqlite3.Error as e:
            self.logger.error(f"Error getting pitcher data: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def get_pitcher_metrics(self, pitcher_id: int, season: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        ピッチャーの成績指標を取得
        
        Args:
            pitcher_id: ピッチャーID
            season: シーズン（指定しない場合は全シーズン）
            
        Returns:
            List[Dict]: 成績指標のリスト
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if season:
                cursor.execute('''
                SELECT * FROM pitcher_metrics 
                WHERE pitcher_id = ? AND season = ?
                ORDER BY season DESC
                ''', (pitcher_id, season))
            else:
                cursor.execute('''
                SELECT * FROM pitcher_metrics 
                WHERE pitcher_id = ?
                ORDER BY season DESC
                ''', (pitcher_id,))
                
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except sqlite3.Error as e:
            self.logger.error(f"Error getting pitcher metrics: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    # src/data_storage/database.py の get_pitch_usage_data メソッドを修正
    def get_pitch_usage_data(self, pitcher_id: int, season: Optional[int] = None, all_seasons: bool = False) -> List[Dict[str, Any]]:
        """
        ピッチャーの球種使用割合を取得
        
        Args:
            pitcher_id: ピッチャーID
            season: シーズン（指定しない場合は最新シーズン、all_seasons=Trueの場合は無視）
            all_seasons: 全シーズンのデータを取得するフラグ
            
        Returns:
            List[Dict]: 球種使用割合データのリスト
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if all_seasons:
                # 全シーズンのデータを取得
                cursor.execute('''
                SELECT pu.*, pt.code, pt.name
                FROM pitch_usage pu
                JOIN pitch_types pt ON pu.pitch_type_id = pt.id
                WHERE pu.pitcher_id = ?
                ORDER BY pu.season DESC, pu.usage_pct DESC
                ''', (pitcher_id,))
            elif season:
                # 特定シーズンのデータを取得
                cursor.execute('''
                SELECT pu.*, pt.code, pt.name
                FROM pitch_usage pu
                JOIN pitch_types pt ON pu.pitch_type_id = pt.id
                WHERE pu.pitcher_id = ? AND pu.season = ?
                ORDER BY pu.usage_pct DESC
                ''', (pitcher_id, season))
            else:
                # 最新シーズンのデータのみ取得
                cursor.execute('''
                SELECT pu.*, pt.code, pt.name
                FROM pitch_usage pu
                JOIN pitch_types pt ON pu.pitch_type_id = pt.id
                WHERE pu.pitcher_id = ? AND pu.season = (
                    SELECT MAX(season) FROM pitch_usage WHERE pitcher_id = ?
                )
                ORDER BY pu.usage_pct DESC
                ''', (pitcher_id, pitcher_id))
                
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except sqlite3.Error as e:
            self.logger.error(f"Error getting pitch usage data: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()                
    def get_pitchers_by_team(self, team: str) -> List[Dict[str, Any]]:
        """
        チームに所属する全ピッチャーを取得
        
        Args:
            team: チーム略称
            
        Returns:
            List[Dict]: ピッチャー情報のリスト
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT * FROM pitchers WHERE team = ?
            ORDER BY name
            ''', (team,))
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except sqlite3.Error as e:
            self.logger.error(f"Error getting pitchers by team: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def search_pitchers(self, search_term: str) -> List[Dict[str, Any]]:
        """
        ピッチャー名で検索
        
        Args:
            search_term: 検索キーワード
            
        Returns:
            List[Dict]: ピッチャー情報のリスト
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT * FROM pitchers 
            WHERE name LIKE ?
            ORDER BY name
            ''', (f'%{search_term}%',))
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except sqlite3.Error as e:
            self.logger.error(f"Error searching pitchers: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                
    def get_all_teams(self) -> List[str]:
        """
        データベースに存在する全チームを取得
        
        Returns:
            List[str]: チーム略称のリスト
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT DISTINCT team FROM pitchers
            WHERE team IS NOT NULL
            ORDER BY team
            ''')
            
            results = cursor.fetchall()
            return [row['team'] for row in results]
            
        except sqlite3.Error as e:
            self.logger.error(f"Error getting all teams: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
    
    def get_all_pitchers(self) -> List[Dict[str, Any]]:
        """
        全ピッチャーのリストを取得
        
        Returns:
            List[Dict]: ピッチャー情報のリスト
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT * FROM pitchers
            ORDER BY name
            ''')
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except sqlite3.Error as e:
            self.logger.error(f"Error getting all pitchers: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()