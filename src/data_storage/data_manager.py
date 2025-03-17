# src/data_storage/data_manager.py
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd

from src.data_storage.database import Database

class DataManager:
    """
    データの処理と管理を行うクラス
    """
    
    def __init__(self, database: Database, logging_level=logging.INFO):
        """
        DataManagerの初期化
        
        Args:
            database: Databaseインスタンス
            logging_level: ロギングレベル
        """
        self.db = database
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)
        
        # 球種の初期データをロード
        self._initialize_pitch_types()
        
    def _initialize_pitch_types(self):
        """
        球種の初期データをデータベースに挿入
        """
        pitch_types = [
            {'code': 'FF', 'name': 'Four-Seam Fastball', 'description': 'Standard four-seam fastball'},
            {'code': 'FT', 'name': 'Two-Seam Fastball', 'description': 'Two-seam fastball with horizontal movement'},
            {'code': 'FC', 'name': 'Cutter', 'description': 'Fastball with late cutting action'},
            {'code': 'SI', 'name': 'Sinker', 'description': 'Fastball with downward movement'},
            {'code': 'SL', 'name': 'Slider', 'description': 'Breaking ball with lateral and downward movement'},
            {'code': 'CU', 'name': 'Curveball', 'description': 'Breaking ball with significant downward movement'},
            {'code': 'CH', 'name': 'Changeup', 'description': 'Off-speed pitch that mimics fastball arm action'},
            {'code': 'KN', 'name': 'Knuckleball', 'description': 'Pitch with minimal spin and unpredictable movement'},
            {'code': 'SP', 'name': 'Splitter', 'description': 'Fastball variant with sharp downward movement'},
            {'code': 'FS', 'name': 'Split-Finger', 'description': 'Similar to splitter with more velocity'},
            {'code': 'ST', 'name': 'Sweeper', 'description': 'Combination slider/curveball with horizontal break'},
            {'code': 'SV', 'name': 'Slurve', 'description': 'Combination slider/curveball'},
            {'code': 'KC', 'name': 'Knuckle-Curve', 'description': 'Curveball with knuckleball grip'},
            {'code': 'EP', 'name': 'Eephus', 'description': 'Very slow, high-arcing lob pitch'},
            {'code': 'SC', 'name': 'Screwball', 'description': 'Breaking ball with movement opposite to curveball'},
        ]
        
        self.db.insert_pitch_types(pitch_types)
        self.logger.info("Initialized pitch types in database")
        
    def process_statcast_data(self, pitcher_id: int, mlb_id: int, name: str, 
                             data: pd.DataFrame, team: Optional[str] = None):
        """
        Statcastから取得したデータを処理しデータベースに保存
        
        Args:
            pitcher_id: データベース内のピッチャーID
            mlb_id: MLB選手ID
            name: ピッチャー名
            data: Statcastデータ
            team: チーム略称（オプション）
        """
        if data.empty:
            self.logger.warning(f"Empty data for pitcher {name} (ID: {mlb_id})")
            return
            
        try:
            # 1. ピッチャー情報の更新/挿入
            db_pitcher_id = self.db.get_pitcher_id(mlb_id)
            if not db_pitcher_id:
                db_pitcher_id = self.db.insert_pitcher(mlb_id, name, team)
                
            # 2. 投球データの処理
            pitches_to_insert = []
            
            for _, row in data.iterrows():
                # 球種IDの取得
                pitch_type = row.get('pitch_type')
                pitch_type_id = None
                if pitch_type:
                    pitch_type_id = self.db.get_pitch_type_id(pitch_type)
                
                # 試合情報の処理
                game_date = row.get('game_date')
                game_id = None
                
                if game_date:
                    # 日付形式の変換
                    if isinstance(game_date, pd.Timestamp):
                        game_date_str = game_date.strftime('%Y-%m-%d')
                    else:
                        game_date_str = str(game_date)
                        
                    # 仮の試合情報を挿入（チーム情報が不完全な場合）
                    season = int(game_date_str[:4])  # 年を取得
                    home_team = row.get('home_team', 'UNKNOWN')
                    away_team = row.get('away_team', 'UNKNOWN')
                    
                    game_id = self.db.insert_game(game_date_str, home_team, away_team, season)
                
                # 投球情報の詳細
                description = row.get('description')
                
                # ストライク、スイング、空振り判定
                is_strike = False
                is_swing = False
                is_whiff = False
                is_in_zone = False
                
                if description:
                    is_strike = 'strike' in description.lower() or description.lower() in ['swinging_strike', 'called_strike', 'foul', 'foul_tip']
                    is_swing = description.lower() in ['swinging_strike', 'foul', 'foul_tip', 'hit_into_play']
                    is_whiff = description.lower() == 'swinging_strike'
                
                # ゾーン判定
                zone = row.get('zone')
                if zone and 1 <= zone <= 9:
                    is_in_zone = True
                
                # 投球データ辞書の作成
                pitch_data = {
                    'pitcher_id': db_pitcher_id,
                    'game_id': game_id,
                    'pitch_type_id': pitch_type_id,
                    'release_speed': row.get('release_speed'),
                    'release_spin_rate': row.get('release_spin_rate'),
                    'pfx_x': row.get('pfx_x'),
                    'pfx_z': row.get('pfx_z'),
                    'plate_x': row.get('plate_x'),
                    'plate_z': row.get('plate_z'),
                    'description': description,
                    'zone': zone,
                    'type': row.get('type'),
                    'launch_speed': row.get('launch_speed'),
                    'launch_angle': row.get('launch_angle'),
                    'is_strike': is_strike,
                    'is_swing': is_swing,
                    'is_whiff': is_whiff,
                    'is_in_zone': is_in_zone
                }
                
                pitches_to_insert.append(pitch_data)
            
            # 3. バッチ処理でデータベースに挿入
            if pitches_to_insert:
                self.db.insert_pitches(pitches_to_insert)
                
            # 4. 集計データの計算と保存
            self._calculate_and_save_metrics(db_pitcher_id, data)
            
            self.logger.info(f"Successfully processed data for pitcher {name} (ID: {mlb_id})")
            
        except Exception as e:
            self.logger.error(f"Error processing data for pitcher {name} (ID: {mlb_id}): {str(e)}")
            raise
            
    def _calculate_and_save_metrics(self, pitcher_id: int, data: pd.DataFrame):
        """
        投球データから各種指標を計算してデータベースに保存
        
        Args:
            pitcher_id: ピッチャーID
            data: 投球データ
        """
        if data.empty:
            return
            
        try:
            # 年ごとにデータを分割
            if 'game_date' in data.columns:
                # 日付カラムの形式を確認
                if not pd.api.types.is_datetime64_dtype(data['game_date']):
                    data['game_date'] = pd.to_datetime(data['game_date'])
                    
                # 年ごとにグループ化
                data['season'] = data['game_date'].dt.year
                seasons = data['season'].unique()
                
                for season in seasons:
                    season_data = data[data['season'] == season]
                    
                    # 1. 球種ごとの使用割合と指標の計算
                    self._calculate_pitch_usage(pitcher_id, season_data, int(season))
                    
                    # 2. 総合成績指標の計算
                    self._calculate_pitcher_metrics(pitcher_id, season_data, int(season))
            
        except Exception as e:
            self.logger.error(f"Error calculating metrics for pitcher {pitcher_id}: {str(e)}")
            raise
            
    def _calculate_pitch_usage(self, pitcher_id: int, data: pd.DataFrame, season: int):
        """
        球種ごとの使用割合と関連指標を計算
        
        Args:
            pitcher_id: ピッチャーID
            data: シーズンごとの投球データ
            season: シーズン年
        """
        if 'pitch_type' not in data.columns or data['pitch_type'].isna().all():
            self.logger.warning(f"No pitch type data available for pitcher {pitcher_id} in season {season}")
            return
            
        # 球種ごとにグループ化
        pitch_groups = data.groupby('pitch_type')
        total_pitches = len(data)
        
        for pitch_type, group in pitch_groups:
            pitch_type_id = self.db.get_pitch_type_id(pitch_type)
            if not pitch_type_id:
                self.logger.warning(f"Unknown pitch type: {pitch_type}")
                continue
                
            # 球種ごとの投球数
            pitch_count = len(group)
            usage_pct = (pitch_count / total_pitches) * 100
            
            # 平均球速
            avg_velocity = group['release_speed'].mean() if 'release_speed' in group.columns else None
            
            # 平均回転数
            avg_spin_rate = group['release_spin_rate'].mean() if 'release_spin_rate' in group.columns else None
            
            # 平均変化量
            avg_pfx_x = group['pfx_x'].mean() if 'pfx_x' in group.columns else None
            avg_pfx_z = group['pfx_z'].mean() if 'pfx_z' in group.columns else None
            
            # 空振り率の計算
            swings = 0
            whiffs = 0
            
            if 'description' in group.columns:
                swings = group['description'].str.contains('swinging|foul|hit', case=False, regex=True).sum()
                whiffs = group['description'].str.contains('swinging_strike', case=False, regex=True).sum()
            
            whiff_pct = (whiffs / swings) * 100 if swings > 0 else 0
            
            # データベースに保存
            usage_data = {
                'pitcher_id': pitcher_id,
                'pitch_type_id': pitch_type_id,
                'season': season,
                'usage_pct': usage_pct,
                'avg_velocity': avg_velocity,
                'avg_spin_rate': avg_spin_rate,
                'avg_pfx_x': avg_pfx_x,
                'avg_pfx_z': avg_pfx_z,
                'whiff_pct': whiff_pct
            }
            
            self.db.update_pitch_usage(usage_data)
            
    def _calculate_pitcher_metrics(self, pitcher_id: int, data: pd.DataFrame, season: int):
        """
        投手の総合成績指標を計算
        
        Args:
            pitcher_id: ピッチャーID
            data: シーズンごとの投球データ
            season: シーズン年
        """
        # 注: 完全な成績指標の計算には追加データが必要かもしれない
        # 基本的なサロゲート計算を行う
        
        # 投球数
        total_pitches = len(data)
        
        # 空振り率（SwStr%）の計算
        swings = 0
        whiffs = 0
        pitches_in_zone = 0
        swings_out_of_zone = 0
        contact_in_zone = 0
        
        if 'description' in data.columns:
            swings = data['description'].str.contains('swinging|foul|hit', case=False, regex=True).sum()
            whiffs = data['description'].str.contains('swinging_strike', case=False, regex=True).sum()
        
        swstr_pct = (whiffs / total_pitches) * 100 if total_pitches > 0 else 0
        
        # ゾーン内外の判定
        if 'zone' in data.columns:
            zone_mask = data['zone'].between(1, 9)
            pitches_in_zone = zone_mask.sum()
            
            # ゾーン外スイング（O-Swing%）
            if 'description' in data.columns:
                swing_mask = data['description'].str.contains('swinging|foul|hit', case=False, regex=True)
                swings_out_of_zone = ((~zone_mask) & swing_mask).sum()
                pitches_out_of_zone = (~zone_mask).sum()
                
                # ゾーン内コンタクト（Z-Contact%）
                contact_mask = data['description'].str.contains('foul|hit', case=False, regex=True)
                contact_in_zone = (zone_mask & contact_mask).sum()
                swings_in_zone = (zone_mask & swing_mask).sum()
        
        o_swing_pct = (swings_out_of_zone / pitches_out_of_zone) * 100 if pitches_out_of_zone > 0 else 0
        z_contact_pct = (contact_in_zone / swings_in_zone) * 100 if swings_in_zone > 0 else 0
        
        # CSW% (Called Strikes + Whiffs)
        called_strikes = 0
        if 'description' in data.columns:
            called_strikes = data['description'].str.contains('called_strike', case=False, regex=True).sum()
        
        csw = called_strikes + whiffs
        csw_pct = (csw / total_pitches) * 100 if total_pitches > 0 else 0
        
        # 注: ERA, FIP, WHIP, K/9などはより詳細なデータが必要
        # ここでは仮の値を設定
        
        metrics = {
            'pitcher_id': pitcher_id,
            'season': season,
            'era': None,  # APIから追加データが必要
            'fip': None,  # APIから追加データが必要
            'whip': None,  # APIから追加データが必要
            'k_per_9': None,  # APIから追加データが必要
            'bb_per_9': None,  # APIから追加データが必要
            'hr_per_9': None,  # APIから追加データが必要
            'swstr_pct': swstr_pct,
            'csw_pct': csw_pct,
            'o_swing_pct': o_swing_pct,
            'z_contact_pct': z_contact_pct,
            'innings_pitched': None,  # APIから追加データが必要
            'games': len(data['game_date'].unique()) if 'game_date' in data.columns else None,
            'strikeouts': None,  # APIから追加データが必要
            'walks': None,  # APIから追加データが必要
            'home_runs': None,  # APIから追加データが必要
            'hits': None,  # APIから追加データが必要
            'earned_runs': None  # APIから追加データが必要
        }
        
        self.db.update_pitcher_metrics(metrics)