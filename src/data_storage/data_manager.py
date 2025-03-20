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
                # 省略...（既存コード）
                pass
            
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
                
                # シーズン情報をログに出力（デバッグ用）
                self.logger.info(f"Found {len(seasons)} seasons for pitcher {pitcher_id}: {', '.join(map(str, seasons))}")
                
                for season in seasons:
                    season_data = data[data['season'] == season]
                    
                    # データ量をログに出力（デバッグ用）
                    self.logger.info(f"Processing {len(season_data)} pitches for pitcher {pitcher_id} in season {season}")
                    
                    # 1. 球種ごとの使用割合と指標の計算
                    self._calculate_pitch_usage(pitcher_id, season_data, int(season))
                    
                    # 2. 総合成績指標の計算
                    self._calculate_pitcher_metrics(pitcher_id, season_data, int(season))
                
            else:
                self.logger.warning(f"No game_date column found in data for pitcher {pitcher_id}")
                
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
        
        if total_pitches == 0:
            self.logger.warning(f"No pitches found for pitcher {pitcher_id} in season {season}")
            return
        
        self.logger.info(f"Calculating pitch usage for pitcher {pitcher_id} in season {season}: {total_pitches} total pitches")
        
        for pitch_type, group in pitch_groups:
            pitch_type_id = self.db.get_pitch_type_id(pitch_type)
            if not pitch_type_id:
                self.logger.warning(f"Unknown pitch type: {pitch_type}")
                continue
                
            # 球種ごとの投球数
            pitch_count = len(group)
            usage_pct = (pitch_count / total_pitches) * 100
            
            self.logger.info(f"  Pitch type {pitch_type}: {pitch_count} pitches ({usage_pct:.1f}%)")
            
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
        from src.data_acquisition.statcast_client import StatcastClient
        
        if data.empty:
            return
            
        try:
            # 投手の基本情報を取得
            pitcher_info = self.db.get_pitcher_data(pitcher_id)
            if not pitcher_info or 'mlb_id' not in pitcher_info:
                self.logger.warning(f"No MLB ID found for pitcher {pitcher_id}")
                return
                
            mlb_id = pitcher_info['mlb_id']
            
            # StatcastClientを使用して投手の詳細統計を取得
            client = StatcastClient()
            pitcher_stats = client.get_pitcher_stats(mlb_id, season)
            
            if pitcher_stats:
                # Baseball Referenceから取得した統計を使用
                self.logger.info(f"Using Baseball Reference stats for pitcher {pitcher_id} in season {season}")
                metrics = {
                    'pitcher_id': pitcher_id,
                    'season': season,
                    'era': pitcher_stats.get('era'),
                    'fip': pitcher_stats.get('fip'),
                    'whip': pitcher_stats.get('whip'),
                    'k_per_9': pitcher_stats.get('k_per_9'),
                    'bb_per_9': pitcher_stats.get('bb_per_9'),
                    'hr_per_9': pitcher_stats.get('hr_per_9'),
                    'innings_pitched': pitcher_stats.get('innings_pitched'),
                    'games': pitcher_stats.get('games'),
                    'strikeouts': pitcher_stats.get('strikeouts'),
                    'walks': pitcher_stats.get('walks'),
                    'home_runs': pitcher_stats.get('home_runs'),
                    'hits': pitcher_stats.get('hits'),
                    'earned_runs': pitcher_stats.get('earned_runs')
                }
                
                # Statcastデータからスイング指標を計算
                swing_metrics = self._calculate_swing_metrics_from_data(data)
                
                # スイング指標を追加
                metrics.update({
                    'swstr_pct': swing_metrics.get('swstr_pct'),
                    'csw_pct': swing_metrics.get('csw_pct'),
                    'o_swing_pct': swing_metrics.get('o_swing_pct'),
                    'z_contact_pct': swing_metrics.get('z_contact_pct')
                })
                
                # データベースに保存
                self.db.update_pitcher_metrics(metrics)
                
            else:
                # Baseball Referenceからデータを取得できなかった場合はStatcastデータから概算
                self.logger.info(f"Using Statcast data to estimate metrics for pitcher {pitcher_id} in season {season}")
                self._estimate_metrics_from_statcast(pitcher_id, data, season)
        
        except Exception as e:
            self.logger.error(f"Error calculating metrics for pitcher {pitcher_id}: {str(e)}")
            raise

    def _calculate_swing_metrics_from_data(self, data: pd.DataFrame) -> Dict[str, float]:
        """
        投球データからスイング関連指標を計算
        
        Args:
            data: 投球データ
            
        Returns:
            Dict: スイング指標の辞書
        """
        if data.empty:
            return {
                'swstr_pct': None,
                'csw_pct': None,
                'o_swing_pct': None,
                'z_contact_pct': None
            }
        
        # 必要なフラグを確認
        required_columns = ['is_swing', 'is_strike', 'is_whiff', 'is_in_zone']
        if not all(col in data.columns for col in required_columns):
            self.logger.warning("Required columns missing for swing metrics calculation")
            return {
                'swstr_pct': None,
                'csw_pct': None,
                'o_swing_pct': None,
                'z_contact_pct': None
            }
        
        # 投球総数
        total_pitches = len(data)
        
        # SwStr% (Swinging Strike Percentage) - 空振り率
        swinging_strikes = data['is_whiff'].sum()
        swstr_pct = (swinging_strikes / total_pitches) * 100 if total_pitches > 0 else 0
        
        # CSW% (Called Strikes + Whiffs Percentage) - 見逃し三振 + 空振り率
        called_strikes = (data['is_strike'] & ~data['is_swing']).sum()
        csw = called_strikes + swinging_strikes
        csw_pct = (csw / total_pitches) * 100 if total_pitches > 0 else 0
        
        # O-Swing% (Outside Zone Swing Percentage) - ゾーン外スイング率
        pitches_outside_zone = (~data['is_in_zone']).sum()
        swings_outside_zone = (data['is_swing'] & ~data['is_in_zone']).sum()
        o_swing_pct = (swings_outside_zone / pitches_outside_zone) * 100 if pitches_outside_zone > 0 else 0
        
        # Z-Contact% (Zone Contact Percentage) - ゾーン内コンタクト率
        swings_in_zone = (data['is_swing'] & data['is_in_zone']).sum()
        contact_in_zone = swings_in_zone - (data['is_whiff'] & data['is_in_zone']).sum()
        z_contact_pct = (contact_in_zone / swings_in_zone) * 100 if swings_in_zone > 0 else 0
        
        return {
            'swstr_pct': swstr_pct,
            'csw_pct': csw_pct,
            'o_swing_pct': o_swing_pct,
            'z_contact_pct': z_contact_pct
        }

    def _estimate_metrics_from_statcast(self, pitcher_id: int, data: pd.DataFrame, season: int):
        """
        Statcastデータから投手指標を概算
        
        Args:
            pitcher_id: ピッチャーID
            data: 投球データ
            season: シーズン年
        """
        from src.data_analysis.statistical_calculator import StatisticalCalculator
        
        # 統計計算機のインスタンス化
        calculator = StatisticalCalculator()
        
        # スイング指標の計算
        swing_metrics = self._calculate_swing_metrics_from_data(data)
        
        # 投球イニング、奪三振、四球、本塁打、安打、自責点を概算
        innings_pitched = None
        strikeouts = None
        walks = None
        home_runs = None
        hits = None
        earned_runs = None
        
        # 投球データから奪三振、四球、本塁打の数を概算
        if 'description' in data.columns:
            # 奪三振の概算（空振り三振、見逃し三振などを含む）
            strikeouts = data['description'].str.contains('strikeout|swinging_strike|called_strike_three', case=False, regex=True).sum()
            
            # 四球の概算
            walks = data['description'].str.contains('ball_four|walk', case=False, regex=True).sum()
            
            # 本塁打の概算
            home_runs = data['description'].str.contains('home_run', case=False, regex=True).sum()
            
            # 安打の概算
            hits = data['description'].str.contains('hit_into_play|single|double|triple|home_run', case=False, regex=True).sum()
        
        # 打席数からイニング数を概算（27アウトで9イニング）
        if 'description' in data.columns:
            # アウト数の概算
            outs = data['description'].str.contains('out|strikeout|ground_out|fly_out', case=False, regex=True).sum()
            
            # イニング数の概算
            innings_pitched = outs / 3 if outs > 0 else None
        
        # 自責点の概算（かなり大雑把な推定）
        if hits is not None and walks is not None and home_runs is not None:
            # 非常に大雑把な推定。実際のERAは別のAPIから取得するべき
            earned_runs = home_runs + (hits + walks - home_runs) * 0.25
        
        # K/9, BB/9, HR/9を計算
        k_per_9 = None
        bb_per_9 = None
        hr_per_9 = None
        
        if innings_pitched is not None and innings_pitched > 0:
            if strikeouts is not None:
                k_per_9 = calculator.calculate_k_per_9(strikeouts, innings_pitched)
            
            if walks is not None:
                bb_per_9 = calculator.calculate_bb_per_9(walks, innings_pitched)
            
            if home_runs is not None:
                hr_per_9 = calculator.calculate_hr_per_9(home_runs, innings_pitched)
        
        # ERA, FIP, WHIPの計算
        era = None
        fip = None
        whip = None
        
        if innings_pitched is not None and innings_pitched > 0:
            # ERA
            if earned_runs is not None:
                era = calculator.calculate_era(earned_runs, innings_pitched)
            
            # FIP
            if strikeouts is not None and walks is not None and home_runs is not None:
                fip = calculator.calculate_fip(home_runs, walks, 0, strikeouts, innings_pitched, 3.10)
            
            # WHIP
            if hits is not None and walks is not None:
                whip = calculator.calculate_whip(hits, walks, innings_pitched)
        
        # メトリクスの登録
        metrics = {
            'pitcher_id': pitcher_id,
            'season': season,
            'era': era,
            'fip': fip,
            'whip': whip,
            'k_per_9': k_per_9,
            'bb_per_9': bb_per_9,
            'hr_per_9': hr_per_9,
            'swstr_pct': swing_metrics.get('swstr_pct'),
            'csw_pct': swing_metrics.get('csw_pct'),
            'o_swing_pct': swing_metrics.get('o_swing_pct'),
            'z_contact_pct': swing_metrics.get('z_contact_pct'),
            'innings_pitched': innings_pitched,
            'games': len(data['game_date'].unique()) if 'game_date' in data.columns else None,
            'strikeouts': strikeouts,
            'walks': walks,
            'home_runs': home_runs,
            'hits': hits,
            'earned_runs': earned_runs
        }
        
        self.db.update_pitcher_metrics(metrics)