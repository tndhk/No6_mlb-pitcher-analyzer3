# src/data_analysis/statistical_calculator.py
import logging
import math
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
import numpy as np

class StatisticalCalculator:
    """
    投手の統計指標を計算するクラス
    """
    
    def __init__(self, logging_level=logging.INFO):
        """
        StatisticalCalculatorの初期化
        
        Args:
            logging_level: ロギングレベル
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)
        
    def calculate_era(self, earned_runs: float, innings_pitched: float) -> float:
        """
        ERA (Earned Run Average) を計算
        
        Args:
            earned_runs: 自責点
            innings_pitched: 投球イニング
            
        Returns:
            float: ERA値
        """
        if innings_pitched == 0:
            return 0.0
            
        return (earned_runs * 9) / innings_pitched
        
    def calculate_fip(self, hr: int, bb: int, hbp: int, k: int, innings_pitched: float, 
                     league_constant: float = 3.10) -> float:
        """
        FIP (Fielding Independent Pitching) を計算
        
        Args:
            hr: ホームラン数
            bb: 四球数
            hbp: 死球数
            k: 奪三振数
            innings_pitched: 投球イニング
            league_constant: リーグ定数（デフォルト：3.10）
            
        Returns:
            float: FIP値
        """
        if innings_pitched == 0:
            return 0.0
            
        return ((13 * hr) + (3 * (bb + hbp)) - (2 * k)) / innings_pitched + league_constant
        
    def calculate_whip(self, hits: int, walks: int, innings_pitched: float) -> float:
        """
        WHIP (Walks plus Hits per Inning Pitched) を計算
        
        Args:
            hits: 被安打数
            walks: 四球数
            innings_pitched: 投球イニング
            
        Returns:
            float: WHIP値
        """
        if innings_pitched == 0:
            return 0.0
            
        return (hits + walks) / innings_pitched
        
    def calculate_k_per_9(self, strikeouts: int, innings_pitched: float) -> float:
        """
        K/9 (Strikeouts per 9 innings) を計算
        
        Args:
            strikeouts: 奪三振数
            innings_pitched: 投球イニング
            
        Returns:
            float: K/9値
        """
        if innings_pitched == 0:
            return 0.0
            
        return (strikeouts * 9) / innings_pitched
        
    def calculate_bb_per_9(self, walks: int, innings_pitched: float) -> float:
        """
        BB/9 (Walks per 9 innings) を計算
        
        Args:
            walks: 四球数
            innings_pitched: 投球イニング
            
        Returns:
            float: BB/9値
        """
        if innings_pitched == 0:
            return 0.0
            
        return (walks * 9) / innings_pitched
        
    def calculate_hr_per_9(self, home_runs: int, innings_pitched: float) -> float:
        """
        HR/9 (Home Runs per 9 innings) を計算
        
        Args:
            home_runs: ホームラン数
            innings_pitched: 投球イニング
            
        Returns:
            float: HR/9値
        """
        if innings_pitched == 0:
            return 0.0
            
        return (home_runs * 9) / innings_pitched
        
    def calculate_swing_metrics(self, pitch_data: pd.DataFrame) -> Dict[str, float]:
        """
        スイング関連指標を計算
        
        Args:
            pitch_data: 投球データ
            
        Returns:
            Dict: スイング関連指標（SwStr%, CSW%, O-Swing%, Z-Contact%）
        """
        if pitch_data.empty:
            return {
                'swstr_pct': 0.0,
                'csw_pct': 0.0,
                'o_swing_pct': 0.0,
                'z_contact_pct': 0.0
            }
            
        # 必要なフラグを確認
        if not all(col in pitch_data.columns for col in ['is_swing', 'is_strike', 'is_whiff', 'is_in_zone']):
            self.logger.warning("Required columns missing for swing metrics calculation")
            return {
                'swstr_pct': None,
                'csw_pct': None,
                'o_swing_pct': None,
                'z_contact_pct': None
            }
            
        # 投球総数
        total_pitches = len(pitch_data)
        
        # SwStr% (Swinging Strike Percentage) - 空振り率
        swinging_strikes = pitch_data['is_whiff'].sum()
        swstr_pct = (swinging_strikes / total_pitches) * 100 if total_pitches > 0 else 0
        
        # CSW% (Called Strikes + Whiffs Percentage) - 見逃し三振 + 空振り率
        called_strikes = (pitch_data['is_strike'] & ~pitch_data['is_swing']).sum()
        csw = called_strikes + swinging_strikes
        csw_pct = (csw / total_pitches) * 100 if total_pitches > 0 else 0
        
        # O-Swing% (Outside Zone Swing Percentage) - ゾーン外スイング率
        pitches_outside_zone = (~pitch_data['is_in_zone']).sum()
        swings_outside_zone = (pitch_data['is_swing'] & ~pitch_data['is_in_zone']).sum()
        o_swing_pct = (swings_outside_zone / pitches_outside_zone) * 100 if pitches_outside_zone > 0 else 0
        
        # Z-Contact% (Zone Contact Percentage) - ゾーン内コンタクト率
        swings_in_zone = (pitch_data['is_swing'] & pitch_data['is_in_zone']).sum()
        contact_in_zone = swings_in_zone - (pitch_data['is_whiff'] & pitch_data['is_in_zone']).sum()
        z_contact_pct = (contact_in_zone / swings_in_zone) * 100 if swings_in_zone > 0 else 0
        
        return {
            'swstr_pct': swstr_pct,
            'csw_pct': csw_pct,
            'o_swing_pct': o_swing_pct,
            'z_contact_pct': z_contact_pct
        }
        
    def calculate_pitch_metrics(self, pitch_data: pd.DataFrame, pitch_type: str = None) -> Dict[str, Any]:
        """
        球種ごとの指標を計算
        
        Args:
            pitch_data: 投球データ
            pitch_type: 球種（指定しない場合は全球種）
            
        Returns:
            Dict: 球種ごとの指標
        """
        if pitch_data.empty:
            return {}
            
        # 球種でフィルタリング
        if pitch_type and 'pitch_type' in pitch_data.columns:
            filtered_data = pitch_data[pitch_data['pitch_type'] == pitch_type]
        else:
            filtered_data = pitch_data
            
        if filtered_data.empty:
            return {}
            
        metrics = {}
        
        # 球速
        if 'release_speed' in filtered_data.columns:
            metrics['avg_velocity'] = filtered_data['release_speed'].mean()
            metrics['max_velocity'] = filtered_data['release_speed'].max()
            metrics['min_velocity'] = filtered_data['release_speed'].min()
            metrics['std_velocity'] = filtered_data['release_speed'].std()
            
        # 回転数
        if 'release_spin_rate' in filtered_data.columns:
            metrics['avg_spin_rate'] = filtered_data['release_spin_rate'].mean()
            metrics['max_spin_rate'] = filtered_data['release_spin_rate'].max()
            metrics['min_spin_rate'] = filtered_data['release_spin_rate'].min()
            
        # 変化量
        if 'pfx_x' in filtered_data.columns and 'pfx_z' in filtered_data.columns:
            metrics['avg_pfx_x'] = filtered_data['pfx_x'].mean()
            metrics['avg_pfx_z'] = filtered_data['pfx_z'].mean()
            
            # 合計変化量（横と縦の変化量のベクトル和）
            pfx_total = np.sqrt(filtered_data['pfx_x']**2 + filtered_data['pfx_z']**2)
            metrics['avg_total_break'] = pfx_total.mean()
            metrics['max_total_break'] = pfx_total.max()
            
        # 使用割合
        metrics['usage_count'] = len(filtered_data)
        metrics['usage_pct'] = (len(filtered_data) / len(pitch_data)) * 100 if len(pitch_data) > 0 else 0
        
        # スイング指標
        swing_metrics = self.calculate_swing_metrics(filtered_data)
        metrics.update(swing_metrics)
        
        return metrics