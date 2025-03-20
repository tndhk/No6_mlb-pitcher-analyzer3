# src/data_analysis/pitcher_analyzer.py
import logging
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
import numpy as np
import logging  # これを追加


from src.data_storage.database import Database
from src.data_analysis.statistical_calculator import StatisticalCalculator
from src.data_analysis.time_series_analyzer import TimeSeriesAnalyzer

class PitcherAnalyzer:
    """
    投手データを総合的に分析するクラス
    """
    
    def __init__(self, database: Database, logging_level=logging.INFO):
        """
        PitcherAnalyzerの初期化
        
        Args:
            database: Databaseインスタンス
            logging_level: ロギングレベル
        """
        self.db = database
        self.stat_calculator = StatisticalCalculator(logging_level)
        self.ts_analyzer = TimeSeriesAnalyzer(logging_level)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)
        
    # src/data_analysis/pitcher_analyzer.py の get_pitcher_summary メソッドにデバッグログを追加

    def get_pitcher_summary(self, pitcher_id: int, season: Optional[int] = None) -> Dict[str, Any]:
        """
        投手の総合サマリーを取得
        
        Args:
            pitcher_id: ピッチャーID
            season: シーズン（指定しない場合は最新シーズン）
            
        Returns:
            Dict: 投手サマリー情報
        """
        # デバッグログを追加
        self.logger.info(f"Getting pitcher summary for pitcher_id={pitcher_id}, season={season}")
        
        # 基本情報の取得
        pitcher_info = self.db.get_pitcher_data(pitcher_id)
        if not pitcher_info:
            self.logger.warning(f"Pitcher with ID {pitcher_id} not found")
            return {}
            
        # 成績指標の取得
        metrics = self.db.get_pitcher_metrics(pitcher_id, season)
        metrics_data = metrics[0] if metrics else {}
        
        # 球種使用割合の取得
        pitch_usage = self.db.get_pitch_usage_data(pitcher_id, season)
        
        # デバッグログ追加
        self.logger.info(f"Retrieved {len(metrics)} metrics records and {len(pitch_usage)} pitch usage records")
        if pitch_usage:
            pitch_seasons = set(p.get('season') for p in pitch_usage if 'season' in p)
            self.logger.info(f"Pitch usage seasons: {pitch_seasons}")
        
        # 球種データの整形
        pitch_types = []
        for pitch in pitch_usage:
            pitch_types.append({
                'type': pitch['name'],
                'code': pitch['code'],
                'usage_pct': pitch['usage_pct'],
                'avg_velocity': pitch['avg_velocity'],
                'avg_spin_rate': pitch['avg_spin_rate'],
                'avg_pfx_x': pitch['avg_pfx_x'],
                'avg_pfx_z': pitch['avg_pfx_z'],
                'whiff_pct': pitch['whiff_pct']
            })
            
        # 投手サマリーの構築
        summary = {
            'pitcher_id': pitcher_id,
            'name': pitcher_info.get('name', ''),
            'team': pitcher_info.get('team', ''),
            'season': season or metrics_data.get('season'),
            'metrics': {
                'era': metrics_data.get('era'),
                'fip': metrics_data.get('fip'),
                'whip': metrics_data.get('whip'),
                'k_per_9': metrics_data.get('k_per_9'),
                'bb_per_9': metrics_data.get('bb_per_9'),
                'hr_per_9': metrics_data.get('hr_per_9'),
                'swstr_pct': metrics_data.get('swstr_pct'),
                'csw_pct': metrics_data.get('csw_pct'),
                'o_swing_pct': metrics_data.get('o_swing_pct'),
                'z_contact_pct': metrics_data.get('z_contact_pct'),
                'innings_pitched': metrics_data.get('innings_pitched'),
                'games': metrics_data.get('games'),
                'strikeouts': metrics_data.get('strikeouts'),
                'walks': metrics_data.get('walks'),
                'home_runs': metrics_data.get('home_runs')
            },
            'pitch_types': pitch_types
        }
        
        return summary    
    def get_pitch_type_details(self, pitcher_id: int, pitch_type_id: int, 
                             season: Optional[int] = None) -> Dict[str, Any]:
        """
        特定の球種の詳細情報を取得
        
        Args:
            pitcher_id: ピッチャーID
            pitch_type_id: 球種ID
            season: シーズン（指定しない場合は全シーズン）
            
        Returns:
            Dict: 球種詳細情報
        """
        # ここでは実際のデータの取得は省略し、概念的な実装を示す
        # 実際の実装では、データベースから球種ごとの詳細データを取得し、分析する必要がある
        
        # 仮の球種詳細データ
        pitch_details = {
            'pitcher_id': pitcher_id,
            'pitch_type_id': pitch_type_id,
            'season': season,
            # 各種指標は実際のデータベースから取得する
            'metrics': {}
        }
        
        return pitch_details
        
    def compare_seasons(self, pitcher_id: int, season1: int, season2: int) -> Dict[str, Any]:
        """
        2つのシーズンのパフォーマンスを比較
        
        Args:
            pitcher_id: ピッチャーID
            season1: 比較シーズン1
            season2: 比較シーズン2
            
        Returns:
            Dict: シーズン比較結果
        """
        # シーズン1のデータ
        metrics1 = self.db.get_pitcher_metrics(pitcher_id, season1)
        metrics1_data = metrics1[0] if metrics1 else {}
        
        # シーズン2のデータ
        metrics2 = self.db.get_pitcher_metrics(pitcher_id, season2)
        metrics2_data = metrics2[0] if metrics2 else {}
        
        if not metrics1_data or not metrics2_data:
            self.logger.warning(f"Data missing for season comparison")
            return {}
            
        # 球種使用割合の取得
        pitch_usage1 = self.db.get_pitch_usage_data(pitcher_id, season1)
        pitch_usage2 = self.db.get_pitch_usage_data(pitcher_id, season2)
        
        # 各指標の差異を計算
        diff = {}
        for metric in ['era', 'fip', 'whip', 'k_per_9', 'bb_per_9', 'hr_per_9', 
                      'swstr_pct', 'csw_pct', 'o_swing_pct', 'z_contact_pct']:
            val1 = metrics1_data.get(metric)
            val2 = metrics2_data.get(metric)
            
            if val1 is not None and val2 is not None:
                diff[metric] = val2 - val1
                # パーセンテージ差も計算（分母が0の場合は無限大）
                diff[f'{metric}_pct'] = (diff[metric] / val1) * 100 if val1 != 0 else float('inf')
            else:
                diff[metric] = None
                diff[f'{metric}_pct'] = None
                
        # 球種使用の変化を計算
        pitch_usage_diff = []
        
        # 全球種のコードを集める
        all_pitch_codes = set()
        for pitch in pitch_usage1:
            all_pitch_codes.add(pitch['code'])
        for pitch in pitch_usage2:
            all_pitch_codes.add(pitch['code'])
            
        # 球種ごとの差異を計算
        for code in all_pitch_codes:
            # シーズン1の球種データを検索
            pitch1 = next((p for p in pitch_usage1 if p['code'] == code), None)
            # シーズン2の球種データを検索
            pitch2 = next((p for p in pitch_usage2 if p['code'] == code), None)
            
            usage1 = pitch1['usage_pct'] if pitch1 else 0
            usage2 = pitch2['usage_pct'] if pitch2 else 0
            
            velocity1 = pitch1['avg_velocity'] if pitch1 else None
            velocity2 = pitch2['avg_velocity'] if pitch2 else None
            
            spin1 = pitch1['avg_spin_rate'] if pitch1 else None
            spin2 = pitch2['avg_spin_rate'] if pitch2 else None
            
            whiff1 = pitch1['whiff_pct'] if pitch1 else None
            whiff2 = pitch2['whiff_pct'] if pitch2 else None
            
            # 差異を計算
            usage_diff = usage2 - usage1
            velocity_diff = velocity2 - velocity1 if velocity1 is not None and velocity2 is not None else None
            spin_diff = spin2 - spin1 if spin1 is not None and spin2 is not None else None
            whiff_diff = whiff2 - whiff1 if whiff1 is not None and whiff2 is not None else None
            
            pitch_usage_diff.append({
                'code': code,
                'name': pitch1['name'] if pitch1 else pitch2['name'] if pitch2 else code,
                'usage_season1': usage1,
                'usage_season2': usage2,
                'usage_diff': usage_diff,
                'velocity_season1': velocity1,
                'velocity_season2': velocity2,
                'velocity_diff': velocity_diff,
                'spin_season1': spin1,
                'spin_season2': spin2,
                'spin_diff': spin_diff,
                'whiff_season1': whiff1,
                'whiff_season2': whiff2,
                'whiff_diff': whiff_diff
            })
            
        # 結果の構築
        comparison = {
            'pitcher_id': pitcher_id,
            'season1': season1,
            'season2': season2,
            'metrics_season1': metrics1_data,
            'metrics_season2': metrics2_data,
            'metrics_diff': diff,
            'pitch_usage_diff': pitch_usage_diff
        }
        
        return comparison
        
    def analyze_performance_trend(self, pitcher_id: int, metric: str = 'era', 
                                seasons: Optional[List[int]] = None) -> Dict[str, Any]:
        """
        指定した指標のパフォーマンストレンドを分析
        
        Args:
            pitcher_id: ピッチャーID
            metric: 分析する指標
            seasons: 分析対象シーズンのリスト（指定しない場合は全シーズン）
            
        Returns:
            Dict: トレンド分析結果
        """
        # 全シーズンの成績指標を取得
        all_metrics = self.db.get_pitcher_metrics(pitcher_id)
        
        if not all_metrics:
            self.logger.warning(f"No metrics data found for pitcher {pitcher_id}")
            return {}
            
        # 特定シーズンでフィルタリング
        if seasons:
            filtered_metrics = [m for m in all_metrics if m['season'] in seasons]
        else:
            filtered_metrics = all_metrics
            
        if not filtered_metrics:
            self.logger.warning(f"No metrics data found for pitcher {pitcher_id} in specified seasons")
            return {}
            
        # DataFrameに変換
        df = pd.DataFrame(filtered_metrics)
        
        # 指定した指標がない場合
        if metric not in df.columns:
            self.logger.warning(f"Metric {metric} not found in data")
            return {}
            
        # トレンド分析
        trend_results = {
            'pitcher_id': pitcher_id,
            'metric': metric,
            'seasons': df['season'].tolist(),
            'values': df[metric].tolist(),
            'overall_trend': 'increasing' if df[metric].corr(df['season']) > 0 else 'decreasing',
            'min_value': df[metric].min(),
            'max_value': df[metric].max(),
            'min_season': df.loc[df[metric].idxmin(), 'season'],
            'max_season': df.loc[df[metric].idxmax(), 'season'],
            'avg_value': df[metric].mean(),
            'latest_value': df.loc[df['season'].idxmax(), metric],
            'latest_season': df['season'].max()
        }
        
        return trend_results