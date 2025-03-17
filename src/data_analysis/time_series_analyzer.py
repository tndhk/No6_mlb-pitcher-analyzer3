# src/data_analysis/time_series_analyzer.py
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

class TimeSeriesAnalyzer:
    """
    時系列データを分析するクラス
    """
    
    def __init__(self, logging_level=logging.INFO):
        """
        TimeSeriesAnalyzerの初期化
        
        Args:
            logging_level: ロギングレベル
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)
        
    def analyze_metric_trend(self, time_series_data: pd.DataFrame, 
                           metric_column: str, date_column: str = 'game_date',
                           window_size: int = 10) -> pd.DataFrame:
        """
        指標の時系列トレンドを分析
        
        Args:
            time_series_data: 時系列データ
            metric_column: 分析する指標のカラム名
            date_column: 日付カラム名
            window_size: 移動平均のウィンドウサイズ
            
        Returns:
            DataFrame: トレンド分析結果
        """
        if time_series_data.empty or metric_column not in time_series_data.columns:
            self.logger.warning(f"No data for metric: {metric_column}")
            return pd.DataFrame()
            
        # 日付順にソート
        if date_column in time_series_data.columns:
            sorted_data = time_series_data.sort_values(by=date_column)
        else:
            self.logger.warning(f"Date column {date_column} not found")
            sorted_data = time_series_data
            
        # 欠損値を処理
        sorted_data = sorted_data.dropna(subset=[metric_column])
        
        if sorted_data.empty:
            return pd.DataFrame()
            
        # 移動平均を計算
        sorted_data['rolling_avg'] = sorted_data[metric_column].rolling(window=window_size, min_periods=1).mean()
        
        # 傾向（上昇・下降）を計算
        sorted_data['trend'] = sorted_data['rolling_avg'].diff().apply(lambda x: 'increasing' if x > 0 else 'decreasing' if x < 0 else 'stable')
        
        return sorted_data
        
    def compare_periods(self, data: pd.DataFrame, metric_column: str, 
                      date_column: str = 'game_date', 
                      period1_start: str = None, period1_end: str = None,
                      period2_start: str = None, period2_end: str = None) -> Dict[str, Any]:
        """
        2つの期間のパフォーマンスを比較
        
        Args:
            data: データフレーム
            metric_column: 比較する指標のカラム名
            date_column: 日付カラム名
            period1_start: 期間1の開始日 (YYYY-MM-DD)
            period1_end: 期間1の終了日 (YYYY-MM-DD)
            period2_start: 期間2の開始日 (YYYY-MM-DD)
            period2_end: 期間2の終了日 (YYYY-MM-DD)
            
        Returns:
            Dict: 期間比較結果
        """
        if data.empty or metric_column not in data.columns or date_column not in data.columns:
            self.logger.warning(f"Invalid data for period comparison")
            return {}
            
        # 日付列が文字列の場合はdatetime型に変換
        if not pd.api.types.is_datetime64_dtype(data[date_column]):
            data[date_column] = pd.to_datetime(data[date_column])
            
        # 期間1のデータ
        period1_data = data
        if period1_start:
            period1_data = period1_data[period1_data[date_column] >= pd.to_datetime(period1_start)]
        if period1_end:
            period1_data = period1_data[period1_data[date_column] <= pd.to_datetime(period1_end)]
            
        # 期間2のデータ
        period2_data = data
        if period2_start:
            period2_data = period2_data[period2_data[date_column] >= pd.to_datetime(period2_start)]
        if period2_end:
            period2_data = period2_data[period2_data[date_column] <= pd.to_datetime(period2_end)]
            
        # 各期間の統計を計算
        period1_stats = {
            'mean': period1_data[metric_column].mean() if not period1_data.empty else None,
            'median': period1_data[metric_column].median() if not period1_data.empty else None,
            'min': period1_data[metric_column].min() if not period1_data.empty else None,
            'max': period1_data[metric_column].max() if not period1_data.empty else None,
            'std': period1_data[metric_column].std() if not period1_data.empty else None,
            'count': len(period1_data)
        }
        
        period2_stats = {
            'mean': period2_data[metric_column].mean() if not period2_data.empty else None,
            'median': period2_data[metric_column].median() if not period2_data.empty else None,
            'min': period2_data[metric_column].min() if not period2_data.empty else None,
            'max': period2_data[metric_column].max() if not period2_data.empty else None,
            'std': period2_data[metric_column].std() if not period2_data.empty else None,
            'count': len(period2_data)
        }
        
        # 差分の計算
        diff = {}
        for stat in ['mean', 'median', 'min', 'max']:
            if period1_stats[stat] is not None and period2_stats[stat] is not None:
                diff[stat] = period2_stats[stat] - period1_stats[stat]
                diff[f'{stat}_pct'] = (diff[stat] / period1_stats[stat]) * 100 if period1_stats[stat] != 0 else float('inf')
            else:
                diff[stat] = None
                diff[f'{stat}_pct'] = None
                
        return {
            'period1': period1_stats,
            'period2': period2_stats,
            'diff': diff,
            'period1_dates': (period1_data[date_column].min(), period1_data[date_column].max()) if not period1_data.empty else (None, None),
            'period2_dates': (period2_data[date_column].min(), period2_data[date_column].max()) if not period2_data.empty else (None, None)
        }
        
    def calculate_monthly_stats(self, data: pd.DataFrame, metric_column: str, 
                              date_column: str = 'game_date') -> pd.DataFrame:
        """
        月ごとの統計を計算
        
        Args:
            data: データフレーム
            metric_column: 分析する指標のカラム名
            date_column: 日付カラム名
            
        Returns:
            DataFrame: 月ごとの統計情報
        """
        if data.empty or metric_column not in data.columns or date_column not in data.columns:
            self.logger.warning(f"Invalid data for monthly stats calculation")
            return pd.DataFrame()
            
        # 日付列が文字列の場合はdatetime型に変換
        if not pd.api.types.is_datetime64_dtype(data[date_column]):
            data[date_column] = pd.to_datetime(data[date_column])
            
        # 月ごとにグループ化
        data['year_month'] = data[date_column].dt.to_period('M')
        monthly_stats = data.groupby('year_month').agg({
            metric_column: ['mean', 'median', 'min', 'max', 'std', 'count']
        })
        
        # マルチインデックスを解除
        monthly_stats.columns = ['_'.join(col).strip() for col in monthly_stats.columns.values]
        monthly_stats = monthly_stats.reset_index()
        
        # 年月を文字列に変換（表示用）
        monthly_stats['year_month_str'] = monthly_stats['year_month'].astype(str)
        
        return monthly_stats
        
    def detect_performance_change(self, data: pd.DataFrame, metric_column: str, 
                                date_column: str = 'game_date', 
                                window_size: int = 10, 
                                threshold: float = 1.5) -> List[Dict[str, Any]]:
        """
        パフォーマンスの大きな変化を検出
        
        Args:
            data: データフレーム
            metric_column: 分析する指標のカラム名
            date_column: 日付カラム名
            window_size: 移動平均のウィンドウサイズ
            threshold: 変化を検出するための閾値（標準偏差の倍数）
            
        Returns:
            List[Dict]: 検出された変化点のリスト
        """
        if data.empty or metric_column not in data.columns or date_column not in data.columns:
            self.logger.warning(f"Invalid data for change detection")
            return []
            
        # 日付列が文字列の場合はdatetime型に変換
        if not pd.api.types.is_datetime64_dtype(data[date_column]):
            data[date_column] = pd.to_datetime(data[date_column])
            
        # 日付順にソート
        sorted_data = data.sort_values(by=date_column)
        
        # 欠損値を処理
        sorted_data = sorted_data.dropna(subset=[metric_column])
        
        if len(sorted_data) < window_size * 2:
            self.logger.warning(f"Not enough data points for change detection (needed: {window_size*2}, got: {len(sorted_data)})")
            return []
            
        # 移動平均と移動標準偏差を計算
        sorted_data['rolling_avg'] = sorted_data[metric_column].rolling(window=window_size, min_periods=1).mean()
        sorted_data['rolling_std'] = sorted_data[metric_column].rolling(window=window_size, min_periods=1).std()
        
        # 変化点の検出
        change_points = []
        
        for i in range(window_size, len(sorted_data) - window_size):
            # 前後の期間のデータ
            before = sorted_data.iloc[i-window_size:i]
            after = sorted_data.iloc[i:i+window_size]
            
            # 平均の差
            mean_diff = abs(after[metric_column].mean() - before[metric_column].mean())
            
            # 標準偏差
            pooled_std = np.sqrt((before[metric_column].std()**2 + after[metric_column].std()**2) / 2)
            
            # 効果量（Cohen's d）を計算
            effect_size = mean_diff / pooled_std if pooled_std > 0 else 0
            
            # 閾値を超える変化点を記録
            if effect_size > threshold:
                change_points.append({
                    'date': sorted_data.iloc[i][date_column],
                    'metric': metric_column,
                    'before_mean': before[metric_column].mean(),
                    'after_mean': after[metric_column].mean(),
                    'diff': mean_diff,
                    'diff_pct': (mean_diff / before[metric_column].mean()) * 100 if before[metric_column].mean() != 0 else float('inf'),
                    'effect_size': effect_size,
                    'direction': 'increase' if after[metric_column].mean() > before[metric_column].mean() else 'decrease'
                })
                
        # 変化点を日付順にソート
        change_points.sort(key=lambda x: x['date'])
        
        return change_points