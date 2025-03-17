# tests/data_analysis/test_time_series_analyzer.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from src.data_analysis.time_series_analyzer import TimeSeriesAnalyzer

class TestTimeSeriesAnalyzer:
    
    def test_init(self):
        """TimeSeriesAnalyzerの初期化テスト"""
        analyzer = TimeSeriesAnalyzer()
        assert analyzer is not None
        
    def test_analyze_metric_trend(self):
        """指標トレンド分析テスト"""
        analyzer = TimeSeriesAnalyzer()
        
        # テストデータの作成
        dates = pd.date_range(start='2023-04-01', periods=50, freq='D')
        data = {
            'game_date': dates,
            'metric_a': [i * 0.1 for i in range(50)],  # 単調増加
            'metric_b': [np.sin(i * 0.1) * 10 for i in range(50)]  # 周期的変動
        }
        df = pd.DataFrame(data)
        
        # テスト実行
        result = analyzer.analyze_metric_trend(df, 'metric_a')
        
        # 検証
        assert 'rolling_avg' in result.columns
        assert 'trend' in result.columns
        
        # 単調増加なので、大部分が'increasing'になるはず
        increasing_count = result['trend'].value_counts().get('increasing', 0)
        assert increasing_count > 30
        
    def test_compare_periods(self):
        """期間比較テスト"""
        analyzer = TimeSeriesAnalyzer()
        
        # テストデータの作成
        dates = pd.date_range(start='2023-01-01', periods=100, freq='D')
        data = {
            'game_date': dates,
            'era': [4.0] * 50 + [3.0] * 50,  # 後半の方が良い
            'whip': [1.3] * 50 + [1.1] * 50   # 後半の方が良い
        }
        df = pd.DataFrame(data)
        
        # テスト実行
        result = analyzer.compare_periods(
            df, 'era', 'game_date', 
            period1_start='2023-01-01', period1_end='2023-02-19',
            period2_start='2023-02-20', period2_end='2023-04-10'
        )
        
        # 検証
        assert 'period1' in result
        assert 'period2' in result
        assert 'diff' in result
        
        assert result['period1']['mean'] == 4.0
        assert result['period2']['mean'] == 3.0
        assert result['diff']['mean'] == -1.0  # 3.0 - 4.0 = -1.0
        assert result['diff']['mean_pct'] == -25.0  # (3.0 - 4.0) / 4.0 * 100 = -25%
        
    def test_calculate_monthly_stats(self):
        """月次統計計算テスト"""
        analyzer = TimeSeriesAnalyzer()
        
        # テストデータの作成（3ヶ月分）
        jan_dates = pd.date_range(start='2023-01-01', end='2023-01-31', freq='D')
        feb_dates = pd.date_range(start='2023-02-01', end='2023-02-28', freq='D')
        mar_dates = pd.date_range(start='2023-03-01', end='2023-03-31', freq='D')
        
        dates = jan_dates.append([feb_dates, mar_dates])
        
        data = {
            'game_date': dates,
            'era': [4.0] * len(jan_dates) + [3.5] * len(feb_dates) + [3.0] * len(mar_dates)
        }
        df = pd.DataFrame(data)
        
        # テスト実行
        result = analyzer.calculate_monthly_stats(df, 'era')
        
        # 検証
        assert len(result) == 3  # 3ヶ月分
        
        # 1月のERA平均が4.0
        jan_row = result[result['year_month_str'] == '2023-01']
        assert jan_row['era_mean'].iloc[0] == 4.0
        
        # 3月のERA平均が3.0
        mar_row = result[result['year_month_str'] == '2023-03']
        assert mar_row['era_mean'].iloc[0] == 3.0
        
    def test_detect_performance_change(self):
        """パフォーマンス変化検出テスト"""
        analyzer = TimeSeriesAnalyzer()
        
        # テストデータの作成（前半と後半で明確な差がある）
        dates = pd.date_range(start='2023-01-01', periods=100, freq='D')
        data = {
            'game_date': dates,
            'era': [4.0] * 50 + [3.0] * 50  # 50日目に明確な変化
        }
        df = pd.DataFrame(data)
        
        # テスト実行
        changes = analyzer.detect_performance_change(df, 'era', window_size=10, threshold=1.0)
        
        # 検証
        assert len(changes) > 0
        
        # 変化が検出されたのが50日目前後であることを確認
        # （ウィンドウサイズの影響で、ちょうど50日目でない可能性あり）
        change_dates = [change['date'] for change in changes]
        change_dates_days = [d.day for d in change_dates]
        
        # 2/19か2/20あたりに変化が検出されるはず
        assert datetime(2023, 2, 19) in change_dates or datetime(2023, 2, 20) in change_dates