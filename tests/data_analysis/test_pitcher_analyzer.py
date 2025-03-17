# tests/data_analysis/test_pitcher_analyzer.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.data_analysis.pitcher_analyzer import PitcherAnalyzer

class TestPitcherAnalyzer:
    
    def test_init(self, populated_test_db):
        """PitcherAnalyzerの初期化テスト"""
        analyzer = PitcherAnalyzer(populated_test_db)
        assert analyzer is not None
        assert analyzer.db is populated_test_db
        
    def test_get_pitcher_summary(self, pitcher_analyzer):
        """投手サマリー取得テスト"""
        # テスト実行
        summary = pitcher_analyzer.get_pitcher_summary(1, 2023)
        
        # 検証
        assert summary is not None
        assert summary['name'] == 'Test Pitcher'
        assert summary['team'] == 'NYY'
        assert summary['season'] == 2023
        
        # メトリクスが含まれていることを確認
        metrics = summary['metrics']
        assert metrics['era'] == 3.45
        assert metrics['fip'] == 3.56
        assert metrics['whip'] == 1.21
        
        # 球種データが含まれていることを確認
        pitch_types = summary['pitch_types']
        assert len(pitch_types) == 3
        
        # 球種のソート順を確認（使用割合の降順）
        assert pitch_types[0]['code'] == 'FF'
        assert pitch_types[1]['code'] == 'SL'
        assert pitch_types[2]['code'] == 'CH'
        
    def test_compare_seasons(self, pitcher_analyzer, populated_test_db):
        """シーズン比較テスト"""
        # 追加のシーズンデータを挿入
        metrics_2022 = {
            'pitcher_id': 1,
            'season': 2022,
            'era': 3.75,
            'fip': 3.80,
            'whip': 1.25,
            'k_per_9': 9.0,
            'bb_per_9': 3.0,
            'hr_per_9': 1.3,
            'swstr_pct': 10.5,
            'csw_pct': 29.0,
            'o_swing_pct': 31.0,
            'z_contact_pct': 86.0,
            'innings_pitched': 175.0,
            'games': 28,
            'strikeouts': 170,
            'walks': 60,
            'home_runs': 25,
            'hits': 170,
            'earned_runs': 73
        }
        populated_test_db.update_pitcher_metrics(metrics_2022)
        
        # 球種使用割合データを2022年用に追加
        for pitch_type, code, usage_pct in [
            ('Four-Seam Fastball', 'FF', 55.0),
            ('Slider', 'SL', 25.0),
            ('Changeup', 'CH', 20.0)
        ]:
            pitch_type_id = populated_test_db.get_pitch_type_id(code)
            
            usage_data = {
                'pitcher_id': 1,
                'pitch_type_id': pitch_type_id,
                'season': 2022,
                'usage_pct': usage_pct,
                'avg_velocity': 94.5 if code == 'FF' else (84.5 if code == 'SL' else 83.0),
                'avg_spin_rate': 2400 if code == 'FF' else (2580 if code == 'SL' else 1790),
                'whiff_pct': 8.0 if code == 'FF' else (22.0 if code == 'SL' else 14.0)
            }
            
            populated_test_db.update_pitch_usage(usage_data)
        
        # テスト実行
        comparison = pitcher_analyzer.compare_seasons(1, 2022, 2023)
        
        # 検証
        assert comparison is not None
        assert comparison['season1'] == 2022
        assert comparison['season2'] == 2023
        
        # ERA比較
        assert comparison['metrics_season1']['era'] == 3.75
        assert comparison['metrics_season2']['era'] == 3.45
        assert comparison['metrics_diff']['era'] == -0.3  # 改善
        
        # 球種使用割合の変化
        pitch_usage_diff = comparison['pitch_usage_diff']
        assert len(pitch_usage_diff) == 3
        
        # FF（フォーシーム）の使用率変化
        ff_diff = next(p for p in pitch_usage_diff if p['code'] == 'FF')
        assert ff_diff['usage_season1'] == 55.0
        assert ff_diff['usage_season2'] == 50.0
        assert ff_diff['usage_diff'] == -5.0  # 減少
        
        # SL（スライダー）の使用率変化
        sl_diff = next(p for p in pitch_usage_diff if p['code'] == 'SL')
        assert sl_diff['usage_season1'] == 25.0
        assert sl_diff['usage_season2'] == 30.0
        assert sl_diff['usage_diff'] == 5.0  # 増加
        
    def test_analyze_performance_trend(self, pitcher_analyzer, populated_test_db):
        """パフォーマンストレンド分析テスト"""
        # 複数シーズンのデータを追加
        for season, era in [(2021, 4.00), (2022, 3.75), (2023, 3.45)]:
            metrics = {
                'pitcher_id': 1,
                'season': season,
                'era': era,
                'fip': 3.80 - (season - 2021) * 0.12,
                'whip': 1.30 - (season - 2021) * 0.05,
                'k_per_9': 8.5 + (season - 2021) * 0.5,
                'bb_per_9': 3.2 - (season - 2021) * 0.2,
                'hr_per_9': 1.4 - (season - 2021) * 0.1
            }
            populated_test_db.update_pitcher_metrics(metrics)
        
        # テスト実行
        trend = pitcher_analyzer.analyze_performance_trend(1, 'era')
        
        # 検証
        assert trend is not None
        assert trend['pitcher_id'] == 1
        assert trend['metric'] == 'era'
        assert len(trend['seasons']) == 3
        assert trend['seasons'] == [2021, 2022, 2023]
        assert trend['values'] == [4.00, 3.75, 3.45]
        assert trend['overall_trend'] == 'decreasing'  # ERAは低いほうがよい
        assert trend['min_value'] == 3.45
        assert trend['max_value'] == 4.00
        assert trend['min_season'] == 2023
        assert trend['max_season'] == 2021
        assert trend['latest_value'] == 3.45
        assert trend['latest_season'] == 2023