# tests/data_analysis/test_statistical_calculator.py
import pytest
import pandas as pd
import numpy as np

from src.data_analysis.statistical_calculator import StatisticalCalculator

class TestStatisticalCalculator:
    
    def test_init(self):
        """StatisticalCalculatorの初期化テスト"""
        calculator = StatisticalCalculator()
        assert calculator is not None
        
    def test_calculate_era(self):
        """ERA計算テスト"""
        calculator = StatisticalCalculator()
        
        # テストケース
        test_cases = [
            (69, 180.0, 3.45),  # 通常の値
            (0, 180.0, 0.0),    # 自責点0
            (69, 0.0, 0.0)      # イニング0（エラーではなく0を返す）
        ]
        
        for earned_runs, innings_pitched, expected in test_cases:
            result = calculator.calculate_era(earned_runs, innings_pitched)
            assert result == pytest.approx(expected, 0.01)
            
    def test_calculate_fip(self):
        """FIP計算テスト"""
        calculator = StatisticalCalculator()
        
        # テストケース
        test_cases = [
            (22, 55, 5, 182, 180.0, 3.10, 3.56),  # 通常の値
            (0, 0, 0, 0, 180.0, 3.10, 3.10),      # ゼロの値（リーグ定数のみ）
            (22, 55, 5, 182, 0.0, 3.10, 0.0)      # イニング0
        ]
        
        for hr, bb, hbp, k, innings_pitched, league_constant, expected in test_cases:
            result = calculator.calculate_fip(hr, bb, hbp, k, innings_pitched, league_constant)
            assert result == pytest.approx(expected, 0.01)
            
    def test_calculate_whip(self):
        """WHIP計算テスト"""
        calculator = StatisticalCalculator()
        
        # テストケース
        test_cases = [
            (165, 55, 180.0, 1.22),  # 通常の値
            (0, 0, 180.0, 0.0),      # 被安打・四球0
            (165, 55, 0.0, 0.0)      # イニング0
        ]
        
        for hits, walks, innings_pitched, expected in test_cases:
            result = calculator.calculate_whip(hits, walks, innings_pitched)
            assert result == pytest.approx(expected, 0.01)
            
    def test_calculate_swing_metrics(self):
        """スイング指標計算テスト"""
        calculator = StatisticalCalculator()
        
        # テストデータの作成
        data = {
            'is_swing': [True, True, False, False, True] * 20,
            'is_strike': [True, True, True, False, False] * 20,
            'is_whiff': [True, False, False, False, False] * 20,
            'is_in_zone': [True, True, True, False, False] * 20
        }
        df = pd.DataFrame(data)
        
        # テスト実行
        metrics = calculator.calculate_swing_metrics(df)
        
        # 検証
        assert 'swstr_pct' in metrics
        assert 'csw_pct' in metrics
        assert 'o_swing_pct' in metrics
        assert 'z_contact_pct' in metrics
        
        # swstr_pct: 空振り率 (20/100 = 20%)
        assert metrics['swstr_pct'] == 20.0
        
        # csw_pct: 見逃し三振 + 空振り率 (20 + 20 = 40%)
        assert metrics['csw_pct'] == 40.0
        
        # o_swing_pct: ゾーン外スイング率 (20/40 = 50%)
        assert metrics['o_swing_pct'] == 50.0
        
        # z_contact_pct: ゾーン内コンタクト率 (20/40 = 50%)
        assert metrics['z_contact_pct'] == 50.0
        
    def test_calculate_pitch_metrics(self):
        """球種指標計算テスト"""
        calculator = StatisticalCalculator()
        
        # テストデータの作成
        data = {
            'pitch_type': ['FF'] * 50 + ['SL'] * 30 + ['CH'] * 20,
            'release_speed': [95.0] * 50 + [85.0] * 30 + [83.0] * 20,
            'release_spin_rate': [2400] * 50 + [2600] * 30 + [1800] * 20,
            'pfx_x': [2.0] * 50 + [-2.0] * 30 + [8.0] * 20,
            'pfx_z': [8.0] * 50 + [2.0] * 30 + [4.0] * 20,
            'is_swing': [True] * 40 + [False] * 60,
            'is_strike': [True] * 60 + [False] * 40,
            'is_whiff': [True] * 20 + [False] * 80,
            'is_in_zone': [True] * 60 + [False] * 40
        }
        df = pd.DataFrame(data)
        
        # 全球種のテスト
        all_metrics = calculator.calculate_pitch_metrics(df)
        
        # 検証
        assert 'avg_velocity' in all_metrics
        assert 'avg_spin_rate' in all_metrics
        assert 'avg_pfx_x' in all_metrics
        assert 'avg_pfx_z' in all_metrics
        assert 'usage_pct' in all_metrics
        assert all_metrics['usage_count'] == 100
        assert all_metrics['usage_pct'] == 100.0
        
        # 特定球種のテスト
        ff_metrics = calculator.calculate_pitch_metrics(df, 'FF')
        
        # 検証
        assert ff_metrics['avg_velocity'] == 95.0
        assert ff_metrics['avg_spin_rate'] == 2400
        assert ff_metrics['usage_count'] == 50
        assert ff_metrics['usage_pct'] == 50.0
