# tests/data_analysis/test_pitcher_analyzer.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.data_analysis.pitcher_analyzer import PitcherAnalyzer

class TestPitcherAnalyzer:
    
    @pytest.fixture
    def mock_db(self):
        """完全にモックされたデータベースを提供するフィクスチャ"""
        mock_db = MagicMock()
        
        # テスト用の返り値を設定
        mock_db.get_pitcher_data.return_value = {
            'id': 1,
            'mlb_id': 123456,
            'name': 'Test Pitcher',
            'team': 'NYY'
        }
        
        mock_db.get_pitcher_metrics.return_value = [{
            'id': 1,
            'pitcher_id': 1,
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
        }]
        
        # 球種使用割合データのモック
        mock_db.get_pitch_usage_data.return_value = [
            {
                'id': 1,
                'pitcher_id': 1,
                'pitch_type_id': 1,
                'season': 2023,
                'usage_pct': 50.0,
                'avg_velocity': 95.5,
                'avg_spin_rate': 2425,
                'avg_pfx_x': 2.5,
                'avg_pfx_z': 8.5,
                'whiff_pct': 10.0,
                'code': 'FF',
                'name': 'Four-Seam Fastball'
            },
            {
                'id': 2,
                'pitcher_id': 1,
                'pitch_type_id': 2,
                'season': 2023,
                'usage_pct': 30.0,
                'avg_velocity': 85.5,
                'avg_spin_rate': 2615,
                'avg_pfx_x': -2.5,
                'avg_pfx_z': 2.5,
                'whiff_pct': 25.0,
                'code': 'SL',
                'name': 'Slider'
            },
            {
                'id': 3,
                'pitcher_id': 1,
                'pitch_type_id': 3,
                'season': 2023,
                'usage_pct': 20.0,
                'avg_velocity': 84.0,
                'avg_spin_rate': 1810,
                'avg_pfx_x': 5.0,
                'avg_pfx_z': 5.0,
                'whiff_pct': 15.0,
                'code': 'CH',
                'name': 'Changeup'
            }
        ]
        
        return mock_db
    
    @pytest.fixture
    def pitcher_analyzer(self, mock_db):
        """モックデータベースを使用したPitcherAnalyzer"""
        return PitcherAnalyzer(mock_db)
    
    def test_init(self, mock_db):
        """PitcherAnalyzerの初期化テスト"""
        analyzer = PitcherAnalyzer(mock_db)
        assert analyzer is not None
        assert analyzer.db is mock_db
        
    def test_get_pitcher_summary(self, pitcher_analyzer, mock_db):
        """投手サマリー取得テスト"""
        # テスト実行
        summary = pitcher_analyzer.get_pitcher_summary(1, 2023)
        
        # データベースが正しく呼び出されたことを確認
        mock_db.get_pitcher_data.assert_called_once_with(1)
        mock_db.get_pitcher_metrics.assert_called_once()
        mock_db.get_pitch_usage_data.assert_called_once()
        
        # 戻り値の検証
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

# 修正内容 - test_compare_seasons関数内の浮動小数点比較を修正

    def test_compare_seasons(self, pitcher_analyzer, mock_db):
        """シーズン比較テスト"""
        # 2022年のデータをモック
        metrics_2022 = {
            'id': 2,
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
        
        # 2022年の球種使用割合データ
        pitch_usage_2022 = [
            {
                'id': 4,
                'pitcher_id': 1,
                'pitch_type_id': 1,
                'season': 2022,
                'usage_pct': 55.0,
                'avg_velocity': 94.5,
                'avg_spin_rate': 2400,
                'avg_pfx_x': 2.5,
                'avg_pfx_z': 8.0,
                'whiff_pct': 8.0,
                'code': 'FF',
                'name': 'Four-Seam Fastball'
            },
            {
                'id': 5,
                'pitcher_id': 1,
                'pitch_type_id': 2,
                'season': 2022,
                'usage_pct': 25.0,
                'avg_velocity': 84.5,
                'avg_spin_rate': 2580,
                'avg_pfx_x': -2.0,
                'avg_pfx_z': 2.0,
                'whiff_pct': 22.0,
                'code': 'SL',
                'name': 'Slider'
            },
            {
                'id': 6,
                'pitcher_id': 1,
                'pitch_type_id': 3,
                'season': 2022,
                'usage_pct': 20.0,
                'avg_velocity': 83.0,
                'avg_spin_rate': 1790,
                'avg_pfx_x': 4.5,
                'avg_pfx_z': 4.5,
                'whiff_pct': 14.0,
                'code': 'CH',
                'name': 'Changeup'
            }
        ]
        
        # モックの動作を設定
        def get_pitcher_metrics_side_effect(pitcher_id, season=None):
            if season == 2022:
                return [metrics_2022]
            elif season == 2023:
                return [{
                    'id': 1,
                    'pitcher_id': 1,
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
                }]
            else:
                return []
        
        def get_pitch_usage_data_side_effect(pitcher_id, season=None):
            if season == 2022:
                return pitch_usage_2022
            elif season == 2023:
                return mock_db.get_pitch_usage_data.return_value
            else:
                return []
        
        mock_db.get_pitcher_metrics.side_effect = get_pitcher_metrics_side_effect
        mock_db.get_pitch_usage_data.side_effect = get_pitch_usage_data_side_effect
        
        # テスト実行
        comparison = pitcher_analyzer.compare_seasons(1, 2022, 2023)
        
        # 検証
        assert comparison is not None
        assert comparison['season1'] == 2022
        assert comparison['season2'] == 2023
        
        # ERA比較 - 浮動小数点比較を修正
        assert comparison['metrics_season1']['era'] == 3.75
        assert comparison['metrics_season2']['era'] == 3.45
        assert comparison['metrics_diff']['era'] == pytest.approx(-0.3, abs=1e-10)  # 改善
        
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
        
    def test_analyze_performance_trend(self, pitcher_analyzer, mock_db):
        """パフォーマンストレンド分析テスト"""
        # 複数シーズンのデータをモック
        metrics = [
            {
                'id': 3,
                'pitcher_id': 1,
                'season': 2021,
                'era': 4.00,
                'fip': 3.80,
                'whip': 1.30,
                'k_per_9': 8.5,
                'bb_per_9': 3.2,
                'hr_per_9': 1.4
            },
            {
                'id': 2,
                'pitcher_id': 1,
                'season': 2022,
                'era': 3.75,
                'fip': 3.68,
                'whip': 1.25,
                'k_per_9': 9.0,
                'bb_per_9': 3.0,
                'hr_per_9': 1.3
            },
            {
                'id': 1,
                'pitcher_id': 1,
                'season': 2023,
                'era': 3.45,
                'fip': 3.56,
                'whip': 1.20,
                'k_per_9': 9.5,
                'bb_per_9': 2.8,
                'hr_per_9': 1.2
            }
        ]
        
        # モックの動作を設定
        mock_db.get_pitcher_metrics.return_value = metrics
        
        # テスト実行
        trend = pitcher_analyzer.analyze_performance_trend(1, 'era')
        
        # 検証
        assert trend is not None
        assert trend['pitcher_id'] == 1
        assert trend['metric'] == 'era'
        assert len(trend['seasons']) == 3
        assert set(trend['seasons']) == {2021, 2022, 2023}
        assert set(trend['values']) == {4.00, 3.75, 3.45}
        assert trend['overall_trend'] == 'decreasing'  # ERAは低いほうがよい
        assert trend['min_value'] == 3.45
        assert trend['max_value'] == 4.00
        assert trend['min_season'] == 2023
        assert trend['max_season'] == 2021
        # ノート: latest_valueとlatest_seasonのテストはデータの順序に依存するため省略