# tests/integration/test_real_world.py
import pytest
import pandas as pd
import os
import sqlite3
import tempfile
import json
import datetime
from unittest.mock import patch, MagicMock, mock_open

from src.data_acquisition.statcast_client import StatcastClient
from src.data_acquisition.batch_processor import BatchProcessor
from src.data_acquisition.team_processor import TeamProcessor
from src.data_storage.database import Database
from src.data_storage.data_manager import DataManager
from src.data_analysis.pitcher_analyzer import PitcherAnalyzer

class TestRealWorldScenarios:
    """
    実際の使用シナリオに近い形での結合テスト
    """
    
    @pytest.fixture
    def mock_api_responses(self):
        """実際のAPIレスポンスに似たモックデータを提供するフィクスチャ"""
        # StatcastのAPIレスポンスをシミュレート
        statcast_response = pd.DataFrame({
            'game_date': pd.date_range(start='2023-04-01', periods=200, freq='D'),
            'player_name': ['Shohei Ohtani'] * 200,
            'pitcher': [660271] * 200,  # Ohtani's MLB ID
            'pitch_type': ['FF'] * 70 + ['SL'] * 50 + ['CU'] * 30 + ['CH'] * 30 + ['FS'] * 20,
            'release_speed': ([95.5 + i * 0.01 for i in range(70)] +
                            [86.5 + i * 0.01 for i in range(50)] +
                            [82.5 + i * 0.01 for i in range(30)] +
                            [84.5 + i * 0.01 for i in range(30)] +
                            [88.5 + i * 0.01 for i in range(20)]),
            'release_spin_rate': ([2400 + i for i in range(70)] +
                                [2600 + i for i in range(50)] +
                                [2800 + i for i in range(30)] +
                                [1800 + i for i in range(30)] +
                                [2200 + i for i in range(20)]),
            'pfx_x': ([2.0 + i * 0.01 for i in range(70)] +
                     [-4.0 - i * 0.01 for i in range(50)] +
                     [8.0 + i * 0.01 for i in range(30)] +
                     [6.0 + i * 0.01 for i in range(30)] +
                     [-2.0 - i * 0.01 for i in range(20)]),
            'pfx_z': ([10.0 + i * 0.01 for i in range(70)] +
                     [2.0 + i * 0.01 for i in range(50)] +
                     [-8.0 - i * 0.01 for i in range(30)] +
                     [4.0 + i * 0.01 for i in range(30)] +
                     [0.0 for i in range(20)]),
            'plate_x': [0.0 + i * 0.01 for i in range(200)],
            'plate_z': [2.5 + i * 0.005 for i in range(200)],
            'description': (['swinging_strike'] * 40 +
                           ['called_strike'] * 30 +
                           ['ball'] * 50 +
                           ['foul'] * 40 +
                           ['hit_into_play'] * 40),
            'zone': [5] * 40 + [1] * 20 + [2] * 20 + [3] * 20 + [4] * 20 +
                   [6] * 20 + [7] * 20 + [8] * 20 + [9] * 20,
            'type': ['S'] * 110 + ['B'] * 50 + ['X'] * 40,
            'home_team': ['LAA'] * 100 + ['HOU'] * 50 + ['SEA'] * 50,
            'away_team': ['HOU'] * 50 + ['SEA'] * 50 + ['LAA'] * 100
        })
        
        # playerid_lookupのレスポンスをシミュレート
        playerid_lookup_response = pd.DataFrame({
            'key_mlbam': [660271],
            'key_retro': ['ohtas001'],
            'name_last': ['Ohtani'],
            'name_first': ['Shohei'],
            'key_bbref': ['ohtansh01'],
            'key_fangraphs': [19755],
            'mlb_played_first': [2018],
            'mlb_played_last': [2023]
        })
        
        # チーム投手リストのレスポンスをシミュレート
        team_pitchers_response = [
            {
                'mlbam_id': 660271,
                'name': 'Shohei Ohtani',
                'position': 'P',
                'team': 'LAA'
            },
            {
                'mlbam_id': 123456,
                'name': 'Fake Pitcher 1',
                'position': 'P',
                'team': 'LAA'
            },
            {
                'mlbam_id': 234567,
                'name': 'Fake Pitcher 2',
                'position': 'P',
                'team': 'LAA'
            }
        ]
        
        return {
            'statcast': statcast_response,
            'playerid_lookup': playerid_lookup_response,
            'team_pitchers': team_pitchers_response
        }
    
    @pytest.mark.integration
    def test_update_data_workflow(self, mock_api_responses):
        """update_data.pyのワークフローシミュレーション"""
        # テスト用の一時DBファイルパス
        db_path = "test_update_workflow.db"
        
        try:
            # 既存のテストDBがあれば削除
            if os.path.exists(db_path):
                os.remove(db_path)
            
            # モックの設定
            with patch('src.data_acquisition.statcast_client.statcast_pitcher') as mock_statcast, \
                 patch('src.data_acquisition.statcast_client.playerid_lookup') as mock_playerid_lookup, \
                 patch('src.data_acquisition.team_processor.TeamProcessor.get_team_pitchers') as mock_get_team_pitchers:
                
                # APIレスポンスをモック
                mock_statcast.return_value = mock_api_responses['statcast']
                mock_playerid_lookup.return_value = mock_api_responses['playerid_lookup']
                mock_get_team_pitchers.return_value = mock_api_responses['team_pitchers']
                
                # 実際のコンポーネントの初期化
                client = StatcastClient()
                team_processor = TeamProcessor()
                db = Database(db_path)
                data_manager = DataManager(db)
                batch_processor = BatchProcessor(client, max_workers=2, rate_limit_pause=0.1)
                
                # ワークフローのシミュレーション
                # 1. チームの投手リストを取得
                team = "LAA"
                pitchers = team_processor.get_team_pitchers(team, 2023)
                
                assert len(pitchers) > 0, "投手リストが空です"
                
                # 2. 各投手のデータを処理
                for pitcher in pitchers:
                    mlb_id = pitcher['mlbam_id']
                    name = pitcher['name']
                    
                    # 投手データの取得
                    data = client.get_last_n_years_data(mlb_id, years=1)
                    assert not data.empty, f"投手 {name} のデータが空です"
                    
                    # データの変換
                    transformed_data = client.transform_pitcher_data(data)
                    assert not transformed_data.empty, f"投手 {name} の変換後のデータが空です"
                    
                    # DBに投手情報を登録
                    db_pitcher_id = db.get_pitcher_id(mlb_id)
                    if db_pitcher_id is None:
                        db_pitcher_id = db.insert_pitcher(mlb_id, name, team)
                    
                    # データ処理と保存
                    data_manager.process_statcast_data(db_pitcher_id, mlb_id, name, transformed_data, team)
                
                # 3. 保存されたデータの確認
                conn = db._get_connection()
                cursor = conn.cursor()
                
                # 投手データのチェック
                cursor.execute("SELECT COUNT(*) as count FROM pitchers")
                pitchers_count = cursor.fetchone()['count']
                assert pitchers_count == len(pitchers), f"保存された投手数が一致しません: expected={len(pitchers)}, actual={pitchers_count}"
                
                # 投球データのチェック
                cursor.execute("SELECT COUNT(*) as count FROM pitches")
                pitches_count = cursor.fetchone()['count']
                assert pitches_count > 0, "投球データが保存されていません"
                
                # 球種使用割合のチェック
                cursor.execute("SELECT COUNT(*) as count FROM pitch_usage")
                usage_count = cursor.fetchone()['count']
                assert usage_count > 0, "球種使用割合データが保存されていません"
                
                # 成績指標のチェック
                cursor.execute("SELECT COUNT(*) as count FROM pitcher_metrics")
                metrics_count = cursor.fetchone()['count']
                assert metrics_count > 0, "成績指標データが保存されていません"
                
                conn.close()
                
        finally:
            # テスト後のクリーンアップ
            if os.path.exists(db_path):
                os.remove(db_path)
    
    @pytest.mark.integration
    def test_complex_pitcher_comparison(self, mock_api_responses):
        """複数投手の比較分析シナリオ"""
        # テスト用の一時DBファイルパス
        db_path = "test_complex_comparison.db"
        
        try:
            # 既存のテストDBがあれば削除
            if os.path.exists(db_path):
                os.remove(db_path)
            
            # DBとコンポーネントの初期化
            db = Database(db_path)
            data_manager = DataManager(db)
            analyzer = PitcherAnalyzer(db)
            
            # テスト用の投手データを登録
            pitcher_ids = []
            for i, (mlb_id, name, team) in enumerate([
                (660271, "Shohei Ohtani", "LAA"),
                (543037, "Gerrit Cole", "NYY"),
                (477132, "Max Scherzer", "NYM")
            ]):
                # 投手情報の登録
                db_pitcher_id = db.insert_pitcher(mlb_id, name, team)
                pitcher_ids.append(db_pitcher_id)
                
                # 成績指標の登録（投手ごとに異なる傾向）
                for season in [2021, 2022, 2023]:
                    # それぞれの投手の特徴を反映
                    if name == "Shohei Ohtani":
                        # 多才な投手、年々向上
                        era_base = 3.20 - (season - 2021) * 0.15
                        whip_base = 1.10 - (season - 2021) * 0.03
                        k_rate = 10.8 + (season - 2021) * 0.4
                    elif name == "Gerrit Cole":
                        # 安定した好成績
                        era_base = 3.00 + (season - 2021) * 0.05
                        whip_base = 1.05 + (season - 2021) * 0.01
                        k_rate = 12.0 - (season - 2021) * 0.1
                    else:  # Max Scherzer
                        # ベテラン投手、やや衰え
                        era_base = 2.80 + (season - 2021) * 0.25
                        whip_base = 0.95 + (season - 2021) * 0.05
                        k_rate = 11.5 - (season - 2021) * 0.3
                    
                    metrics = {
                        'pitcher_id': db_pitcher_id,
                        'season': season,
                        'era': era_base + (i * 0.1),
                        'fip': era_base - 0.2 + (i * 0.08),
                        'whip': whip_base + (i * 0.02),
                        'k_per_9': k_rate,
                        'bb_per_9': 2.5 + (i * 0.2),
                        'hr_per_9': 1.0 + (i * 0.1),
                        'swstr_pct': 11.0 + (k_rate - 9.0) * 0.5,
                        'csw_pct': 30.0 + (k_rate - 9.0) * 1.2,
                        'o_swing_pct': 32.0 + (i * 0.5),
                        'z_contact_pct': 85.0 - (i * 1.0),
                        'innings_pitched': 180.0 - (i * 10.0) - (2023 - season) * 5.0,
                        'games': 30 - (i * 2) - (2023 - season),
                        'strikeouts': int(k_rate * (180.0 - (i * 10.0) - (2023 - season) * 5.0) / 9),
                        'walks': int((2.5 + (i * 0.2)) * (180.0 - (i * 10.0) - (2023 - season) * 5.0) / 9),
                        'home_runs': int((1.0 + (i * 0.1)) * (180.0 - (i * 10.0) - (2023 - season) * 5.0) / 9),
                        'hits': int((whip_base + (i * 0.02) - (2.5 + (i * 0.2)) / 9) * (180.0 - (i * 10.0) - (2023 - season) * 5.0)),
                        'earned_runs': int(era_base * (180.0 - (i * 10.0) - (2023 - season) * 5.0) / 9)
                    }
                    
                    db.update_pitcher_metrics(metrics)
                
                # 球種データの登録
                pitch_types = [
                    {'code': 'FF', 'name': 'Four-Seam Fastball', 'description': 'Standard fastball'},
                    {'code': 'SL', 'name': 'Slider', 'description': 'Breaking ball'},
                    {'code': 'CU', 'name': 'Curveball', 'description': 'Breaking ball with downward action'},
                    {'code': 'CH', 'name': 'Changeup', 'description': 'Off-speed pitch'},
                    {'code': 'FS', 'name': 'Splitter', 'description': 'Forkball/Split-finger fastball'}
                ]
                db.insert_pitch_types(pitch_types)
                
                # 投手ごとの特徴的な球種構成
                if name == "Shohei Ohtani":
                    # スプリッターを多用
                    pitch_mix = [
                        ('FF', 40, 95.5, 2400),
                        ('SL', 25, 82.5, 2600),
                        ('CU', 5, 75.0, 2800),
                        ('CH', 5, 85.0, 1800),
                        ('FS', 25, 89.0, 2100)
                    ]
                elif name == "Gerrit Cole":
                    # フォーシームとスライダー中心
                    pitch_mix = [
                        ('FF', 55, 97.0, 2500),
                        ('SL', 30, 87.0, 2700),
                        ('CU', 10, 82.0, 2850),
                        ('CH', 5, 88.0, 1850),
                        ('FS', 0, 0, 0)  # 使用しない
                    ]
                else:  # Max Scherzer
                    # バランスの取れた球種構成
                    pitch_mix = [
                        ('FF', 45, 94.0, 2350),
                        ('SL', 20, 85.0, 2550),
                        ('CU', 15, 78.0, 2750),
                        ('CH', 15, 84.0, 1750),
                        ('FS', 5, 86.0, 2050)
                    ]
                
                # 球種使用データを登録
                for season in [2021, 2022, 2023]:
                    for code, pct, velo, spin in pitch_mix:
                        if pct > 0:  # 使用率が0より大きい場合のみ登録
                            pitch_type_id = db.get_pitch_type_id(code)
                            usage_data = {
                                'pitcher_id': db_pitcher_id,
                                'pitch_type_id': pitch_type_id,
                                'season': season,
                                'usage_pct': pct + (season - 2021) * (1 if code in ['SL', 'CU'] else -1),  # トレンド: 変化球増加
                                'avg_velocity': velo - (2023 - season) * 0.3,  # 過去の方が球速はやや速い
                                'avg_spin_rate': spin + (season - 2021) * 20,  # スピン率は徐々に上昇
                                'avg_pfx_x': (2.0 if code == 'FF' else 
                                           -4.0 if code == 'SL' else 
                                           6.0 if code == 'CU' else 
                                           4.0 if code == 'CH' else -2.0),
                                'avg_pfx_z': (10.0 if code == 'FF' else 
                                           2.0 if code == 'SL' else 
                                           -8.0 if code == 'CU' else 
                                           4.0 if code == 'CH' else 0.0),
                                'whiff_pct': (8.0 if code == 'FF' else 
                                           30.0 if code == 'SL' else 
                                           25.0 if code == 'CU' else 
                                           20.0 if code == 'CH' else 35.0)
                            }
                            db.update_pitch_usage(usage_data)
            
            # 様々な分析を実行して結合的な機能をテスト
            
            # 1. 投手サマリーの取得
            for pitcher_id in pitcher_ids:
                summary = analyzer.get_pitcher_summary(pitcher_id, 2023)
                
                assert summary is not None
                assert 'name' in summary
                assert 'team' in summary
                assert 'metrics' in summary
                assert 'pitch_types' in summary
                assert len(summary['pitch_types']) > 0
            
            # 2. シーズン比較
            for pitcher_id in pitcher_ids:
                comparison = analyzer.compare_seasons(pitcher_id, 2022, 2023)
                
                assert comparison is not None
                assert 'season1' in comparison
                assert 'season2' in comparison
                assert 'metrics_season1' in comparison
                assert 'metrics_season2' in comparison
                assert 'metrics_diff' in comparison
                assert 'pitch_usage_diff' in comparison
                
                # 球種使用割合の変化を確認
                for pitch_diff in comparison['pitch_usage_diff']:
                    if pitch_diff['code'] in ['SL', 'CU']:
                        # スライダーとカーブの使用率は増加傾向
                        assert pitch_diff['usage_diff'] >= -0.1  # 許容誤差を考慮
                    elif pitch_diff['code'] == 'FF':
                        # フォーシームの使用率は減少傾向
                        assert pitch_diff['usage_diff'] <= 0.1  # 許容誤差を考慮
            
            # 3. パフォーマンストレンド分析
            for pitcher_id in pitcher_ids:
                trend = analyzer.analyze_performance_trend(pitcher_id, 'era')
                
                assert trend is not None
                assert 'overall_trend' in trend
                
                # 投手名の取得
                pitcher_data = db.get_pitcher_data(pitcher_id)
                name = pitcher_data['name']
                
                # Ohtaniのみ、ERAは改善傾向（下降）
                if name == "Shohei Ohtani":
                    assert trend['overall_trend'] == 'decreasing'
                # Coleは安定、もしくは微増
                elif name == "Gerrit Cole":
                    assert trend['overall_trend'] in ['stable', 'increasing']
                # Scherzerは上昇傾向（悪化）
                else:
                    assert trend['overall_trend'] == 'increasing'
            
        finally:
            # テスト後のクリーンアップ
            if os.path.exists(db_path):
                os.remove(db_path)