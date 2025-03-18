# tests/integration/test_dashboard_ui.py
import pytest
import pandas as pd
import os
import tempfile
from unittest.mock import patch, MagicMock, PropertyMock

from src.data_storage.database import Database
from src.data_storage.data_manager import DataManager
from src.visualization.dashboard import Dashboard

class TestDashboardUI:
    """
    ダッシュボードUIの結合テスト
    """
    
    @pytest.fixture
    def mock_streamlit(self):
        """Streamlitのモジュールをモックするフィクスチャ"""
        with patch('src.visualization.dashboard.st') as mock_st:
            # セッション状態のモックをPropertyMockに変更
            session_state = {}
            type(mock_st).session_state = PropertyMock(return_value=session_state)
            
            # 他のStreamlit関数のモック
            mock_st.columns.return_value = [MagicMock(), MagicMock()]
            mock_st.tabs.return_value = [MagicMock(), MagicMock(), MagicMock(), MagicMock()]
            
            yield mock_st
    
    @pytest.fixture
    def test_db_with_data(self):
        """テストデータを含むデータベースを提供するフィクスチャ"""
        # 一時ファイルを使用
        db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        db_path = db_file.name
        db_file.close()
        
        try:
            # DBの初期化
            db = Database(db_path)
            data_manager = DataManager(db)
            
            # テストデータの投入
            # 1. 投手情報
            pitcher_ids = []
            for i, (name, team) in enumerate([
                ("Test Pitcher 1", "NYY"),
                ("Test Pitcher 2", "LAD"),
                ("Test Pitcher 3", "BOS")
            ]):
                pitcher_id = db.insert_pitcher(100000 + i, name, team)
                pitcher_ids.append(pitcher_id)
                
                # 2. 各投手の成績指標
                for season in [2021, 2022, 2023]:
                    metrics = {
                        'pitcher_id': pitcher_id,
                        'season': season,
                        'era': 3.0 + i * 0.5 + (2023 - season) * 0.2,
                        'fip': 3.2 + i * 0.4 + (2023 - season) * 0.15,
                        'whip': 1.1 + i * 0.1 + (2023 - season) * 0.05,
                        'k_per_9': 9.0 + i * 0.5 - (2023 - season) * 0.2,
                        'bb_per_9': 2.5 + i * 0.3 + (2023 - season) * 0.1,
                        'hr_per_9': 1.0 + i * 0.2 + (2023 - season) * 0.1,
                        'swstr_pct': 11.0 + i * 0.5 - (2023 - season) * 0.3,
                        'csw_pct': 30.0 + i * 1.0 - (2023 - season) * 0.5,
                        'o_swing_pct': 32.0 + i * 1.0 - (2023 - season) * 0.5,
                        'z_contact_pct': 85.0 - i * 1.0 + (2023 - season) * 0.5,
                        'innings_pitched': 180.0 - i * 10.0 - (2023 - season) * 5.0,
                        'games': 30 - i - (2023 - season),
                        'strikeouts': 180 - i * 10 - (2023 - season) * 5,
                        'walks': 50 + i * 5 + (2023 - season) * 3,
                        'home_runs': 20 + i * 2 + (2023 - season) * 1,
                        'hits': 150 + i * 10 + (2023 - season) * 5,
                        'earned_runs': 60 + i * 5 + (2023 - season) * 3
                    }
                    db.update_pitcher_metrics(metrics)
                
                # 3. 各投手の球種データ
                # 球種コードの登録
                pitch_types = [
                    {'code': 'FF', 'name': 'Four-Seam Fastball', 'description': 'Standard fastball'},
                    {'code': 'SL', 'name': 'Slider', 'description': 'Breaking ball'},
                    {'code': 'CH', 'name': 'Changeup', 'description': 'Off-speed pitch'}
                ]
                db.insert_pitch_types(pitch_types)
                
                # 各球種の使用割合
                for season in [2021, 2022, 2023]:
                    # 各球種ごとに設定
                    pitch_configs = [
                        # (コード, 使用率, 球速, 回転数) の形式
                        ('FF', 50 - i * 5, 95.0 - i * 0.5, 2400 + i * 50), 
                        ('SL', 30 + i * 3, 85.0 - i * 0.5, 2600 + i * 50),
                        ('CH', 20 + i * 2, 83.0 - i * 0.5, 1800 + i * 50)
                    ]
                    
                    for pt_code, pct, velo, spin in pitch_configs:
                        pitch_type_id = db.get_pitch_type_id(pt_code)
                        
                        # 球種トレンドの調整（スライダーは増加傾向、他は減少傾向）
                        trend_factor = 1 if pt_code in ['SL'] else -1
                        
                        usage_data = {
                            'pitcher_id': pitcher_id,
                            'pitch_type_id': pitch_type_id,
                            'season': season,
                            'usage_pct': pct + (season - 2021) * trend_factor,
                            'avg_velocity': velo - (2023 - season) * 0.3,
                            'avg_spin_rate': spin + (season - 2021) * 20,
                            'avg_pfx_x': (2.0 if pt_code == 'FF' else 
                                        -4.0 if pt_code == 'SL' else 
                                        4.0),
                            'avg_pfx_z': (10.0 if pt_code == 'FF' else 
                                        2.0 if pt_code == 'SL' else 
                                        4.0),
                            'whiff_pct': (8.0 if pt_code == 'FF' else 
                                        30.0 if pt_code == 'SL' else 
                                        20.0)
                        }
                        db.update_pitch_usage(usage_data)
            
            yield db
            
        finally:
            # 一時ファイルの削除
            try:
                os.unlink(db_path)
            except:
                pass
    
    @pytest.mark.integration
    def test_dashboard_initialization(self, mock_streamlit, test_db_with_data):
        """ダッシュボードの初期化テスト"""
        # ダッシュボードの初期化
        dashboard = Dashboard(test_db_with_data.db_path)
        
        # Streamlitの設定メソッドが呼ばれたことを確認
        mock_streamlit.set_page_config.assert_called_once()
        mock_streamlit.title.assert_called_once()
        
        # カスタムCSSが適用されたことを確認
        mock_streamlit.markdown.assert_called()
    
    @pytest.mark.integration
    def test_dashboard_pitcher_search(self, mock_streamlit, test_db_with_data):
        """投手検索機能のテスト"""
        # モックを直接上書き
        mock_search = MagicMock()
        
        # ダッシュボードの初期化
        dashboard = Dashboard(test_db_with_data.db_path)
        
        # _pitcher_name_search メソッドを差し替え
        dashboard._pitcher_name_search = mock_search
        
        # ラジオボタンのコールバック機能をモック
        def side_effect_radio(label, options, **kwargs):
            # "投手名検索"を選択したときのコールバック
            if label == "検索方法を選択" and "投手名検索" in options:
                return "投手名検索"
            return options[0]
        
        mock_streamlit.radio.side_effect = side_effect_radio
        
        # サイドバーの作成
        dashboard._create_sidebar()
        
        # 検索メソッドが呼ばれたことを確認
        mock_search.assert_called_once()
    
    @pytest.mark.integration
    def test_pitcher_display(self, mock_streamlit, test_db_with_data):
        """投手データ表示のテスト"""
        # ダッシュボードの初期化
        dashboard = Dashboard(test_db_with_data.db_path)
        
        # セッション状態へのアクセス方法をパッチする
        # モックをsession_stateが属性アクセスを可能にするように再設定
        session_state_mock = MagicMock()
        session_state_mock.selected_pitcher = 1
        session_state_mock.selected_season = 2023
        type(mock_streamlit).session_state = PropertyMock(return_value=session_state_mock)
        
        # AnalyzerのモックとGet_pitcher_summaryの戻り値を設定
        with patch.object(dashboard.analyzer, 'get_pitcher_summary') as mock_get_summary:
            mock_get_summary.return_value = {
                'name': 'Test Pitcher',
                'team': 'NYY',
                'season': 2023,
                'metrics': {
                    'era': 3.45,
                    'fip': 3.56,
                    'whip': 1.21,
                    'k_per_9': 9.5,
                    'bb_per_9': 2.8,
                    'hr_per_9': 1.2,
                    'games': 30,
                    'innings_pitched': 180.2
                },
                'pitch_types': [
                    {
                        'type': 'Four-Seam Fastball',
                        'code': 'FF',
                        'usage_pct': 50.0,
                        'avg_velocity': 95.5,
                        'avg_spin_rate': 2425,
                        'whiff_pct': 10.0
                    },
                    {
                        'type': 'Slider',
                        'code': 'SL',
                        'usage_pct': 30.0,
                        'avg_velocity': 85.5,
                        'avg_spin_rate': 2615,
                        'whiff_pct': 25.0
                    }
                ]
            }
            
            # 実際のメソッドの内部をパッチしてsession_stateアクセスエラーを回避
            with patch.object(dashboard, '_display_overview_tab'):
                with patch.object(dashboard, '_display_pitch_types_tab'):
                    with patch.object(dashboard, '_display_time_series_tab'):
                        with patch.object(dashboard, '_display_detailed_metrics_tab'):
                            # 投手ダッシュボードの表示
                            dashboard._display_pitcher_dashboard()
                            
                            # サマリー取得が呼ばれたことを確認
                            mock_get_summary.assert_called_once()
        
        # ヘッダーが表示されたことを確認
        mock_streamlit.header.assert_called()
        mock_streamlit.subheader.assert_called()
        
        # タブが作成されたことを確認
        mock_streamlit.tabs.assert_called_once()
    
    @pytest.mark.integration
    def test_overview_tab(self, mock_streamlit, test_db_with_data):
        """概要タブの表示テスト"""
        # ダッシュボードの初期化
        dashboard = Dashboard(test_db_with_data.db_path)
        
        # テスト用のピッチャーサマリーを作成
        pitcher_summary = {
            'name': 'Test Pitcher',
            'team': 'NYY',
            'season': 2023,
            'metrics': {
                'era': 3.45,
                'fip': 3.56,
                'whip': 1.21,
                'k_per_9': 9.5,
                'bb_per_9': 2.8,
                'hr_per_9': 1.2,
                'games': 30,
                'innings_pitched': 180.2
            },
            'pitch_types': [
                {
                    'type': 'Four-Seam Fastball',
                    'code': 'FF',
                    'usage_pct': 50.0,
                    'avg_velocity': 95.5,
                    'avg_spin_rate': 2425,
                    'whiff_pct': 10.0
                },
                {
                    'type': 'Slider',
                    'code': 'SL',
                    'usage_pct': 30.0,
                    'avg_velocity': 85.5,
                    'avg_spin_rate': 2615,
                    'whiff_pct': 25.0
                }
            ]
        }
        
        # 概要タブの表示
        with patch.object(dashboard, '_create_pitch_usage_chart') as mock_chart:
            dashboard._display_overview_tab(pitcher_summary)
            
            # 円グラフが作成されたことを確認
            mock_chart.assert_called_once()
            
        # 指標が表示されたことを確認
        mock_streamlit.metric.assert_called()
        mock_streamlit.dataframe.assert_called()
    
    @pytest.mark.integration
    def test_pitch_types_tab(self, mock_streamlit, test_db_with_data):
        """球種分析タブの表示テスト"""
        # ダッシュボードの初期化
        dashboard = Dashboard(test_db_with_data.db_path)
        
        # モックの作成
        mock_comparison = MagicMock()
        mock_movement = MagicMock()
        
        # モックを明示的に設定
        dashboard._create_pitch_comparison_chart = mock_comparison
        dashboard._create_movement_chart = mock_movement
        
        # 球種選択のモック
        mock_streamlit.selectbox.return_value = "平均球速 (mph)"
        
        # テスト用のピッチャーサマリーを作成
        pitcher_summary = {
            'name': 'Test Pitcher',
            'team': 'NYY',
            'season': 2023,
            'metrics': {},
            'pitch_types': [
                {
                    'type': 'Four-Seam Fastball',
                    'code': 'FF',
                    'usage_pct': 50.0,
                    'avg_velocity': 95.5,
                    'avg_spin_rate': 2425,
                    'avg_pfx_x': 2.5,
                    'avg_pfx_z': 8.5,
                    'whiff_pct': 10.0
                },
                {
                    'type': 'Slider',
                    'code': 'SL',
                    'usage_pct': 30.0,
                    'avg_velocity': 85.5,
                    'avg_spin_rate': 2615,
                    'avg_pfx_x': -2.5,
                    'avg_pfx_z': 2.5,
                    'whiff_pct': 25.0
                }
            ]
        }
        
        # 球種分析タブの表示
        dashboard._display_pitch_types_tab(pitcher_summary)
        
        # 比較チャートが作成されたことを確認
        mock_comparison.assert_called_once()
        
        # 変化量チャートが作成されたことを確認
        mock_movement.assert_called_once()
        
        # セレクトボックスが表示されたことを確認
        mock_streamlit.selectbox.assert_called()
        mock_streamlit.dataframe.assert_called()