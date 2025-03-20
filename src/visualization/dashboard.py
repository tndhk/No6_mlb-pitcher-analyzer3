# src/visualization/dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import logging  # これを追加
from typing import Dict, List, Any, Optional

from src.data_storage.database import Database
from src.data_analysis.pitcher_analyzer import PitcherAnalyzer

def safe_rerun():
    """
    StreamlitのバージョンによってrerunメソッドのAPIが異なるため、
    複数のバージョンに対応する安全なrerun方法を提供する
    """
    try:
        # 新しいバージョン（v1.23.0以降）
        st.rerun()
    except AttributeError:
        st.experimental_rerun()


class Dashboard:
    """
    Streamlitダッシュボードを管理するクラス
    """
    
    def __init__(self, db_path: str):
        """
        Dashboardの初期化
        
        Args:
            db_path: SQLiteデータベースファイルのパス
        """
            # ロガーの設定を追加
        self.logger = logging.getLogger(__name__)
        
        self.db = Database(db_path)
        self.analyzer = PitcherAnalyzer(self.db)
        
        # ページ設定
        st.set_page_config(
            page_title="MLB Pitcher Dashboard",
            page_icon="⚾",
            layout="wide",
            initial_sidebar_state="expanded",
        )
        
        # アプリケーションタイトル
        st.title("MLB Pitcher Performance Dashboard")
        
        # カスタムCSS
        self._set_custom_css()
        
    def _set_custom_css(self):
        """
        カスタムCSSの適用
        """
        st.markdown("""
        <style>
        .main {
            background-color: #0E1117;
            color: white;
        }
        .sidebar .sidebar-content {
            background-color: #0E1117;
            color: white;
        }
        h1, h2, h3 {
            color: white;
        }
        .stMetric {
            background-color: #262730;
            padding: 15px;
            border-radius: 5px;
        }
        .metric-title {
            font-size: 1rem;
            font-weight: 600;
            color: #9DA5B4;
        }
        .metric-value {
            font-size: 2rem;
            font-weight: 700;
            color: white;
        }
        .metric-delta {
            font-size: 0.9rem;
        }
        </style>
        """, unsafe_allow_html=True)
        
    def run(self):
        """
        ダッシュボードの実行
        """
        # サイドバーに検索機能を配置
        self._create_sidebar()
        
        # メインコンテンツ
        if 'selected_pitcher' in st.session_state:
            self._display_pitcher_dashboard()
        else:
            self._display_welcome_page()
            
    def _create_sidebar(self):
        """
        サイドバーの作成
        """
        with st.sidebar:
            st.header("検索オプション")
            
            # 検索方法の選択
            search_method = st.radio(
                "検索方法を選択",
                ["投手名検索", "チーム検索", "ブラウズ全選手"]
            )
            
            if search_method == "投手名検索":
                self._pitcher_name_search()
            elif search_method == "チーム検索":
                self._team_search()
            else:
                self._browse_all_pitchers()
                
            # 期間選択
            if 'selected_pitcher' in st.session_state:
                st.subheader("期間選択")
                
                # 有効なシーズンを取得
                available_seasons = self._get_available_seasons()
                
                if available_seasons:
                    if 'selected_season' not in st.session_state or st.session_state.selected_season not in available_seasons:
                        st.session_state.selected_season = max(available_seasons)
                        
                    st.selectbox(
                        "シーズン選択",
                        options=available_seasons,
                        index=available_seasons.index(st.session_state.selected_season),
                        key="selected_season"
                    )
                else:
                    st.warning("選択した投手のデータがありません")
                    
    def _pitcher_name_search(self):
        """
        投手名による検索
        """
        search_term = st.text_input("投手名を入力", key="pitcher_search")
        
        if search_term:
            results = self.db.search_pitchers(search_term)
            
            if results:
                # 検索結果をDataFrameに変換
                df = pd.DataFrame(results)
                df = df[['id', 'name', 'team']]
                df.columns = ['ID', '投手名', 'チーム']
                
                # 結果の表示
                st.dataframe(df, hide_index=True)
                
                # 選手選択
                pitcher_ids = df['ID'].tolist()
                pitcher_names = df['投手名'].tolist()
                selected_index = st.selectbox(
                    "選手を選択",
                    range(len(pitcher_names)),
                    format_func=lambda i: pitcher_names[i]
                )
                
                if st.button("選手を表示"):
                    st.session_state.selected_pitcher = pitcher_ids[selected_index]
                    safe_rerun() 
            else:
                st.info("該当する投手が見つかりませんでした")
                
    def _team_search(self):
        """
        チームによる検索
        """
        # チーム一覧の取得
        teams = self.db.get_all_teams()
        
        if teams:
            selected_team = st.selectbox("チームを選択", teams)
            
            # 選択したチームの投手一覧
            pitchers = self.db.get_pitchers_by_team(selected_team)
            
            if pitchers:
                # DataFrameに変換
                df = pd.DataFrame(pitchers)
                df = df[['id', 'name']]
                df.columns = ['ID', '投手名']
                
                # 投手選択
                pitcher_ids = df['ID'].tolist()
                pitcher_names = df['投手名'].tolist()
                selected_index = st.selectbox(
                    "投手を選択",
                    range(len(pitcher_names)),
                    format_func=lambda i: pitcher_names[i]
                )
                
                if st.button("選手を表示"):
                    st.session_state.selected_pitcher = pitcher_ids[selected_index]
                    safe_rerun() 
            else:
                st.info(f"{selected_team}の投手データが見つかりませんでした")
        else:
            st.info("チームデータが見つかりませんでした")
            
    def _browse_all_pitchers(self):
        """
        全選手のブラウズ
        """
        # 全選手の取得
        all_pitchers = self.db.get_all_pitchers()
        
        if all_pitchers:
            # DataFrameに変換
            df = pd.DataFrame(all_pitchers)
            df = df[['id', 'name', 'team']]
            df.columns = ['ID', '投手名', 'チーム']
            
            # チームでフィルタリング
            teams = ['All'] + sorted(df['チーム'].dropna().unique().tolist())
            selected_team = st.selectbox("チームでフィルタリング", teams)
            
            if selected_team != 'All':
                filtered_df = df[df['チーム'] == selected_team]
            else:
                filtered_df = df
                
            # 投手選択
            pitcher_ids = filtered_df['ID'].tolist()
            pitcher_names = filtered_df['投手名'].tolist()
            
            if pitcher_ids:
                selected_index = st.selectbox(
                    "投手を選択",
                    range(len(pitcher_names)),
                    format_func=lambda i: pitcher_names[i]
                )
                
                if st.button("選手を表示"):
                    st.session_state.selected_pitcher = pitcher_ids[selected_index]
                    safe_rerun() 
            else:
                st.info("フィルタ条件に一致する投手がいません")
        else:
            st.info("投手データが見つかりませんでした")
            
    def _get_available_seasons(self) -> List[int]:
        """
        選択した投手の利用可能なシーズンを取得
        
        Returns:
            List[int]: 利用可能なシーズンのリスト
        """
        if 'selected_pitcher' not in st.session_state:
            return []
            
        pitcher_id = st.session_state.selected_pitcher
        metrics = self.db.get_pitcher_metrics(pitcher_id)
        
        if not metrics:
            return []
            
        seasons = [m['season'] for m in metrics if m['season'] is not None]
        return sorted(seasons, reverse=True)
        
    def _display_welcome_page(self):
        """
        ウェルカムページの表示
        """
        st.markdown("""
        ## ようこそ MLB Pitcher Performance Dashboard へ
        
        このダッシュボードでは、MLB投手のパフォーマンスを様々な指標から分析できます。
        
        ### 主な機能
        
        - **投手の基本指標表示**: ERA, FIP, WHIP, K/9, BB/9, HR/9などの基本指標を確認できます
        - **球種分析**: 球種ごとの使用割合、平均球速、回転数、変化量などを視覚化します
        - **スイング指標**: SwStr%, CSW%, O-Swing%, Z-Contact%などの詳細な投球指標を分析します
        - **時系列分析**: 各指標の推移を時系列グラフで確認できます
        
        ### 使い方
        
        1. 左側のサイドバーから投手を検索するか、チームから選択します
        2. 投手を選択すると、詳細な分析データが表示されます
        3. 期間選択で特定のシーズンのデータを表示できます
        
        サイドバーから投手を選択して始めましょう！
        """)
        
        # サンプルの投手を選択するボタン（開発中の場合）
        if st.button("サンプル投手を表示"):
            # データベースから最初の投手を選択
            all_pitchers = self.db.get_all_pitchers()
            if all_pitchers:
                st.session_state.selected_pitcher = all_pitchers[0]['id']
                safe_rerun() 
            else:
                st.warning("データベースに投手データがありません")
                
# src/visualization/dashboard.py の _display_pitcher_dashboard メソッドにデバッグログを追加
    def _display_pitcher_dashboard(self):
        """
        投手ダッシュボードの表示
        """
        pitcher_id = st.session_state.selected_pitcher
        season = st.session_state.selected_season if 'selected_season' in st.session_state else None
        
        # デバッグログを追加
        self.logger.info(f"Displaying dashboard for pitcher {pitcher_id}, selected season: {season}")
        
        # 投手データの取得
        pitcher_summary = self.analyzer.get_pitcher_summary(pitcher_id, season)
        
        # 取得したデータのログ
        self.logger.info(f"Retrieved pitcher summary: name={pitcher_summary.get('name')}, season={pitcher_summary.get('season')}")
        self.logger.info(f"Retrieved {len(pitcher_summary.get('pitch_types', []))} pitch types")
        
        if not pitcher_summary:
            st.error("投手データの取得に失敗しました")
            return
            
        # ヘッダー情報の表示
        st.header(f"{pitcher_summary['name']} ({pitcher_summary['team']})")
        st.subheader(f"{pitcher_summary['season']}シーズン")
        
        # タブの作成
        tabs = st.tabs(["概要", "球種分析", "時系列分析", "詳細指標"])
        
        # 概要タブ
        with tabs[0]:
            self._display_overview_tab(pitcher_summary)
            
        # 球種分析タブ
        with tabs[1]:
            self._display_pitch_types_tab(pitcher_summary)
            
        # 時系列分析タブ
        with tabs[2]:
            self._display_time_series_tab(pitcher_id)
            
        # 詳細指標タブ
        with tabs[3]:
            self._display_detailed_metrics_tab(pitcher_summary)
            
    # src/visualization/dashboard.py の _display_overview_tab メソッドを修正
    def _display_overview_tab(self, pitcher_summary: Dict[str, Any]):
        """
        概要タブの表示
        
        Args:
            pitcher_summary: 投手サマリー情報
        """
        metrics = pitcher_summary['metrics']
        
        # 2カラムレイアウト
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("基本指標")
            
            # 安全な値変換関数を定義
            def safe_value(value, format_str=None):
                """bytes型や他の型を安全に表示可能な値に変換"""
                if value is None:
                    return 'N/A'
                if isinstance(value, bytes):
                    try:
                        # bytesを整数に変換
                        return int.from_bytes(value, byteorder='little')
                    except:
                        # 変換できない場合は文字列に
                        return str(value)
                # 数値の場合はフォーマット
                if format_str and isinstance(value, (int, float)):
                    return format_str.format(value)
                return value
            
            # 主要指標を表示（bytes型を考慮）
            metrics_data = {
                "ERA": safe_value(metrics.get('era'), "{:.2f}"),
                "FIP": safe_value(metrics.get('fip'), "{:.2f}"),
                "WHIP": safe_value(metrics.get('whip'), "{:.2f}"),
                "K/9": safe_value(metrics.get('k_per_9'), "{:.2f}"),
                "BB/9": safe_value(metrics.get('bb_per_9'), "{:.2f}"),
                "HR/9": safe_value(metrics.get('hr_per_9'), "{:.2f}")
            }
            
            # 指標テーブルの作成
            df = pd.DataFrame([metrics_data])
            st.dataframe(df, hide_index=True)
            
            # 試合数・イニング
            st.metric("登板試合数", safe_value(metrics.get('games')))
            st.metric("投球イニング", safe_value(metrics.get('innings_pitched'), "{:.1f}"))
            
        with col2:
            st.subheader("球種構成")
            
            # 球種円グラフの作成
            if pitcher_summary['pitch_types']:
                self._create_pitch_usage_chart(pitcher_summary['pitch_types'])
            else:
                st.info("球種データがありません")

    def _create_pitch_usage_chart(self, pitch_types: List[Dict[str, Any]]):
        """
        球種使用割合の円グラフを作成
        
        Args:
            pitch_types: 球種データのリスト
        """
        # DataFrameの作成
        df = pd.DataFrame(pitch_types)
        
        # 使用割合でソート
        df = df.sort_values('usage_pct', ascending=False)
        
        # 円グラフの作成
        fig = px.pie(
            df,
            values='usage_pct',
            names='type',
            title='球種使用割合',
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        
        # レイアウトの調整
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(
            legend_title_text='球種',
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
            margin=dict(l=20, r=20, t=40, b=20),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    def _display_pitch_types_tab(self, pitcher_summary: Dict[str, Any]):
        """
        球種分析タブの表示
        
        Args:
            pitcher_summary: 投手サマリー情報
        """
        pitch_types = pitcher_summary['pitch_types']
        
        if not pitch_types:
            st.info("球種データがありません")
            return
            
        st.subheader("球種ごとの詳細分析")
        
        # DataFrameの作成
        df = pd.DataFrame(pitch_types)
        
        # 表形式で表示
        st.dataframe(
            df[['type', 'usage_pct', 'avg_velocity', 'avg_spin_rate', 'whiff_pct']].rename(
                columns={
                    'type': '球種',
                    'usage_pct': '使用割合 (%)',
                    'avg_velocity': '平均球速 (mph)',
                    'avg_spin_rate': '平均回転数 (rpm)',
                    'whiff_pct': '空振り率 (%)'
                }
            ).sort_values('使用割合 (%)', ascending=False),
            hide_index=True,
            use_container_width=True
        )
        
        # 球種ごとの指標を可視化
        st.subheader("球種比較")
        
        # 比較する指標の選択
        metric_option = st.selectbox(
            "比較する指標を選択",
            [
                "平均球速 (mph)",
                "平均回転数 (rpm)",
                "空振り率 (%)",
                "使用割合 (%)"
            ]
        )
        
        # 指標名とデータカラムのマッピング
        metric_mapping = {
            "平均球速 (mph)": "avg_velocity",
            "平均回転数 (rpm)": "avg_spin_rate",
            "空振り率 (%)": "whiff_pct",
            "使用割合 (%)": "usage_pct"
        }
        
        selected_metric = metric_mapping.get(metric_option)
        
        if selected_metric:
            self._create_pitch_comparison_chart(df, selected_metric, metric_option)
            
        # 変化量の表示
        st.subheader("球種ごとの変化量")
        self._create_movement_chart(df)
        
    def _create_pitch_comparison_chart(self, df: pd.DataFrame, metric: str, metric_label: str):
        """
        球種ごとの指標比較棒グラフを作成
        
        Args:
            df: 球種データのDataFrame
            metric: 比較する指標のカラム名
            metric_label: 指標の表示名
        """
        # 使用割合でソート
        df = df.sort_values('usage_pct', ascending=False)
        
        # 棒グラフの作成
        fig = px.bar(
            df,
            x='type',
            y=metric,
            color='type',
            title=f'球種ごとの{metric_label}',
            labels={'type': '球種', metric: metric_label}
        )
        
        # レイアウトの調整
        fig.update_layout(
            xaxis_title='球種',
            yaxis_title=metric_label,
            showlegend=False,
            margin=dict(l=20, r=20, t=40, b=20),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    def _create_movement_chart(self, df: pd.DataFrame):
        """
        球種ごとの変化量を散布図で表示
        
        Args:
            df: 球種データのDataFrame
        """
        # 水平・垂直変化量のデータがあるか確認
        if 'avg_pfx_x' not in df.columns or 'avg_pfx_z' not in df.columns:
            st.info("変化量データがありません")
            return
            
        # 変化量データの前処理
        df = df.dropna(subset=['avg_pfx_x', 'avg_pfx_z'])
        
        if df.empty:
            st.info("変化量データがありません")
            return
            
        # 散布図の作成
        fig = px.scatter(
            df,
            x='avg_pfx_x',
            y='avg_pfx_z',
            size='usage_pct',
            color='type',
            text='type',
            title='球種ごとの変化量',
            labels={
                'avg_pfx_x': '水平変化量 (インチ)',
                'avg_pfx_z': '垂直変化量 (インチ)',
                'type': '球種',
                'usage_pct': '使用割合 (%)'
            },
            size_max=50
        )
        
        # 原点を表示
        fig.add_shape(
            type="line",
            x0=0, y0=-10, x1=0, y1=10,
            line=dict(color="white", width=1, dash="dash")
        )
        fig.add_shape(
            type="line",
            x0=-10, y0=0, x1=10, y1=0,
            line=dict(color="white", width=1, dash="dash")
        )
        
        # レイアウトの調整
        fig.update_traces(
            textposition='top center',
            marker=dict(line=dict(width=1, color='DarkSlateGrey'))
        )
        fig.update_layout(
            xaxis=dict(range=[-20, 20], zeroline=False),
            yaxis=dict(range=[-20, 20], zeroline=False),
            margin=dict(l=20, r=20, t=40, b=20),
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    def _display_time_series_tab(self, pitcher_id: int):
        """
        時系列分析タブの表示
        
        Args:
            pitcher_id: 投手ID
        """
        st.subheader("指標の時系列推移")
        
        # 全シーズンの成績を取得
        metrics = self.db.get_pitcher_metrics(pitcher_id)
        
        if not metrics:
            st.info("時系列データがありません")
            return
            
        # DataFrameに変換
        df = pd.DataFrame(metrics)
        
        # シーズンで並べ替え
        df = df.sort_values('season')
        
        # 表示する指標の選択
        metric_options = {
            "ERA": "era",
            "FIP": "fip",
            "WHIP": "whip",
            "K/9": "k_per_9",
            "BB/9": "bb_per_9",
            "HR/9": "hr_per_9",
            "空振り率 (%)": "swstr_pct",
            "CSW (%)": "csw_pct",
            "ゾーン外スイング率 (%)": "o_swing_pct",
            "ゾーン内コンタクト率 (%)": "z_contact_pct"
        }
        
        selected_metrics = st.multiselect(
            "表示する指標を選択",
            list(metric_options.keys()),
            default=["ERA", "FIP", "WHIP"]
        )
        
        if not selected_metrics:
            st.info("指標を選択してください")
            return
            
        # 時系列グラフの作成
        fig = go.Figure()
        
        for metric_label in selected_metrics:
            metric_column = metric_options[metric_label]
            
            if metric_column in df.columns:
                # 欠損値を除外
                metric_data = df.dropna(subset=[metric_column])
                
                if not metric_data.empty:
                    fig.add_trace(
                        go.Scatter(
                            x=metric_data['season'],
                            y=metric_data[metric_column],
                            mode='lines+markers',
                            name=metric_label,
                            connectgaps=True
                        )
                    )
                    
        # レイアウトの調整
        fig.update_layout(
            title="シーズン別指標推移",
            xaxis_title="シーズン",
            yaxis_title="値",
            legend_title="指標",
            margin=dict(l=20, r=20, t=40, b=20),
            height=500,
            hovermode="x unified"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # 球種使用率の推移を追加（新機能）
        st.subheader("球種使用率の推移")
        
        # 全シーズンの球種使用割合データを取得（修正箇所）
        all_pitch_usage = self.db.get_pitch_usage_data(pitcher_id, all_seasons=True)
        
        if not all_pitch_usage:
            st.info("球種使用率データがありません")
            return
        
        # シーズンごとにデータを整理
        seasons = sorted(list(set([p['season'] for p in all_pitch_usage])))
        
        if len(seasons) < 2:
            st.info("球種使用率の推移を表示するには少なくとも2シーズン以上のデータが必要です")
            return
        
        # 主要球種のみを表示（使用率が一定以上のもの）
        main_pitches = {}
        for pitch in all_pitch_usage:
            pitch_code = pitch['code']
            if pitch_code not in main_pitches and pitch['usage_pct'] >= 5.0:
                main_pitches[pitch_code] = pitch['name']
        
        # 球種選択
        selected_pitches = st.multiselect(
            "表示する球種を選択",
            list(main_pitches.keys()),
            default=list(main_pitches.keys())[:3],  # デフォルトで最初の3つを選択
            format_func=lambda x: f"{x} ({main_pitches[x]})"
        )
        
        if not selected_pitches:
            st.info("球種を選択してください")
            return
        
        # 球種使用率の時系列データを作成
        pitch_usage_data = []
        for season in seasons:
            season_pitches = [p for p in all_pitch_usage if p['season'] == season]
            for pitch in season_pitches:
                if pitch['code'] in selected_pitches:
                    pitch_usage_data.append({
                        'season': season,
                        'pitch_code': pitch['code'],
                        'pitch_name': pitch['name'],
                        'usage_pct': pitch['usage_pct']
                    })
        
        # DataFrameに変換
        df_pitch_usage = pd.DataFrame(pitch_usage_data)
        
        # 球種使用率の推移グラフを作成
        fig_pitch_usage = go.Figure()
        
        for pitch_code in selected_pitches:
            pitch_data = df_pitch_usage[df_pitch_usage['pitch_code'] == pitch_code]
            if not pitch_data.empty:
                fig_pitch_usage.add_trace(
                    go.Scatter(
                        x=pitch_data['season'],
                        y=pitch_data['usage_pct'],
                        mode='lines+markers',
                        name=f"{pitch_code} ({main_pitches[pitch_code]})",
                        connectgaps=True
                    )
                )
        
        # レイアウトの調整
        fig_pitch_usage.update_layout(
            title="シーズン別球種使用率推移",
            xaxis_title="シーズン",
            yaxis_title="使用率 (%)",
            legend_title="球種",
            margin=dict(l=20, r=20, t=40, b=20),
            height=500,
            hovermode="x unified"
        )
        
        st.plotly_chart(fig_pitch_usage, use_container_width=True)
        
        # シーズン比較セクション
        st.subheader("シーズン比較")
        
        available_seasons = sorted(df['season'].unique())
        
        if len(available_seasons) >= 2:
            col1, col2 = st.columns(2)
            
            with col1:
                season1 = st.selectbox("比較シーズン1", available_seasons, index=len(available_seasons) - 2)
                
            with col2:
                season2 = st.selectbox("比較シーズン2", available_seasons, index=len(available_seasons) - 1)
                
            if season1 != season2:
                # シーズン比較の実行
                comparison = self.analyzer.compare_seasons(pitcher_id, season1, season2)
                
                if comparison:
                    self._display_season_comparison(comparison)
                else:
                    st.info("比較データを取得できませんでした")
            else:
                st.warning("異なるシーズンを選択してください")
        else:
            st.info("比較には2シーズン以上のデータが必要です")

    def _display_season_comparison(self, comparison: Dict[str, Any]):
        """
        シーズン比較結果の表示
        
        Args:
            comparison: シーズン比較データ
        """
        season1 = comparison['season1']
        season2 = comparison['season2']
        
        st.subheader(f"{season1}シーズン vs {season2}シーズン")
        
        # 主要指標の比較
        metrics_diff = comparison['metrics_diff']
        metrics1 = comparison['metrics_season1']
        metrics2 = comparison['metrics_season2']
        
        # 指標比較テーブルの作成
        metrics_compare = []
        
        for metric, label in [
            ('era', 'ERA'), 
            ('fip', 'FIP'), 
            ('whip', 'WHIP'),
            ('k_per_9', 'K/9'), 
            ('bb_per_9', 'BB/9'), 
            ('hr_per_9', 'HR/9'),
            ('swstr_pct', 'SwStr%'), 
            ('csw_pct', 'CSW%')
        ]:
            if metric in metrics1 and metric in metrics2:
                val1 = metrics1.get(metric)
                val2 = metrics2.get(metric)
                diff = metrics_diff.get(metric)
                
                # 値が存在する場合のみ追加
                if val1 is not None and val2 is not None and diff is not None:
                    # 指標によって増減の評価を変える
                    if metric in ['era', 'fip', 'whip', 'bb_per_9', 'hr_per_9']:
                        is_improvement = diff < 0  # 低いほうが良い指標
                    else:
                        is_improvement = diff > 0  # 高いほうが良い指標
                        
                    metrics_compare.append({
                        '指標': label,
                        f'{season1}': round(val1, 2) if val1 is not None else 'N/A',
                        f'{season2}': round(val2, 2) if val2 is not None else 'N/A',
                        '変化': round(diff, 2) if diff is not None else 'N/A',
                        '変化率(%)': round(metrics_diff.get(f'{metric}_pct', 0), 1) if metrics_diff.get(f'{metric}_pct') is not None else 'N/A',
                        '評価': '改善' if is_improvement else '悪化'
                    })
                    
        if metrics_compare:
            df_metrics = pd.DataFrame(metrics_compare)
            st.dataframe(df_metrics, hide_index=True, use_container_width=True)
            
            # 球種使用率の変化
            st.subheader("球種使用率の変化")
            pitch_diff = comparison.get('pitch_usage_diff', [])
            
            if pitch_diff:
                pitch_diff_data = []
                
                for pitch in pitch_diff:
                    usage1 = pitch.get('usage_season1', 0)
                    usage2 = pitch.get('usage_season2', 0)
                    
                    # 表示用データを作成
                    pitch_diff_data.append({
                        '球種': pitch.get('name', pitch.get('code', '不明')),
                        f'{season1} (%)': round(usage1, 1) if usage1 is not None else 'N/A',
                        f'{season2} (%)': round(usage2, 1) if usage2 is not None else 'N/A',
                        '変化 (%)': round(pitch.get('usage_diff', 0), 1) if pitch.get('usage_diff') is not None else 'N/A'
                    })
                    
                # 使用率の高い順にソート
                pitch_diff_data = sorted(pitch_diff_data, key=lambda x: x.get(f'{season2} (%)', 0) if x.get(f'{season2} (%)') != 'N/A' else 0, reverse=True)
                
                # DataFrameに変換して表示
                df_pitch = pd.DataFrame(pitch_diff_data)
                st.dataframe(df_pitch, hide_index=True, use_container_width=True)
                
                # 球種使用率変化のグラフ
                fig = go.Figure()
                
                for pitch in pitch_diff_data:
                    fig.add_trace(
                        go.Bar(
                            name=pitch['球種'],
                            x=['変化 (%)'],
                            y=[pitch['変化 (%)']] if pitch['変化 (%)'] != 'N/A' else [0],
                            text=[f"{pitch['球種']}: {pitch['変化 (%)']}%"] if pitch['変化 (%)'] != 'N/A' else [f"{pitch['球種']}: N/A"]
                        )
                    )
                    
                fig.update_layout(
                    title=f"球種使用率の変化 ({season1}→{season2})",
                    barmode='relative',
                    margin=dict(l=20, r=20, t=40, b=20),
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("球種使用率の比較データがありません")
        else:
            st.info("比較データがありません")
            
    def _display_detailed_metrics_tab(self, pitcher_summary: Dict[str, Any]):
        """
        詳細指標タブの表示
        
        Args:
            pitcher_summary: 投手サマリー情報
        """
        metrics = pitcher_summary['metrics']
        
        st.subheader("詳細指標分析")
        
        # 3カラムレイアウト
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("### スイング指標")
            st.metric("SwStr%", f"{metrics.get('swstr_pct', 'N/A'):.1f}%" if metrics.get('swstr_pct') is not None else "N/A", 
                      help="総投球数に対する空振りの割合")
            st.metric("CSW%", f"{metrics.get('csw_pct', 'N/A'):.1f}%" if metrics.get('csw_pct') is not None else "N/A", 
                      help="Called Strikes + Whiffs の割合")
            st.metric("O-Swing%", f"{metrics.get('o_swing_pct', 'N/A'):.1f}%" if metrics.get('o_swing_pct') is not None else "N/A", 
                      help="ゾーン外の投球に対するスイング率")
            st.metric("Z-Contact%", f"{metrics.get('z_contact_pct', 'N/A'):.1f}%" if metrics.get('z_contact_pct') is not None else "N/A", 
                      help="ゾーン内の投球に対するコンタクト率")
                      
        with col2:
            st.markdown("### 基本成績指標")
            st.metric("ERA", f"{metrics.get('era', 'N/A'):.2f}" if metrics.get('era') is not None else "N/A", 
                      help="Earned Run Average - 9イニングあたりの自責点")
            st.metric("FIP", f"{metrics.get('fip', 'N/A'):.2f}" if metrics.get('fip') is not None else "N/A", 
                      help="Fielding Independent Pitching - 守備の影響を除いた投手指標")
            st.metric("WHIP", f"{metrics.get('whip', 'N/A'):.2f}" if metrics.get('whip') is not None else "N/A", 
                      help="Walks plus Hits per Inning Pitched - イニングあたりの安打と四球の合計")
                      
        with col3:
            st.markdown("### 割合指標")
            st.metric("K/9", f"{metrics.get('k_per_9', 'N/A'):.2f}" if metrics.get('k_per_9') is not None else "N/A", 
                      help="9イニングあたりの奪三振数")
            st.metric("BB/9", f"{metrics.get('bb_per_9', 'N/A'):.2f}" if metrics.get('bb_per_9') is not None else "N/A", 
                      help="9イニングあたりの四球数")
            st.metric("HR/9", f"{metrics.get('hr_per_9', 'N/A'):.2f}" if metrics.get('hr_per_9') is not None else "N/A", 
                      help="9イニングあたりの被本塁打数")
                      
        # 球種詳細
        st.subheader("球種別詳細指標")
        
        pitch_types = pitcher_summary['pitch_types']
        
        if pitch_types:
            # 球種の選択
            pitch_options = [p['type'] for p in pitch_types]
            selected_pitch = st.selectbox("球種を選択", pitch_options)
            
            # 選択した球種のデータ
            pitch_data = next((p for p in pitch_types if p['type'] == selected_pitch), None)
            
            if pitch_data:
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("使用割合", f"{pitch_data.get('usage_pct', 'N/A'):.1f}%" if pitch_data.get('usage_pct') is not None else "N/A")
                    st.metric("空振り率", f"{pitch_data.get('whiff_pct', 'N/A'):.1f}%" if pitch_data.get('whiff_pct') is not None else "N/A")
                    
                with col2:
                    st.metric("平均球速", f"{pitch_data.get('avg_velocity', 'N/A'):.1f} mph" if pitch_data.get('avg_velocity') is not None else "N/A")
                    st.metric("平均回転数", f"{pitch_data.get('avg_spin_rate', 'N/A'):.0f} rpm" if pitch_data.get('avg_spin_rate') is not None else "N/A")
                    
                with col3:
                    st.metric("水平変化量", f"{pitch_data.get('avg_pfx_x', 'N/A'):.1f} inch" if pitch_data.get('avg_pfx_x') is not None else "N/A")
                    st.metric("垂直変化量", f"{pitch_data.get('avg_pfx_z', 'N/A'):.1f} inch" if pitch_data.get('avg_pfx_z') is not None else "N/A")
        else:
            st.info("球種データがありません")