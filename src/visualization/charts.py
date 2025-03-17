# src/visualization/charts.py
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional

class ChartGenerator:
    """
    グラフを生成するユーティリティクラス
    """
    
    @staticmethod
    def create_pitch_usage_pie(pitch_types: List[Dict[str, Any]], title: str = "球種使用割合"):
        """
        球種使用割合の円グラフを作成
        
        Args:
            pitch_types: 球種データのリスト
            title: グラフタイトル
            
        Returns:
            Figure: Plotly Figure オブジェクト
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
            title=title,
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
        
        return fig
        
    @staticmethod
    def create_pitch_comparison_bar(data: pd.DataFrame, metric: str, metric_label: str, 
                                  title: str = None):
        """
        球種ごとの指標比較棒グラフを作成
        
        Args:
            data: 球種データ
            metric: 比較する指標のカラム名
            metric_label: 指標の表示名
            title: グラフタイトル
            
        Returns:
            Figure: Plotly Figure オブジェクト
        """
        # 使用割合でソート
        df = data.sort_values('usage_pct', ascending=False)
        
        # タイトル設定
        if title is None:
            title = f'球種ごとの{metric_label}'
            
        # 棒グラフの作成
        fig = px.bar(
            df,
            x='type',
            y=metric,
            color='type',
            title=title,
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
        
        return fig
        
    @staticmethod
    def create_movement_scatter(data: pd.DataFrame, title: str = "球種ごとの変化量"):
        """
        球種ごとの変化量散布図を作成
        
        Args:
            data: 球種データ
            title: グラフタイトル
            
        Returns:
            Figure: Plotly Figure オブジェクト
        """
        # 変化量データの前処理
        df = data.dropna(subset=['avg_pfx_x', 'avg_pfx_z'])
        
        if df.empty:
            return None
            
        # 散布図の作成
        fig = px.scatter(
            df,
            x='avg_pfx_x',
            y='avg_pfx_z',
            size='usage_pct',
            color='type',
            text='type',
            title=title,
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
        
        return fig
        
    @staticmethod
    def create_metrics_time_series(data: pd.DataFrame, metrics: List[str], 
                                 metric_labels: List[str], x_column: str = 'season',
                                 title: str = "指標の時系列推移"):
        """
        指標の時系列グラフを作成
        
        Args:
            data: 時系列データ
            metrics: 表示する指標のカラム名リスト
            metric_labels: 指標の表示名リスト
            x_column: X軸のカラム名
            title: グラフタイトル
            
        Returns:
            Figure: Plotly Figure オブジェクト
        """
        # 時系列グラフの作成
        fig = go.Figure()
        
        for i, metric in enumerate(metrics):
            label = metric_labels[i] if i < len(metric_labels) else metric
            
            if metric in data.columns:
                # 欠損値を除外
                metric_data = data.dropna(subset=[metric])
                
                if not metric_data.empty:
                    fig.add_trace(
                        go.Scatter(
                            x=metric_data[x_column],
                            y=metric_data[metric],
                            mode='lines+markers',
                            name=label,
                            connectgaps=True
                        )
                    )
                    
        # レイアウトの調整
        fig.update_layout(
            title=title,
            xaxis_title=x_column.capitalize(),
            yaxis_title="値",
            legend_title="指標",
            margin=dict(l=20, r=20, t=40, b=20),
            height=500,
            hovermode="x unified"
        )
        
        return fig
        
    @staticmethod
    def create_zone_heatmap(zone_data: pd.DataFrame, value_column: str, 
                          title: str = "ゾーン分析"):
        """
        ストライクゾーンのヒートマップを作成
        
        Args:
            zone_data: ゾーンごとのデータ
            value_column: ヒートマップで表示する値のカラム名
            title: グラフタイトル
            
        Returns:
            Figure: Plotly Figure オブジェクト
        """
        # ゾーンデータを3x3のグリッドに変換
        zone_matrix = np.zeros((3, 3))
        
        for _, row in zone_data.iterrows():
            zone = row.get('zone')
            value = row.get(value_column)
            
            if zone is None or value is None:
                continue
                
            # ゾーン番号からグリッド位置への変換
            # ゾーン番号は以下のように配置されている:
            # 1 2 3
            # 4 5 6
            # 7 8 9
            row_idx = (zone - 1) // 3
            col_idx = (zone - 1) % 3
            
            zone_matrix[row_idx, col_idx] = value
            
        # ヒートマップの作成
        fig = go.Figure(data=go.Heatmap(
            z=zone_matrix,
            colorscale='Viridis',
            showscale=True,
            text=[[f'Zone {i*3+j+1}<br>{zone_matrix[i,j]:.2f}' for j in range(3)] for i in range(3)]
        ))
        
        # ストライクゾーンの枠を追加
        fig.add_shape(
            type="rect",
            x0=-0.5, y0=-0.5, x1=2.5, y1=2.5,
            line=dict(color="white", width=2)
        )
        
        # レイアウトの調整
        fig.update_layout(
            title=title,
            xaxis=dict(
                showticklabels=False,
                scaleanchor="y",
                scaleratio=1
            ),
            yaxis=dict(
                showticklabels=False,
                autorange="reversed"
            ),
            margin=dict(l=20, r=20, t=40, b=20),
            height=400,
            width=400
        )
        
        return fig
        
    @staticmethod
    def create_season_comparison_bar(comparison_data: Dict[str, Any], metrics: List[str], 
                                   metric_labels: List[str], title: str = "シーズン比較"):
        """
        2シーズン間の比較棒グラフを作成
        
        Args:
            comparison_data: シーズン比較データ
            metrics: 比較する指標のカラム名リスト
            metric_labels: 指標の表示名リスト
            title: グラフタイトル
            
        Returns:
            Figure: Plotly Figure オブジェクト
        """
        season1 = comparison_data['season1']
        season2 = comparison_data['season2']
        metrics1 = comparison_data['metrics_season1']
        metrics2 = comparison_data['metrics_season2']
        
        fig = go.Figure()
        
        for i, metric in enumerate(metrics):
            label = metric_labels[i] if i < len(metric_labels) else metric
            
            if metric in metrics1 and metric in metrics2:
                val1 = metrics1.get(metric)
                val2 = metrics2.get(metric)
                
                if val1 is not None and val2 is not None:
                    fig.add_trace(go.Bar(
                        x=[f'{label} ({season1})', f'{label} ({season2})'],
                        y=[val1, val2],
                        name=label
                    ))
                    
        # レイアウトの調整
        fig.update_layout(
            title=title,
            margin=dict(l=20, r=20, t=40, b=20),
            height=400,
            barmode='group'
        )
        
        return fig

# src/app.py
import streamlit as st
import os
import logging
from src.visualization.dashboard import Dashboard

def setup_logging():
    """
    ロギングの設定
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("app.log")
        ]
    )

def main():
    """
    アプリケーションのメインエントリポイント
    """
    # ロギングの設定
    setup_logging()
    
    # 環境変数からDB_PATHを取得、なければデフォルト値を使用
    db_path = os.environ.get("DB_PATH", "data/mlb_pitchers.db")
    
    # ダッシュボードの作成と実行
    dashboard = Dashboard(db_path)
    dashboard.run()

if __name__ == "__main__":
    main()