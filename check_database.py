#!/usr/bin/env python3
"""
データマネージャーの球種使用率計算プロセスの詳細をデバッグするスクリプト

このスクリプトは、既存のデータベースからデータを読み込み、データマネージャーの
球種使用率計算プロセスの詳細をログ出力します。
"""
import os
import sys
import logging
import pandas as pd
from datetime import datetime

# ロギングの設定
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"debug_data_manager_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger()

# プロジェクトのルートディレクトリをPYTHONPATHに追加
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.data_storage.database import Database
from src.data_storage.data_manager import DataManager

def debug_calculate_pitch_usage(db_path):
    """
    球種使用率計算プロセスをデバッグする
    """
    try:
        # データベースとデータマネージャーの初期化
        db = Database(db_path)
        data_manager = DataManager(db)
        
        # デバッグログレベルに設定
        data_manager.logger.setLevel(logging.DEBUG)
        
        # 投手IDを取得
        conn = db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM pitchers LIMIT 3")
        pitchers = cursor.fetchall()
        
        if not pitchers:
            logger.error("投手データがありません")
            return
        
        # いくつかの投手について処理
        for pitcher in pitchers:
            pitcher_id = pitcher['id']
            pitcher_name = pitcher['name']
            
            logger.info(f"投手ID {pitcher_id} ({pitcher_name}) のデータ処理を開始")
            
            # 球種使用率が既に存在するか確認
            cursor.execute("""
            SELECT pu.season, pt.code, pt.name, pu.usage_pct
            FROM pitch_usage pu
            JOIN pitch_types pt ON pu.pitch_type_id = pt.id
            WHERE pu.pitcher_id = ?
            ORDER BY pu.season DESC, pu.usage_pct DESC
            """, (pitcher_id,))
            
            existing_usage = cursor.fetchall()
            
            if existing_usage:
                logger.info(f"既存の球種使用率データ:")
                for row in existing_usage:
                    logger.info(f"  シーズン {row['season']}: {row['code']} ({row['name']}): {row['usage_pct']:.1f}%")
            else:
                logger.info("既存の球種使用率データはありません")
            
            # 投球データ取得
            cursor.execute("""
            SELECT p.*, g.season, pt.code as pitch_type_code
            FROM pitches p
            JOIN games g ON p.game_id = g.id
            LEFT JOIN pitch_types pt ON p.pitch_type_id = pt.id
            WHERE p.pitcher_id = ?
            """, (pitcher_id,))
            
            pitches = cursor.fetchall()
            
            if not pitches:
                logger.warning(f"投手ID {pitcher_id} の投球データがありません")
                continue
                
            # DataFrameに変換
            df = pd.DataFrame([dict(pitch) for pitch in pitches])
            
            # シーズンごとの投球数
            if 'season' in df.columns:
                season_counts = df['season'].value_counts().sort_index()
                logger.info(f"シーズンごとの投球数:")
                for season, count in season_counts.items():
                    logger.info(f"  シーズン {season}: {count}投球")
            
            # シーズンごとに球種使用率を計算する処理をシミュレート
            if 'season' in df.columns and 'pitch_type_code' in df.columns:
                for season, season_data in df.groupby('season'):
                    logger.info(f"シーズン {season} のデータ処理 ({len(season_data)}投球)")
                    
                    # 球種使用率の計算
                    pitch_counts = season_data['pitch_type_code'].value_counts()
                    total = len(season_data)
                    
                    logger.info(f"球種ごとの投球数と使用率:")
                    for pitch_type, count in pitch_counts.items():
                        if pitch_type and not pd.isna(pitch_type):
                            usage_pct = (count / total) * 100
                            logger.info(f"  {pitch_type}: {count}投球 ({usage_pct:.1f}%)")
                    
                    # _calculate_pitch_usageメソッドをシミュレート
                    logger.info(f"_calculate_pitch_usage メソッドをシミュレート...")
                    
                    # 球種ごとにグループ化
                    for pitch_type, group in season_data.groupby('pitch_type_code'):
                        if not pitch_type or pd.isna(pitch_type):
                            continue
                            
                        # 球種IDの取得
                        pitch_type_id = db.get_pitch_type_id(pitch_type)
                        if not pitch_type_id:
                            logger.warning(f"未知の球種コード: {pitch_type}")
                            continue
                            
                        # 球種ごとの投球数と使用率
                        pitch_count = len(group)
                        usage_pct = (pitch_count / total) * 100
                        
                        logger.info(f"  球種 {pitch_type} (ID: {pitch_type_id}): {pitch_count}投球 ({usage_pct:.1f}%)")
                        
                        # 指標の計算シミュレーション
                        avg_velocity = group['release_speed'].mean() if 'release_speed' in group.columns else None
                        avg_spin_rate = group['release_spin_rate'].mean() if 'release_spin_rate' in group.columns else None
                        
                        logger.info(f"    平均球速: {avg_velocity:.1f} mph, 平均回転数: {avg_spin_rate:.0f} rpm")
                        
            logger.info(f"投手ID {pitcher_id} のデータ処理を完了")
            logger.info("-" * 50)
        
        conn.close()
        
    except Exception as e:
        logger.error(f"デバッグ処理中にエラーが発生しました: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    # データベースパス
    db_path = os.environ.get("DB_PATH", "data/mlb_pitchers.db")
    debug_calculate_pitch_usage(db_path)