# sample_data.py
import os
import sqlite3
import logging
from datetime import datetime

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_sample_data(db_path):
    """サンプルデータをデータベースに追加するスクリプト"""
    logger.info(f"サンプルデータをデータベースに追加します: {db_path}")
    
    # データベースに接続
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # 球種データの挿入（すでに存在するはずですが、念のため）
        pitch_types = [
            ('FF', 'Four-Seam Fastball', 'Standard four-seam fastball'),
            ('SL', 'Slider', 'Breaking ball with lateral and downward movement'),
            ('CH', 'Changeup', 'Off-speed pitch that mimics fastball arm action'),
            ('CU', 'Curveball', 'Breaking ball with significant downward movement'),
            ('SI', 'Sinker', 'Fastball with downward movement')
        ]
        
        for pitch_type in pitch_types:
            cursor.execute('''
            INSERT OR IGNORE INTO pitch_types (code, name, description)
            VALUES (?, ?, ?)
            ''', pitch_type)
        
        # サンプル投手データ
        pitchers = [
            (1001, 'Shohei Ohtani', 'LAD'),
            (1002, 'Clayton Kershaw', 'LAD'),
            (1003, 'Yoshinobu Yamamoto', 'LAD'),
            (1004, 'Marcus Stroman', 'CHC'),
            (1005, 'Kyle Hendricks', 'CHC'),
            (1006, 'Gerrit Cole', 'NYY'),
            (1007, 'Kodai Senga', 'NYM'),
            (1008, 'Yu Darvish', 'SD')
        ]
        
        for pitcher in pitchers:
            cursor.execute('''
            INSERT OR IGNORE INTO pitchers (mlb_id, name, team)
            VALUES (?, ?, ?)
            ''', pitcher)
            
            # 投手IDを取得
            cursor.execute('SELECT id FROM pitchers WHERE mlb_id = ?', (pitcher[0],))
            pitcher_id = cursor.fetchone()['id']
            
            # サンプル試合データ
            games = [
                ('2023-04-01', pitcher[2], 'BOS', 2023),
                ('2023-04-15', 'BOS', pitcher[2], 2023),
                ('2023-05-01', pitcher[2], 'NYM', 2023),
                ('2023-05-15', 'ATL', pitcher[2], 2023),
                ('2023-06-01', pitcher[2], 'HOU', 2023)
            ]
            
            game_ids = []
            for game in games:
                cursor.execute('''
                INSERT OR IGNORE INTO games (game_date, home_team, away_team, season)
                VALUES (?, ?, ?, ?)
                ''', game)
                
                cursor.execute('''
                SELECT id FROM games WHERE game_date = ? AND home_team = ? AND away_team = ?
                ''', (game[0], game[1], game[2]))
                
                game_id = cursor.fetchone()['id']
                game_ids.append(game_id)
            
            # 成績指標データ
            metrics = {
                'pitcher_id': pitcher_id,
                'season': 2023,
                'era': 3.45 + (pitcher_id % 5) * 0.2,
                'fip': 3.56 + (pitcher_id % 5) * 0.15,
                'whip': 1.21 + (pitcher_id % 5) * 0.05,
                'k_per_9': 9.5 + (pitcher_id % 5) * 0.3,
                'bb_per_9': 2.8 - (pitcher_id % 5) * 0.1,
                'hr_per_9': 1.2 - (pitcher_id % 5) * 0.05,
                'swstr_pct': 11.5 + (pitcher_id % 5) * 0.4,
                'csw_pct': 30.2 + (pitcher_id % 5) * 0.3,
                'o_swing_pct': 32.5 + (pitcher_id % 5) * 0.5,
                'z_contact_pct': 85.3 - (pitcher_id % 5) * 0.7,
                'innings_pitched': 180.2 - (pitcher_id % 5) * 10,
                'games': 30 - (pitcher_id % 5),
                'strikeouts': 182 + (pitcher_id % 5) * 5,
                'walks': 55 - (pitcher_id % 5) * 2,
                'home_runs': 22 - (pitcher_id % 5),
                'hits': 165 - (pitcher_id % 5) * 3,
                'earned_runs': 69 - (pitcher_id % 5) * 2
            }
            
            # 成績指標を挿入
            cursor.execute('''
            INSERT OR REPLACE INTO pitcher_metrics (
                pitcher_id, season, era, fip, whip, k_per_9, bb_per_9, hr_per_9,
                swstr_pct, csw_pct, o_swing_pct, z_contact_pct, innings_pitched,
                games, strikeouts, walks, home_runs, hits, earned_runs
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                metrics['pitcher_id'],
                metrics['season'],
                metrics['era'],
                metrics['fip'],
                metrics['whip'],
                metrics['k_per_9'],
                metrics['bb_per_9'],
                metrics['hr_per_9'],
                metrics['swstr_pct'],
                metrics['csw_pct'],
                metrics['o_swing_pct'],
                metrics['z_contact_pct'],
                metrics['innings_pitched'],
                metrics['games'],
                metrics['strikeouts'],
                metrics['walks'],
                metrics['home_runs'],
                metrics['hits'],
                metrics['earned_runs']
            ))
            
            # 球種使用割合データ
            pitch_usage_data = [
                # FF (フォーシーム)
                {
                    'pitcher_id': pitcher_id,
                    'pitch_type_code': 'FF',
                    'usage_pct': 50.0 - (pitcher_id % 5) * 2,
                    'avg_velocity': 95.5 - (pitcher_id % 5) * 0.5,
                    'avg_spin_rate': 2425 + (pitcher_id % 5) * 20,
                    'avg_pfx_x': 2.5 - (pitcher_id % 5) * 0.2,
                    'avg_pfx_z': 8.5 + (pitcher_id % 5) * 0.3,
                    'whiff_pct': 10.0 + (pitcher_id % 5) * 0.5
                },
                # SL (スライダー)
                {
                    'pitcher_id': pitcher_id,
                    'pitch_type_code': 'SL',
                    'usage_pct': 25.0 + (pitcher_id % 5) * 1,
                    'avg_velocity': 85.5 - (pitcher_id % 5) * 0.3,
                    'avg_spin_rate': 2615 + (pitcher_id % 5) * 15,
                    'avg_pfx_x': -2.5 - (pitcher_id % 5) * 0.15,
                    'avg_pfx_z': 2.5 - (pitcher_id % 5) * 0.2,
                    'whiff_pct': 25.0 + (pitcher_id % 5) * 1.2
                },
                # CH (チェンジアップ)
                {
                    'pitcher_id': pitcher_id,
                    'pitch_type_code': 'CH',
                    'usage_pct': 15.0 + (pitcher_id % 5) * 0.5,
                    'avg_velocity': 84.0 - (pitcher_id % 5) * 0.2,
                    'avg_spin_rate': 1810 - (pitcher_id % 5) * 10,
                    'avg_pfx_x': 5.0 + (pitcher_id % 5) * 0.3,
                    'avg_pfx_z': 5.0 - (pitcher_id % 5) * 0.25,
                    'whiff_pct': 18.0 + (pitcher_id % 5) * 0.8
                },
                # CU (カーブ)
                {
                    'pitcher_id': pitcher_id,
                    'pitch_type_code': 'CU',
                    'usage_pct': 10.0 + (pitcher_id % 5) * 0.3,
                    'avg_velocity': 78.0 - (pitcher_id % 5) * 0.5,
                    'avg_spin_rate': 2800 + (pitcher_id % 5) * 25,
                    'avg_pfx_x': -3.0 - (pitcher_id % 5) * 0.2,
                    'avg_pfx_z': -6.0 - (pitcher_id % 5) * 0.4,
                    'whiff_pct': 22.0 + (pitcher_id % 5) * 1.0
                }
            ]
            
            for usage in pitch_usage_data:
                # 球種IDを取得
                cursor.execute('SELECT id FROM pitch_types WHERE code = ?', (usage['pitch_type_code'],))
                pitch_type_id = cursor.fetchone()['id']
                
                cursor.execute('''
                INSERT OR REPLACE INTO pitch_usage (
                    pitcher_id, pitch_type_id, season, usage_pct, avg_velocity,
                    avg_spin_rate, avg_pfx_x, avg_pfx_z, whiff_pct
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    usage['pitcher_id'],
                    pitch_type_id,
                    2023,
                    usage['usage_pct'],
                    usage['avg_velocity'],
                    usage['avg_spin_rate'],
                    usage['avg_pfx_x'],
                    usage['avg_pfx_z'],
                    usage['whiff_pct']
                ))
        
        # コミット
        conn.commit()
        logger.info(f"サンプルデータの追加が完了しました: {len(pitchers)}人の投手データ")
        
    except Exception as e:
        logger.error(f"サンプルデータの追加中にエラーが発生しました: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # データベースパス
    db_path = os.environ.get("DB_PATH", "data/mlb_pitchers.db")
    create_sample_data(db_path)