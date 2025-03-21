# src/data_acquisition/update_data_statcast.py
import os
import sys
import argparse
import logging
import time
from datetime import datetime
from typing import List, Optional
from tqdm import tqdm

import pandas as pd

# プロジェクトのルートディレクトリをPYTHONPATHに追加
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.data_acquisition.statcast_client import StatcastClient
from src.data_acquisition.statcast_team_processor import StatcastTeamProcessor
from src.data_storage.database import Database
from src.data_storage.data_manager import DataManager

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger(__name__)

def setup_argparse() -> argparse.Namespace:
    """コマンドライン引数の設定"""
    parser = argparse.ArgumentParser(description='MLB投手データの更新スクリプト（Statcastベース）')
    
    parser.add_argument(
        '--db-path', 
        type=str, 
        default=os.environ.get('DB_PATH', 'data/mlb_pitchers.db'),
        help='SQLiteデータベースのパス'
    )
    
    parser.add_argument(
        '--teams', 
        type=str, 
        nargs='+',
        help='更新するチームのリスト（スペース区切り、例: "NYY LAD BOS"）'
    )
    
    parser.add_argument(
        '--years', 
        type=int,
        default=3,
        help='取得する年数（デフォルト: 3年）'
    )
    
    parser.add_argument(
        '--start-year', 
        type=int,
        default=2022,
        help='取得開始年（デフォルト: 2020）'
    )
    
    parser.add_argument(
        '--max-workers', 
        type=int,
        default=5,
        help='並列処理の最大ワーカー数（デフォルト: 5）'
    )
    
    parser.add_argument(
        '--rate-limit', 
        type=float,
        default=2.0,
        help='API呼び出し間の待機時間（秒）（デフォルト: 2.0秒）'
    )
    
    parser.add_argument(
        '--force-update', 
        action='store_true',
        help='既存のデータがあっても強制的に更新する'
    )
    
    return parser.parse_args()

def update_team_data(
    team: str, 
    team_processor: StatcastTeamProcessor,
    statcast_client: StatcastClient,
    data_manager: DataManager,
    years: int = 3,
    force_update: bool = False,
    start_year: int = 2020,
    rate_limit: float = 2.0
) -> int:
    """
    特定チームの投手データを更新する
    
    Args:
        team: チーム略称
        team_processor: StatcastTeamProcessorインスタンス
        statcast_client: StatcastClientインスタンス
        data_manager: DataManagerインスタンス
        years: 取得する年数
        force_update: 強制更新フラグ
        start_year: 開始年
        rate_limit: API呼び出し間の待機時間（秒）
        
    Returns:
        int: 更新された投手の数
    """
    logger.info(f"Updating data for team: {team}")
    
    # 利用可能なシーズンを取得
    available_seasons = team_processor.get_available_seasons()
    if not available_seasons:
        logger.warning("No available seasons configured")
        return 0
        
    # 有効な開始年と終了年を計算
    valid_start_year = team_processor.get_valid_season(start_year)
    
    # 年度の範囲を計算（設定された利用可能シーズン内に制限）
    target_years = []
    for year in range(valid_start_year, valid_start_year + years):
        valid_year = team_processor.get_valid_season(year)
        if valid_year not in target_years:  # 重複を避ける
            target_years.append(valid_year)
    
    # 新しい年から順に処理
    target_years.sort(reverse=True)
    
    logger.info(f"Target seasons for {team}: {target_years}")
    
    # 利用可能な選手リストを取得
    pitchers = []
    for year in target_years:
        try:
            team_pitchers = team_processor.get_team_pitchers(team, year)
            pitchers.extend(team_pitchers)
            logger.info(f"Found {len(team_pitchers)} pitchers for {team} in {year}")
        except Exception as e:
            logger.error(f"Error fetching pitchers for {team} in {year}: {str(e)}")
    
    # 重複を除去（複数シーズンに出場した投手）
    unique_pitchers = {}
    for p in pitchers:
        if p['mlbam_id'] is not None and p['mlbam_id'] not in unique_pitchers:
            unique_pitchers[p['mlbam_id']] = p
    
    logger.info(f"Found {len(unique_pitchers)} unique pitchers for {team}")
    
    # 各投手のデータを取得・更新
    updated_count = 0
    
    for mlb_id, pitcher_info in tqdm(unique_pitchers.items(), desc=f"Updating {team} pitchers"):
        if mlb_id is None:
            logger.warning(f"Skipping pitcher {pitcher_info['name']} with no MLB ID")
            continue
            
        # データベース内の投手IDを取得または作成
        db_pitcher_id = data_manager.db.get_pitcher_id(mlb_id)
        if db_pitcher_id is None:
            db_pitcher_id = data_manager.db.insert_pitcher(
                mlb_id, 
                pitcher_info['name'], 
                pitcher_info['team']
            )
        
        # 各シーズンのデータを取得
        all_pitcher_data = pd.DataFrame()
        
        for year in range(start_year, start_year + years):
            try:
                # Statcastから投手データを取得
                season_data = team_processor.get_pitcher_data(mlb_id, year)
                
                if not season_data.empty:
                    # シーズン情報を追加（あれば）
                    if 'season' not in season_data.columns:
                        season_data['season'] = year
                        
                    # データを結合
                    all_pitcher_data = pd.concat([all_pitcher_data, season_data])
            except Exception as e:
                logger.error(f"Error fetching data for {pitcher_info['name']} in {year}: {str(e)}")
            
            # APIレート制限のための待機
            time.sleep(rate_limit)
            
        if not all_pitcher_data.empty:
            # データを処理して保存
            try:
                # データをStatcastClientの形式に変換
                processed_data = statcast_client.transform_pitcher_data(all_pitcher_data)
                
                # データを保存
                data_manager.process_statcast_data(
                    db_pitcher_id, 
                    mlb_id, 
                    pitcher_info['name'], 
                    processed_data, 
                    team
                )
                
                updated_count += 1
                logger.info(f"Updated data for {pitcher_info['name']} (ID: {mlb_id})")
            except Exception as e:
                logger.error(f"Error processing data for {pitcher_info['name']}: {str(e)}")
        else:
            logger.warning(f"No data found for {pitcher_info['name']} (ID: {mlb_id})")
    
    return updated_count

def main():
    """メイン実行関数"""
    # 引数の解析
    args = setup_argparse()
    
    try:
        # データベースの初期化
        db = Database(args.db_path)
        logger.info(f"Database initialized at {args.db_path}")
        
        # 各種クライアント・プロセッサの初期化
        statcast_client = StatcastClient()
        team_processor = StatcastTeamProcessor(cache_dir="data/cache")
        data_manager = DataManager(db)
        
        # 更新対象のチームを決定
        if args.teams:
            teams_to_update = args.teams
        else:
            # デフォルトでは全チームを更新
            teams_to_update = team_processor.get_all_mlb_teams()
            
        logger.info(f"Updating data for {len(teams_to_update)} teams: {', '.join(teams_to_update)}")
        
        # 各チームの投手データを更新
        total_updated = 0
        start_time = time.time()
        
        for team in teams_to_update:
            updated_count = update_team_data(
                team,
                team_processor,
                statcast_client,
                data_manager,
                years=args.years,
                force_update=args.force_update,
                start_year=args.start_year,
                rate_limit=args.rate_limit
            )
            total_updated += updated_count
            
        elapsed_time = time.time() - start_time
        logger.info(f"Data update completed in {elapsed_time:.2f} seconds")
        logger.info(f"Updated data for {total_updated} pitchers across {len(teams_to_update)} teams")
        
    except Exception as e:
        logger.error(f"Error in data update process: {str(e)}", exc_info=True)
        sys.exit(1)
        
    logger.info("Data update completed successfully")

if __name__ == "__main__":
    main()