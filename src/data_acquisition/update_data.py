# src/data_acquisition/update_data.py
import os
import sys
import argparse
import logging
import time
from datetime import datetime
from typing import List, Optional

import pandas as pd
from tqdm import tqdm

# プロジェクトのルートディレクトリをPYTHONPATHに追加
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.data_acquisition.statcast_client import StatcastClient
from src.data_acquisition.batch_processor import BatchProcessor
from src.data_acquisition.team_processor import TeamProcessor
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
    parser = argparse.ArgumentParser(description='MLB投手データの更新スクリプト')
    
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
    
    parser.add_argument(
        '--start-year', 
        type=int,
        default=2020,
        help='取得開始年（デフォルト: 2020）- pybaseballのデータ安定性を考慮'
    )
    
    return parser.parse_args()

def update_team_data(
    team: str, 
    team_processor: TeamProcessor,
    statcast_client: StatcastClient,
    batch_processor: BatchProcessor,
    data_manager: DataManager,
    years: int = 3,
    force_update: bool = False,
    start_year: int = 2020
) -> int:
    """
    特定チームの投手データを更新する
    
    Args:
        team: チーム略称
        team_processor: TeamProcessorインスタンス
        statcast_client: StatcastClientインスタンス
        batch_processor: BatchProcessorインスタンス
        data_manager: DataManagerインスタンス
        years: 取得する年数
        force_update: 強制更新フラグ
        start_year: 開始年（最新データが不安定な場合に備えて過去データから取得）
        
    Returns:
        int: 更新された投手の数
    """
    logger.info(f"Updating data for team: {team}")
    
    # デバッグ: 直接APIを呼び出して結果を確認
    logger.info(f"DEBUG: Testing direct API call with pitching_stats(2022, qual=0)")
    try:
        from pybaseball import pitching_stats
        test_data = pitching_stats(2022, qual=0)
        if test_data is not None and not test_data.empty:
            logger.info(f"Direct API call successful! Got {len(test_data)} records")
            logger.info(f"Columns: {test_data.columns.tolist()}")
            # チーム列を特定
            team_cols = [col for col in test_data.columns if 'team' in col.lower() or col == 'Tm']
            if team_cols:
                for team_col in team_cols:
                    logger.info(f"Team column '{team_col}' values: {test_data[team_col].unique().tolist()}")
            else:
                logger.warning("No team column found in the data")
        else:
            logger.warning("Direct API call returned empty data")
    except Exception as e:
        logger.error(f"Error in direct API call: {str(e)}")
    
    # 過去の安定したシーズンからデータを取得
    # 現在のコードでは2022年までのデータが安定している可能性が高い
    end_year = min(start_year + years - 1, 2022)
    
    # 取得対象年のリスト作成
    target_years = list(range(start_year, end_year + 1))
    target_years.reverse()  # 新しい年から順に取得
    
    # 各シーズンの投手を取得
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
            
        # 投手データを取得
        try:
            pitcher_data = statcast_client.get_last_n_years_data(mlb_id, years)
            
            if pitcher_data is not None and not pitcher_data.empty:
                # データを処理して保存
                data_manager.process_statcast_data(
                    db_pitcher_id, 
                    mlb_id, 
                    pitcher_info['name'], 
                    pitcher_data, 
                    team
                )
                updated_count += 1
                logger.info(f"Updated data for {pitcher_info['name']} (ID: {mlb_id})")
            else:
                logger.warning(f"No data found for {pitcher_info['name']} (ID: {mlb_id})")
        except Exception as e:
            logger.error(f"Error updating data for {pitcher_info['name']} (ID: {mlb_id}): {str(e)}")
            
        # APIレート制限を考慮した待機
        time.sleep(batch_processor.rate_limit_pause)
    
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
        team_processor = TeamProcessor()
        batch_processor = BatchProcessor(
            statcast_client, 
            max_workers=args.max_workers, 
            rate_limit_pause=args.rate_limit
        )
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
                batch_processor,
                data_manager,
                years=args.years,
                force_update=args.force_update,
                start_year=args.start_year
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