# src/data_acquisition/team_processor.py
import logging
from typing import List, Dict, Optional

import pandas as pd
from pybaseball import team_pitching, team_roster

class TeamProcessor:
    """
    チーム単位でピッチャーデータを取得するクラス
    """
    
    def __init__(self, logging_level=logging.INFO):
        """
        TeamProcessorの初期化
        
        Args:
            logging_level: ロギングレベル
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)
        
    def get_team_pitchers(self, team: str, season: int) -> List[Dict]:
        """
        指定したチームのピッチャー一覧を取得
        
        Args:
            team: チーム略称（例: "NYY", "LAD"）
            season: シーズン年
            
        Returns:
            List[Dict]: ピッチャー情報のリスト
        """
        try:
            self.logger.info(f"Fetching roster for {team} in {season}")
            roster = team_roster(team, season)
            
            # ピッチャーのみをフィルタリング
            pitchers = roster[roster['Position'].str.contains('P')]
            
            result = []
            for _, row in pitchers.iterrows():
                pitcher_info = {
                    'mlbam_id': row.get('MLB_ID', None),
                    'name': f"{row.get('Name', '')}",
                    'position': row.get('Position', ''),
                    'team': team
                }
                result.append(pitcher_info)
                
            self.logger.info(f"Found {len(result)} pitchers for {team} in {season}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error fetching team pitchers for {team} in {season}: {str(e)}")
            raise
    
    def get_team_pitching_stats(self, team: str, start_season: int, end_season: int) -> pd.DataFrame:
        """
        指定したチームの投手成績を取得
        
        Args:
            team: チーム略称
            start_season: 開始シーズン
            end_season: 終了シーズン
            
        Returns:
            DataFrame: チーム投手成績
        """
        try:
            self.logger.info(f"Fetching pitching stats for {team} from {start_season} to {end_season}")
            stats = team_pitching(start_season, end_season, team=team)
            return stats
        except Exception as e:
            self.logger.error(f"Error fetching team pitching stats for {team}: {str(e)}")
            raise
    
    def get_all_mlb_teams(self) -> List[str]:
        """
        MLB全チームの略称リストを返す
        
        Returns:
            List[str]: チーム略称のリスト
        """
        # MLBの全30チームの略称
        return [
            "ARI", "ATL", "BAL", "BOS", "CHC", "CHW", "CIN", "CLE", 
            "COL", "DET", "HOU", "KC", "LAA", "LAD", "MIA", "MIL", 
            "MIN", "NYM", "NYY", "OAK", "PHI", "PIT", "SD", "SEA", 
            "SF", "STL", "TB", "TEX", "TOR", "WSH"
        ]