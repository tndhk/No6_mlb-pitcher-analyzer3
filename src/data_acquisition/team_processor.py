# src/data_acquisition/team_processor.py
import logging
from typing import List, Dict, Optional

import pandas as pd
from pybaseball import team_pitching, playerid_lookup, pitching_stats

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
            self.logger.info(f"Fetching pitchers for {team} in {season}")
            
            # 有効なシーズンの設定 (2022年までのデータが安定)
            valid_season = min(season, 2022)
            if valid_season != season:
                self.logger.info(f"Using {valid_season} data instead of {season} for stability")
            
            # キーポイント1: qual=0 で全投手を取得
            # キーポイント2: エラー処理を強化
            try:
                self.logger.info(f"Requesting data with pitching_stats({valid_season}, qual=0)")
                all_pitchers = pitching_stats(valid_season, qual=0)
                
                # 結果が期待通りか確認
                if all_pitchers is None or all_pitchers.empty:
                    self.logger.warning(f"Empty result from pitching_stats for season {valid_season}")
                    all_pitchers = pd.DataFrame()
                else:
                    self.logger.info(f"Successfully retrieved data with {len(all_pitchers)} rows")
                    self.logger.info(f"Columns: {all_pitchers.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error calling pitching_stats: {str(e)}")
                all_pitchers = pd.DataFrame()
            
            # データが空の場合は早期リターン
            if all_pitchers.empty:
                self.logger.warning(f"No pitching data available for season {valid_season}")
                return []
            
            # Team列の名前を特定 (APIの変更に対応)
            team_column = None
            for possible_column in ['Team', 'team', 'teamIDs', 'Tm']:
                if possible_column in all_pitchers.columns:
                    team_column = possible_column
                    self.logger.info(f"Found team column: {team_column}")
                    break
            
            if team_column is None:
                self.logger.warning(f"No team column found in data. Columns: {all_pitchers.columns.tolist()}")
                return []
            
            # チームでフィルタリング
            team_pitchers = all_pitchers[all_pitchers[team_column] == team]
            
            if team_pitchers.empty:
                # チーム名のマッピングの問題かもしれない
                self.logger.warning(f"No pitchers found for team {team} in season {valid_season}")
                self.logger.info(f"Available teams: {all_pitchers[team_column].unique().tolist()}")
                return []
            
            if team_pitchers.empty:
                self.logger.warning(f"No pitchers found for {team} in {season}")
                return []
            
            result = []
            for _, row in team_pitchers.iterrows():
                # IDの取得
                mlbam_id = None
                name = row.get('Name', '')
                if name:
                    # 名前をファーストネームとラストネームに分割
                    name_parts = name.split(' ', 1)
                    if len(name_parts) == 2:
                        first_name, last_name = name_parts
                        try:
                            player_info = playerid_lookup(last_name, first_name)
                            if not player_info.empty:
                                mlbam_id = player_info.iloc[0].get('key_mlbam')
                        except Exception as e:
                            self.logger.error(f"Error looking up ID for {name}: {str(e)}")
                
                pitcher_info = {
                    'mlbam_id': mlbam_id,
                    'name': name,
                    'position': 'P',  # ピッチング統計なので全て投手
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
            
            # 全チームのデータを取得
            all_teams_stats = team_pitching(start_season, end_season)
            
            # Teamカラムがあることを確認
            if 'Team' not in all_teams_stats.columns:
                self.logger.warning(f"'Team' column not found in team pitching stats")
                return pd.DataFrame()
                
            # チームでフィルタリング
            team_stats = all_teams_stats[all_teams_stats['Team'] == team]
            
            if team_stats.empty:
                self.logger.warning(f"No pitching stats found for {team} from {start_season} to {end_season}")
                
            return team_stats
            
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