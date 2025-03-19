# src/data_acquisition/statcast_team_processor.py
import logging
import os
import json
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import time

import pandas as pd
from pybaseball import playerid_lookup, statcast_pitcher

class StatcastTeamProcessor:
    """
    Statcastからチームと選手のデータを取得するクラス
    機能しているAPIのみを使用
    """
    
    def __init__(self, logging_level=logging.INFO, cache_dir="data/cache", 
                 roster_file="data/pitchers.csv", config_file="data/seasons_config.json"):
        """初期化
        
        Args:
            logging_level: ロギングレベル
            cache_dir: キャッシュディレクトリ
            roster_file: 投手ロスターCSVファイルパス
            config_file: 設定ファイルパス（シーズン情報など）
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)
        
        # キャッシュディレクトリの設定
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # ロスターファイルの設定
        self.roster_file = roster_file
        
        # 設定ファイルの読み込み
        self.config_file = config_file
        self.config = self._load_config()
        
        # チームロスターデータの読み込み
        self.team_rosters = self._load_team_rosters_from_csv()
    
    def _load_config(self) -> Dict:
        """設定ファイルを読み込む
        
        Returns:
            Dict: 設定情報
        """
        default_config = {
            "available_seasons": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022],
            "default_range": 3,
            "most_stable_season": 2022,
            "api_settings": {
                "statcast_pitcher": {
                    "available_years": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022],
                    "stable_years": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]
                }
            }
        }
        
        # 設定ファイルが存在するか確認
        if not os.path.exists(self.config_file):
            self.logger.warning(f"Config file not found: {self.config_file}, using defaults")
            return default_config
            
        try:
            # JSONファイルの読み込み
            with open(self.config_file, 'r') as f:
                config = json.load(f)
                
            self.logger.info(f"Loaded config from {self.config_file}")
            return config
            
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            return default_config
    
    def get_valid_season(self, requested_season: int) -> int:
        """有効なシーズンを取得（リクエストされた年度が利用不可の場合は代替を返す）
        
        Args:
            requested_season: リクエストされたシーズン
            
        Returns:
            int: 有効なシーズン
        """
        # 利用可能なシーズンを取得
        available_seasons = self.config.get("available_seasons", [])
        
        # 最も安定した最新シーズン
        most_stable_season = self.config.get("most_stable_season", 2022)
        
        # リクエストされたシーズンが利用可能かチェック
        if not available_seasons or requested_season not in available_seasons:
            self.logger.warning(f"Season {requested_season} not available, using {most_stable_season} instead")
            return most_stable_season
            
        return requested_season
        
    def get_available_seasons(self) -> List[int]:
        """利用可能なシーズンのリストを取得
        
        Returns:
            List[int]: 利用可能なシーズンのリスト
        """
        return self.config.get("available_seasons", [])
    
    def get_team_pitchers(self, team: str, season: int) -> List[Dict]:
        """指定したチームのピッチャー一覧を取得
        
        Args:
            team: チーム略称（例: "NYY", "LAD"）
            season: シーズン年
            
        Returns:
            List[Dict]: ピッチャー情報のリスト
        """
        # チームロスターキャッシュの確認
        cache_file = os.path.join(self.cache_dir, f"{team}_{season}_roster.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    roster = json.load(f)
                self.logger.info(f"Loaded {len(roster)} players from cache for {team}")
                return roster
            except Exception as e:
                self.logger.error(f"Error loading cache: {str(e)}")
        
        # キャッシュがない場合はロスターデータを使用
        if team in self.team_rosters:
            roster = self.team_rosters[team]
            self.logger.info(f"Using roster for {team} with {len(roster)} players")
            
            # キャッシュに保存
            try:
                with open(cache_file, 'w') as f:
                    json.dump(roster, f, indent=2)
            except Exception as e:
                self.logger.error(f"Error saving to cache: {str(e)}")
                
            return roster
        
        # 対応するチームがない場合は空のリストを返す
        self.logger.warning(f"No roster data available for team {team}")
        return []
    
    def _load_team_rosters_from_csv(self) -> Dict[str, List[Dict]]:
        """CSVファイルからチームロスターデータを読み込む
        
        Returns:
            Dict[str, List[Dict]]: チームごとの選手リスト
        """
        team_rosters = {}
        
        # ファイルが存在するか確認
        if not os.path.exists(self.roster_file):
            self.logger.warning(f"Roster file not found: {self.roster_file}")
            return self._get_fallback_team_rosters()
        
        try:
            # CSVファイルの読み込み
            df = pd.read_csv(self.roster_file)
            
            # 必要なカラムがあるか確認
            required_columns = ['mlbam_id', 'name', 'team', 'position']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                self.logger.warning(f"Missing columns in roster file: {missing_columns}")
                return self._get_fallback_team_rosters()
            
            # Pポジション（投手）だけをフィルタリング
            pitchers_df = df[df['position'] == 'P']
            
            # チーム別にグループ化
            for team, group in pitchers_df.groupby('team'):
                pitchers = []
                
                for _, row in group.iterrows():
                    pitcher = {
                        'mlbam_id': int(row['mlbam_id']),
                        'name': row['name'],
                        'position': 'P',
                        'team': row['team']
                    }
                    pitchers.append(pitcher)
                
                team_rosters[team] = pitchers
                
            self.logger.info(f"Loaded roster data for {len(team_rosters)} teams from CSV")
            
            return team_rosters
            
        except Exception as e:
            self.logger.error(f"Error loading roster from CSV: {str(e)}")
            return self._get_fallback_team_rosters()
            
    def _get_fallback_team_rosters(self) -> Dict[str, List[Dict]]:
        """フォールバックのハードコードされたチームロスターデータ
        
        Returns:
            Dict[str, List[Dict]]: チームごとの選手リスト
        """
        self.logger.warning("Using fallback hardcoded roster data")
        
        return {
            "NYY": [
                {"mlbam_id": 543037, "name": "Gerrit Cole", "position": "P", "team": "NYY"},
                {"mlbam_id": 547888, "name": "Carlos Rodon", "position": "P", "team": "NYY"},
                {"mlbam_id": 656756, "name": "Clarke Schmidt", "position": "P", "team": "NYY"},
                {"mlbam_id": 547973, "name": "Nestor Cortes", "position": "P", "team": "NYY"},
                {"mlbam_id": 543883, "name": "Clay Holmes", "position": "P", "team": "NYY"}
            ],
            "LAD": [
                {"mlbam_id": 657277, "name": "Walker Buehler", "position": "P", "team": "LAD"},
                {"mlbam_id": 477132, "name": "Clayton Kershaw", "position": "P", "team": "LAD"},
                {"mlbam_id": 657756, "name": "Dustin May", "position": "P", "team": "LAD"},
                {"mlbam_id": 605182, "name": "Blake Treinen", "position": "P", "team": "LAD"},
                {"mlbam_id": 592826, "name": "Evan Phillips", "position": "P", "team": "LAD"}
            ],
            "CHC": [
                {"mlbam_id": 592767, "name": "Marcus Stroman", "position": "P", "team": "CHC"},
                {"mlbam_id": 628317, "name": "Justin Steele", "position": "P", "team": "CHC"},
                {"mlbam_id": 665871, "name": "Adbert Alzolay", "position": "P", "team": "CHC"},
                {"mlbam_id": 543606, "name": "Drew Smyly", "position": "P", "team": "CHC"},
                {"mlbam_id": 621112, "name": "Jameson Taillon", "position": "P", "team": "CHC"}
            ]
        }
    
    def get_pitcher_data(self, pitcher_id: int, season: int) -> pd.DataFrame:
        """特定の投手のシーズンデータを取得
        
        Args:
            pitcher_id: MLBAM投手ID
            season: シーズン
            
        Returns:
            pd.DataFrame: 投球データ
        """
        # 有効なシーズンを取得
        valid_season = self.get_valid_season(season)
        
        # シーズンが変更された場合はログ出力
        if valid_season != season:
            self.logger.info(f"Using {valid_season} data instead of {season}")
        
        # キャッシュの確認
        cache_file = os.path.join(self.cache_dir, f"pitcher_{pitcher_id}_{valid_season}.csv")
        if os.path.exists(cache_file):
            try:
                data = pd.read_csv(cache_file)
                self.logger.info(f"Loaded {len(data)} pitches from cache for pitcher {pitcher_id}")
                return data
            except Exception as e:
                self.logger.error(f"Error loading pitcher data from cache: {str(e)}")
        
        # シーズンの始まりと終わりの日付を設定
        start_date = f"{valid_season}-03-01"  # シーズン開始前（春季キャンプ含む）
        end_date = f"{valid_season}-11-30"    # シーズン終了後（ポストシーズン含む）
        
        # データの取得を試みる
        try:
            # statcast_pitcherが動作することを確認済み
            self.logger.info(f"Fetching data for pitcher {pitcher_id} from {start_date} to {end_date}")
            data = statcast_pitcher(start_dt=start_date, end_dt=end_date, player_id=pitcher_id)
            
            if data is not None and not data.empty:
                self.logger.info(f"Retrieved {len(data)} pitches for pitcher {pitcher_id}")
                
                # キャッシュに保存
                try:
                    data.to_csv(cache_file, index=False)
                    self.logger.info(f"Saved pitcher data to cache: {cache_file}")
                except Exception as e:
                    self.logger.error(f"Error saving pitcher data to cache: {str(e)}")
                
                return data
            else:
                self.logger.warning(f"No data retrieved for pitcher {pitcher_id}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching data for pitcher {pitcher_id}: {str(e)}")
            return pd.DataFrame()
    
    def get_all_mlb_teams(self) -> List[str]:
        """MLB全チームの略称リストを返す
        
        Returns:
            List[str]: チーム略称のリスト
        """
        # ロードされたロスターからチームを取得
        if self.team_rosters:
            return sorted(list(self.team_rosters.keys()))
        
        # フォールバック: MLBの全30チームの略称
        return [
            "ARI", "ATL", "BAL", "BOS", "CHC", "CHW", "CIN", "CLE", 
            "COL", "DET", "HOU", "KC", "LAA", "LAD", "MIA", "MIL", 
            "MIN", "NYM", "NYY", "OAK", "PHI", "PIT", "SD", "SEA", 
            "SF", "STL", "TB", "TEX", "TOR", "WSH"
        ]
        
    def save_roster_to_csv(self, output_file: Optional[str] = None) -> None:
        """現在のロスターデータをCSVファイルに保存
        
        Args:
            output_file: 出力ファイルパス（省略時は初期化時に指定したパス）
        """
        if output_file is None:
            output_file = self.roster_file
            
        try:
            # 全選手データをリストに変換
            all_pitchers = []
            for team, pitchers in self.team_rosters.items():
                all_pitchers.extend(pitchers)
                
            # DataFrameに変換
            df = pd.DataFrame(all_pitchers)
            
            # CSVに保存
            df.to_csv(output_file, index=False)
            self.logger.info(f"Saved {len(all_pitchers)} pitchers to {output_file}")
        except Exception as e:
            self.logger.error(f"Error saving roster to CSV: {str(e)}")
            
    def add_pitcher_to_roster(self, mlbam_id: int, name: str, team: str) -> None:
        """投手をロスターに追加
        
        Args:
            mlbam_id: MLBAM選手ID
            name: 選手名
            team: チーム略称
        """
        # チームが存在しない場合は新規作成
        if team not in self.team_rosters:
            self.team_rosters[team] = []
            
        # 既存の選手をチェック
        for pitcher in self.team_rosters[team]:
            if pitcher['mlbam_id'] == mlbam_id:
                # 既に存在する場合は更新
                pitcher['name'] = name
                self.logger.info(f"Updated pitcher {name} (ID: {mlbam_id}) for team {team}")
                return
                
        # 存在しない場合は新規追加
        pitcher_info = {
            'mlbam_id': mlbam_id,
            'name': name,
            'position': 'P',
            'team': team
        }
        
        self.team_rosters[team].append(pitcher_info)
        self.logger.info(f"Added pitcher {name} (ID: {mlbam_id}) to team {team}")
        
    def lookup_player_id(self, last_name: str, first_name: str) -> Optional[int]:
        """選手名からIDを検索
        
        Args:
            last_name: 姓
            first_name: 名
            
        Returns:
            Optional[int]: 選手ID（見つからない場合はNone）
        """
        try:
            player_info = playerid_lookup(last_name, first_name)
            if player_info.empty:
                self.logger.warning(f"No player found for {first_name} {last_name}")
                return None
                
            if 'key_mlbam' in player_info.columns:
                return player_info.iloc[0]['key_mlbam']
            else:
                self.logger.warning(f"key_mlbam not found in result for {first_name} {last_name}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error looking up player {first_name} {last_name}: {str(e)}")
            return None