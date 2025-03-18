# tests/mock_database.py
import logging
from typing import List, Dict, Any, Optional, Union, Tuple
from unittest.mock import MagicMock

class MockDatabase:
    """
    データベース操作をモックするためのクラス
    実際のデータベース接続なしでテストを行う
    """
    
    def __init__(self, db_path: str = ":memory:", logging_level=logging.INFO):
        """
        MockDatabaseの初期化
        
        Args:
            db_path: SQLiteデータベースファイルのパス
            logging_level: ロギングレベル
        """
        self.db_path = db_path
        self.logger = MagicMock()
        
        # 各テーブルのデータを保持する辞書
        self.pitch_types = {}  # id -> {code, name, description}
        self.pitchers = {}     # id -> {mlb_id, name, team}
        self.games = {}        # id -> {game_date, home_team, away_team, season}
        self.pitches = {}      # id -> {pitch data...}
        self.pitcher_metrics = {}  # (pitcher_id, season) -> {metrics...}
        self.pitch_usage = {}  # (pitcher_id, pitch_type_id, season) -> {usage data...}
        
        # 各テーブルの現在のID (auto-increment)
        self.current_ids = {
            'pitch_types': 1,
            'pitchers': 1,
            'games': 1,
            'pitches': 1,
            'pitcher_metrics': 1,
            'pitch_usage': 1
        }
    
    def _get_connection(self):
        """データベース接続を模したモックを返す (実際には使用されない)"""
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MagicMock()
        return mock_conn
        
    def insert_pitch_types(self, pitch_types: List[Dict[str, str]]):
        """
        球種データをデータベースに挿入 (モック)
        
        Args:
            pitch_types: 球種情報のリスト [{'code': 'FF', 'name': 'Four-Seam Fastball', 'description': '...'}]
        """
        for pitch_type in pitch_types:
            # codeが既に存在する場合はスキップ (INSERT OR IGNORE の挙動)
            code = pitch_type['code']
            if not any(p['code'] == code for p in self.pitch_types.values()):
                pitch_id = self.current_ids['pitch_types']
                self.pitch_types[pitch_id] = {
                    'id': pitch_id,
                    'code': code,
                    'name': pitch_type['name'],
                    'description': pitch_type.get('description', '')
                }
                self.current_ids['pitch_types'] += 1
                
    def get_pitch_type_id(self, pitch_code: str) -> Optional[int]:
        """
        球種コードからIDを取得 (モック)
        
        Args:
            pitch_code: 球種コード (FF, SL, CHなど)
            
        Returns:
            int: 球種ID（存在しない場合はNone）
        """
        for pitch_id, pitch_data in self.pitch_types.items():
            if pitch_data['code'] == pitch_code:
                return pitch_id
        return None
                
    def insert_pitcher(self, mlb_id: int, name: str, team: Optional[str] = None) -> int:
        """
        ピッチャー情報をデータベースに挿入 (モック)
        
        Args:
            mlb_id: MLB選手ID
            name: ピッチャー名
            team: チーム略称（オプション）
            
        Returns:
            int: データベース内のピッチャーID
        """
        # 既存のピッチャーを検索
        for pitcher_id, pitcher_data in self.pitchers.items():
            if pitcher_data['mlb_id'] == mlb_id:
                return pitcher_id
                
        # 新規ピッチャーを挿入
        pitcher_id = self.current_ids['pitchers']
        self.pitchers[pitcher_id] = {
            'id': pitcher_id,
            'mlb_id': mlb_id,
            'name': name,
            'team': team
        }
        self.current_ids['pitchers'] += 1
        return pitcher_id
                
    def get_pitcher_id(self, mlb_id: int) -> Optional[int]:
        """
        MLB選手IDからデータベース内のピッチャーIDを取得 (モック)
        
        Args:
            mlb_id: MLB選手ID
            
        Returns:
            int: データベース内のピッチャーID（存在しない場合はNone）
        """
        for pitcher_id, pitcher_data in self.pitchers.items():
            if pitcher_data['mlb_id'] == mlb_id:
                return pitcher_id
        return None
                
    def insert_game(self, game_date: str, home_team: str, away_team: str, season: int) -> int:
        """
        試合情報をデータベースに挿入 (モック)
        
        Args:
            game_date: 試合日（YYYY-MM-DD）
            home_team: ホームチーム略称
            away_team: アウェイチーム略称
            season: シーズン年
            
        Returns:
            int: データベース内の試合ID
        """
        # 既存の試合を検索
        for game_id, game_data in self.games.items():
            if (game_data['game_date'] == game_date and 
                game_data['home_team'] == home_team and 
                game_data['away_team'] == away_team):
                return game_id
                
        # 新規試合を挿入
        game_id = self.current_ids['games']
        self.games[game_id] = {
            'id': game_id,
            'game_date': game_date,
            'home_team': home_team,
            'away_team': away_team,
            'season': season
        }
        self.current_ids['games'] += 1
        return game_id
                
    def insert_pitches(self, pitches_data: List[Dict[str, Any]]):
        """
        複数の投球データをバッチで挿入 (モック)
        
        Args:
            pitches_data: 投球データのリスト
        """
        for pitch_data in pitches_data:
            pitch_id = self.current_ids['pitches']
            self.pitches[pitch_id] = {
                'id': pitch_id,
                **pitch_data
            }
            self.current_ids['pitches'] += 1
                
    def update_pitcher_metrics(self, metrics: Dict[str, Any]):
        """
        投手の成績指標を更新 (モック)
        
        Args:
            metrics: 成績指標データ
        """
        key = (metrics['pitcher_id'], metrics['season'])
        self.pitcher_metrics[key] = {
            'id': self.current_ids['pitcher_metrics'],
            **metrics
        }
        self.current_ids['pitcher_metrics'] += 1
                
    def update_pitch_usage(self, usage_data: Dict[str, Any]):
        """
        球種使用割合データを更新 (モック)
        
        Args:
            usage_data: 球種使用割合データ
        """
        key = (usage_data['pitcher_id'], usage_data['pitch_type_id'], usage_data['season'])
        self.pitch_usage[key] = {
            'id': self.current_ids['pitch_usage'],
            **usage_data
        }
        self.current_ids['pitch_usage'] += 1
                
    def get_pitcher_data(self, pitcher_id: int) -> Dict[str, Any]:
        """
        ピッチャーの基本情報を取得 (モック)
        
        Args:
            pitcher_id: ピッチャーID
            
        Returns:
            Dict: ピッチャー情報
        """
        return self.pitchers.get(pitcher_id, {})
                
    def get_pitcher_metrics(self, pitcher_id: int, season: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        ピッチャーの成績指標を取得 (モック)
        
        Args:
            pitcher_id: ピッチャーID
            season: シーズン（指定しない場合は全シーズン）
            
        Returns:
            List[Dict]: 成績指標のリスト
        """
        results = []
        for (p_id, s), metrics in self.pitcher_metrics.items():
            if p_id == pitcher_id and (season is None or s == season):
                results.append(metrics)
                
        # 降順でソート
        return sorted(results, key=lambda x: x.get('season', 0), reverse=True)
                
    def get_pitch_usage_data(self, pitcher_id: int, season: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        ピッチャーの球種使用割合を取得 (モック)
        
        Args:
            pitcher_id: ピッチャーID
            season: シーズン（指定しない場合は最新シーズン）
            
        Returns:
            List[Dict]: 球種使用割合データのリスト
        """
        results = []
        seasons = set()
        
        # 全シーズンを収集
        for (p_id, _, s) in self.pitch_usage.keys():
            if p_id == pitcher_id:
                seasons.add(s)
                
        # 特定のシーズンまたは最新シーズンのみを取得
        target_season = season
        if target_season is None and seasons:
            target_season = max(seasons)
            
        for (p_id, pt_id, s), usage in self.pitch_usage.items():
            if p_id == pitcher_id and (target_season is None or s == target_season):
                # 球種情報を追加
                pitch_type = self.pitch_types.get(pt_id, {})
                usage_with_pitch_info = {
                    **usage,
                    'code': pitch_type.get('code', ''),
                    'name': pitch_type.get('name', '')
                }
                results.append(usage_with_pitch_info)
                
        # 使用率で降順ソート
        return sorted(results, key=lambda x: x.get('usage_pct', 0), reverse=True)
                
    def get_pitchers_by_team(self, team: str) -> List[Dict[str, Any]]:
        """
        チームに所属する全ピッチャーを取得 (モック)
        
        Args:
            team: チーム略称
            
        Returns:
            List[Dict]: ピッチャー情報のリスト
        """
        results = []
        for pitcher_id, pitcher_data in self.pitchers.items():
            if pitcher_data.get('team') == team:
                results.append(pitcher_data)
                
        # 名前でソート
        return sorted(results, key=lambda x: x.get('name', ''))
                
    def search_pitchers(self, search_term: str) -> List[Dict[str, Any]]:
        """
        ピッチャー名で検索 (モック)
        
        Args:
            search_term: 検索キーワード
            
        Returns:
            List[Dict]: ピッチャー情報のリスト
        """
        results = []
        for pitcher_id, pitcher_data in self.pitchers.items():
            name = pitcher_data.get('name', '')
            if search_term.lower() in name.lower():
                results.append(pitcher_data)
                
        # 名前でソート
        return sorted(results, key=lambda x: x.get('name', ''))
                
    def get_all_teams(self) -> List[str]:
        """
        データベースに存在する全チームを取得 (モック)
        
        Returns:
            List[str]: チーム略称のリスト
        """
        teams = set()
        for pitcher_data in self.pitchers.values():
            team = pitcher_data.get('team')
            if team:
                teams.add(team)
                
        return sorted(list(teams))
    
    def get_all_pitchers(self) -> List[Dict[str, Any]]:
        """
        全ピッチャーのリストを取得 (モック)
        
        Returns:
            List[Dict]: ピッチャー情報のリスト
        """
        return sorted(list(self.pitchers.values()), key=lambda x: x.get('name', ''))