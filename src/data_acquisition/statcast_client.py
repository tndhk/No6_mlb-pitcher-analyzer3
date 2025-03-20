# src/data_acquisition/statcast_client.py
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any

import pandas as pd
from pybaseball import statcast_pitcher, playerid_lookup

class StatcastClient:
    """
    Statcastからピッチャーデータを取得するクライアント
    """
    
    def __init__(self, logging_level=logging.INFO):
        """
        StatcastClientの初期化
        
        Args:
            logging_level: ロギングレベル
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)
        
    def get_pitcher_data(self, pitcher_id: int, start_date: str, end_date: str) -> pd.DataFrame:
        """
        指定したピッチャーの特定期間のデータを取得
        
        Args:
            pitcher_id: ピッチャーID
            start_date: 開始日 (YYYY-MM-DD)
            end_date: 終了日 (YYYY-MM-DD)
            
        Returns:
            DataFrame: ピッチャーの投球データ
        """
        try:
            self.logger.info(f"Fetching data for pitcher {pitcher_id} from {start_date} to {end_date}")
            data = statcast_pitcher(start_dt=start_date, end_dt=end_date, player_id=pitcher_id)
            self.logger.info(f"Retrieved {len(data)} pitches for pitcher {pitcher_id}")
            return data
        except Exception as e:
            self.logger.error(f"Error fetching data for pitcher {pitcher_id}: {str(e)}")
            raise
    
    def get_pitcher_id_by_name(self, first_name: str, last_name: str) -> Optional[int]:
        """
        ピッチャーの名前からIDを取得
        
        Args:
            first_name: ファーストネーム
            last_name: ラストネーム
            
        Returns:
            int: ピッチャーID（見つからない場合はNone）
        """
        try:
            player_info = playerid_lookup(last_name, first_name)
            if player_info.empty:
                self.logger.warning(f"No player found with name {first_name} {last_name}")
                return None
                
            # 最も関連性の高い選手のIDを返す（同姓同名の場合を考慮）
            return player_info.iloc[0]['key_mlbam']
        except Exception as e:
            self.logger.error(f"Error looking up player {first_name} {last_name}: {str(e)}")
            raise
    
    def get_last_n_years_data(self, pitcher_id: int, years: int = 3) -> pd.DataFrame:
        """
        指定したピッチャーの直近N年分のデータを取得
        
        Args:
            pitcher_id: ピッチャーID
            years: 取得する年数（デフォルト: 3）
            
        Returns:
            DataFrame: ピッチャーの投球データ
        """
        # 現在日付から指定年数前までのデータを取得
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365 * years)).strftime('%Y-%m-%d')
        
        return self.get_pitcher_data(pitcher_id, start_date, end_date)
    
    def transform_pitcher_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        取得したデータを分析しやすい形式に変換
        
        Args:
            data: 元のピッチャーデータ
            
        Returns:
            DataFrame: 変換後のデータ
        """
        if data.empty:
            self.logger.warning("Empty dataset provided for transformation")
            return pd.DataFrame()
            
        # 必要なカラムの選択と名前変更
        try:
            # 必要なカラムを選択
            columns_of_interest = [
                'game_date', 'player_name', 'pitcher', 'pitch_type', 
                'release_speed', 'release_spin_rate', 'pfx_x', 'pfx_z',
                'plate_x', 'plate_z', 'description', 'zone', 
                'type', 'launch_speed', 'launch_angle'
            ]
            
            # 存在するカラムのみを選択
            available_columns = [col for col in columns_of_interest if col in data.columns]
            transformed_data = data[available_columns].copy()
            
            # 日付を datetime 型に変換
            if 'game_date' in transformed_data.columns:
                transformed_data['game_date'] = pd.to_datetime(transformed_data['game_date'])
                
            # 欠損値の処理
            numeric_columns = transformed_data.select_dtypes(include=['float64', 'int64']).columns
            transformed_data[numeric_columns] = transformed_data[numeric_columns].fillna(0)
            
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Error transforming pitcher data: {str(e)}")
            raise
    # src/data_acquisition/statcast_client.py に追加するメソッド
    def get_pitcher_stats(self, pitcher_id: int, season: int) -> Optional[Dict[str, Any]]:
        """
        指定したピッチャーの特定シーズンの成績統計を取得
        
        Args:
            pitcher_id: MLB選手ID
            season: シーズン年
            
        Returns:
            Dict: ピッチャーの成績統計（ERA、FIPなど）
        """
        try:
            from pybaseball import pitching_stats_bref
            
            self.logger.info(f"Fetching pitcher stats for ID {pitcher_id} in season {season}")
            
            # シーズンデータを取得
            season_stats = pitching_stats_bref(season)
            
            if season_stats.empty:
                self.logger.warning(f"No stats data found for season {season}")
                return None
            
            # MLBIDを使って選手を検索
            player_stats = season_stats[season_stats['mlbID'] == str(pitcher_id)]
            
            if player_stats.empty:
                self.logger.warning(f"No stats found for pitcher {pitcher_id} in season {season}")
                return None
            
            # 複数チームでプレイした場合は統計を合計
            if len(player_stats) > 1:
                self.logger.info(f"Pitcher {pitcher_id} played for multiple teams in {season}")
                
                # イニング数を文字列から浮動小数点数に変換
                if 'IP' in player_stats.columns:
                    player_stats['IP'] = player_stats['IP'].astype(float)
                
                # 単純に合計できる指標
                total_games = player_stats['G'].sum()
                total_starts = player_stats['GS'].sum() if 'GS' in player_stats.columns else 0
                total_innings = player_stats['IP'].sum() if 'IP' in player_stats.columns else 0
                total_strikeouts = player_stats['SO'].sum() if 'SO' in player_stats.columns else 0
                total_walks = player_stats['BB'].sum() if 'BB' in player_stats.columns else 0
                total_hits = player_stats['H'].sum() if 'H' in player_stats.columns else 0
                total_hr = player_stats['HR'].sum() if 'HR' in player_stats.columns else 0
                total_earned_runs = player_stats['ER'].sum() if 'ER' in player_stats.columns else 0
                
                # イニング数に基づいた加重平均が必要な指標
                total_ip = player_stats['IP'].sum()
                if total_ip > 0:
                    weighted_era = ((player_stats['ERA'] * player_stats['IP']).sum() / total_ip) 
                    weighted_whip = ((player_stats['WHIP'] * player_stats['IP']).sum() / total_ip)
                    weighted_so9 = ((player_stats['SO9'] * player_stats['IP']).sum() / total_ip) if 'SO9' in player_stats.columns else None
                else:
                    weighted_era = None
                    weighted_whip = None
                    weighted_so9 = None
                
                # FIPの手動計算（FIPが直接提供されていない場合）
                # FIP = (13*HR + 3*(BB+HBP) - 2*K) / IP + 定数(約3.10)
                if total_innings > 0:
                    fip_constant = 3.10  # 一般的なFIP定数
                    total_hbp = player_stats['HBP'].sum() if 'HBP' in player_stats.columns else 0
                    fip = ((13 * total_hr + 3 * (total_walks + total_hbp) - 2 * total_strikeouts) / total_innings) + fip_constant
                else:
                    fip = None
                
                # 統計を辞書にまとめる
                stats = {
                    'season': season,
                    'era': weighted_era,
                    'fip': fip,
                    'whip': weighted_whip,
                    'k_per_9': weighted_so9,
                    'bb_per_9': (9 * total_walks / total_innings) if total_innings > 0 else None,
                    'hr_per_9': (9 * total_hr / total_innings) if total_innings > 0 else None,
                    'innings_pitched': total_innings,
                    'games': total_games,
                    'games_started': total_starts,
                    'strikeouts': total_strikeouts,
                    'walks': total_walks,
                    'home_runs': total_hr,
                    'hits': total_hits,
                    'earned_runs': total_earned_runs
                }
            else:
                # 1チームの場合は直接値を取得
                stats_row = player_stats.iloc[0]
                
                # FIPの手動計算（FIPが直接提供されていない場合）
                innings = float(stats_row['IP']) if 'IP' in stats_row else 0
                if innings > 0:
                    fip_constant = 3.10  # 一般的なFIP定数
                    hr = stats_row['HR'] if 'HR' in stats_row else 0
                    bb = stats_row['BB'] if 'BB' in stats_row else 0
                    hbp = stats_row['HBP'] if 'HBP' in stats_row else 0
                    so = stats_row['SO'] if 'SO' in stats_row else 0
                    fip = ((13 * hr + 3 * (bb + hbp) - 2 * so) / innings) + fip_constant
                else:
                    fip = None
                    
                stats = {
                    'season': season,
                    'era': stats_row['ERA'] if 'ERA' in stats_row else None,
                    'fip': fip,
                    'whip': stats_row['WHIP'] if 'WHIP' in stats_row else None,
                    'k_per_9': stats_row['SO9'] if 'SO9' in stats_row else None,
                    'bb_per_9': (9 * stats_row['BB'] / stats_row['IP']) if 'BB' in stats_row and 'IP' in stats_row and stats_row['IP'] > 0 else None,
                    'hr_per_9': (9 * stats_row['HR'] / stats_row['IP']) if 'HR' in stats_row and 'IP' in stats_row and stats_row['IP'] > 0 else None,
                    'innings_pitched': float(stats_row['IP']) if 'IP' in stats_row else None,
                    'games': stats_row['G'] if 'G' in stats_row else None,
                    'games_started': stats_row['GS'] if 'GS' in stats_row else None,
                    'strikeouts': stats_row['SO'] if 'SO' in stats_row else None,
                    'walks': stats_row['BB'] if 'BB' in stats_row else None,
                    'home_runs': stats_row['HR'] if 'HR' in stats_row else None,
                    'hits': stats_row['H'] if 'H' in stats_row else None,
                    'earned_runs': stats_row['ER'] if 'ER' in stats_row else None
                }
                
            self.logger.info(f"Successfully retrieved stats for pitcher {pitcher_id} in season {season}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error fetching pitcher stats for {pitcher_id} in season {season}: {str(e)}")
            return None
        