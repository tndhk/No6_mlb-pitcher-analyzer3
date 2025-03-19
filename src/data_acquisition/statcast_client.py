# src/data_acquisition/statcast_client.py
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

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