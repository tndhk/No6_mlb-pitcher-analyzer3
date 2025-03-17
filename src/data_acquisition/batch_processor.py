# src/data_acquisition/batch_processor.py
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.data_acquisition.statcast_client import StatcastClient

class BatchProcessor:
    """
    複数ピッチャーのデータを一括で取得するバッチ処理クラス
    """
    
    def __init__(self, statcast_client: StatcastClient, max_workers: int = 5, 
                 rate_limit_pause: float = 2.0, logging_level=logging.INFO):
        """
        BatchProcessorの初期化
        
        Args:
            statcast_client: StatcastClientインスタンス
            max_workers: 並列処理の最大ワーカー数
            rate_limit_pause: API呼び出し間の待機時間（秒）
            logging_level: ロギングレベル
        """
        self.client = statcast_client
        self.max_workers = max_workers
        self.rate_limit_pause = rate_limit_pause
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)
        
    def process_pitcher_list(self, pitcher_ids: List[int], years: int = 3) -> Dict[int, pd.DataFrame]:
        """
        複数のピッチャーのデータを取得し、結果を辞書形式で返す
        
        Args:
            pitcher_ids: ピッチャーIDのリスト
            years: 取得する年数
            
        Returns:
            Dict[int, DataFrame]: ピッチャーID -> DataFrame のマッピング
        """
        results = {}
        failed_ids = []
        
        self.logger.info(f"Starting batch processing for {len(pitcher_ids)} pitchers")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {
                executor.submit(self._get_pitcher_data_with_retry, pitcher_id, years): pitcher_id 
                for pitcher_id in pitcher_ids
            }
            
            for future in as_completed(future_to_id):
                pitcher_id = future_to_id[future]
                try:
                    data = future.result()
                    if data is not None and not data.empty:
                        results[pitcher_id] = data
                        self.logger.info(f"Successfully processed pitcher {pitcher_id}")
                    else:
                        failed_ids.append(pitcher_id)
                        self.logger.warning(f"No data retrieved for pitcher {pitcher_id}")
                except Exception as e:
                    failed_ids.append(pitcher_id)
                    self.logger.error(f"Error processing pitcher {pitcher_id}: {str(e)}")
                
                # API呼び出しのレート制限を考慮した待機
                time.sleep(self.rate_limit_pause)
        
        self.logger.info(f"Batch processing completed. Successful: {len(results)}, Failed: {len(failed_ids)}")
        
        return results
    
    def _get_pitcher_data_with_retry(self, pitcher_id: int, years: int, 
                                    max_retries: int = 3, retry_delay: float = 5.0) -> Optional[pd.DataFrame]:
        """
        リトライ機能付きでピッチャーデータを取得
        
        Args:
            pitcher_id: ピッチャーID
            years: 取得する年数
            max_retries: 最大リトライ回数
            retry_delay: リトライ間の待機時間（秒）
            
        Returns:
            DataFrame: 変換済みのピッチャーデータ（失敗した場合はNone）
        """
        retries = 0
        
        while retries < max_retries:
            try:
                data = self.client.get_last_n_years_data(pitcher_id, years)
                
                if data is not None and not data.empty:
                    return self.client.transform_pitcher_data(data)
                return None
                
            except Exception as e:
                retries += 1
                self.logger.warning(
                    f"Attempt {retries}/{max_retries} failed for pitcher {pitcher_id}: {str(e)}"
                )
                
                if retries < max_retries:
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"All retries failed for pitcher {pitcher_id}")
                    return None