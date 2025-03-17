# tests/data_acquisition/test_batch_processor.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.data_acquisition.batch_processor import BatchProcessor

class TestBatchProcessor:
    
    def test_init(self, mock_statcast_client):
        """BatchProcessorの初期化テスト"""
        processor = BatchProcessor(mock_statcast_client)
        assert processor is not None
        assert processor.client is mock_statcast_client
        
    def test_process_pitcher_list(self, mock_statcast_client):
        """process_pitcher_listメソッドのテスト"""
        # モック設定
        sample_data = pd.DataFrame({
            'pitch_type': ['FF', 'SL'],
            'release_speed': [95.0, 85.0]
        })
        
        mock_statcast_client.get_last_n_years_data.return_value = sample_data
        mock_statcast_client.transform_pitcher_data.return_value = sample_data
        
        # テスト
        processor = BatchProcessor(mock_statcast_client, max_workers=1, rate_limit_pause=0)
        result = processor.process_pitcher_list([123456, 234567], 3)
        
        # 検証
        assert isinstance(result, dict)
        assert mock_statcast_client.get_last_n_years_data.call_count == 2
        
    def test_get_pitcher_data_with_retry(self, mock_statcast_client):
        """_get_pitcher_data_with_retryメソッドのテスト"""
        # モック設定
        sample_data = pd.DataFrame({
            'pitch_type': ['FF', 'SL'],
            'release_speed': [95.0, 85.0]
        })
        
        mock_statcast_client.get_last_n_years_data.return_value = sample_data
        mock_statcast_client.transform_pitcher_data.return_value = sample_data
        
        # テスト
        processor = BatchProcessor(mock_statcast_client)
        result = processor._get_pitcher_data_with_retry(123456, 3, 1, 0)
        
        # 検証
        assert result is not None
        mock_statcast_client.get_last_n_years_data.assert_called_once_with(123456, 3)
        mock_statcast_client.transform_pitcher_data.assert_called_once()
