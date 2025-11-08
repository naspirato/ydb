#!/usr/bin/env python3

import pandas as pd
import numpy as np
import hashlib
import json
import re
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict


class Preprocessing:
    """Preprocessing and grouping of measurement data"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize preprocessing
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.context_fields = config['context_fields']
        self.metric_fields = config['metric_fields']
        self.timestamp_field = config['timestamp_field']
        self.metric_name_field = self.metric_fields[0]  # First field is metric_name
        self.metric_value_field = self.metric_fields[1]  # Second field is metric_value
        # Опция для автоматической агрегации с указанием интервала
        # Поддерживаемые значения: 'S' (секунды), 'min' (минуты), 'H' (часы), 'D' (дни)
        # Можно указать число + интервал, например: '15min', '1H', '30S'
        aggregate_by = config.get('data_source', {}).get('aggregate_by', None)
        if aggregate_by:
            # Парсим интервал (например, '1H', '15min', '30S', '1D')
            self.aggregate_interval = self._parse_aggregation_interval(aggregate_by)
        else:
            self.aggregate_interval = None
        # Опция для вычисления процента ошибок (error_rate)
        self.compute_error_rate = config.get('data_source', {}).get('compute_error_rate', False)
    
    def group_by_context(self, df: pd.DataFrame) -> Dict[Tuple[str, ...], pd.DataFrame]:
        """
        Group data by (metric_name × context_fields)
        
        Args:
            df: DataFrame with measurements
            
        Returns:
            Dictionary mapping (metric_name, *context_values) to DataFrame
        """
        if df.empty:
            return {}
        
        grouped = {}
        
        # Group by metric_name and context fields
        groupby_cols = [self.metric_name_field] + self.context_fields
        
        # Filter to only existing columns
        groupby_cols = [col for col in groupby_cols if col in df.columns]
        
        for group_key, group_df in df.groupby(groupby_cols):
            # Ensure group_key is a tuple
            if not isinstance(group_key, tuple):
                group_key = (group_key,)
            
            # Sort by timestamp
            if self.timestamp_field in group_df.columns:
                group_df = group_df.sort_values(by=self.timestamp_field).reset_index(drop=True)
            
            grouped[group_key] = group_df
        
        return grouped
    
    def clean_data(self, df: pd.DataFrame, remove_outliers: bool = True) -> pd.DataFrame:
        """
        Clean and preprocess data
        
        Args:
            df: DataFrame to clean
            remove_outliers: Whether to remove outliers using IQR method
            
        Returns:
            Cleaned DataFrame
        """
        if df.empty:
            return df
        
        df = df.copy()
        
        # Remove rows with missing metric values
        if self.metric_value_field in df.columns:
            df = df.dropna(subset=[self.metric_value_field])
        
        # Convert metric_value to numeric if possible
        if self.metric_value_field in df.columns:
            df[self.metric_value_field] = pd.to_numeric(
                df[self.metric_value_field], 
                errors='coerce'
            )
            # Remove rows where conversion failed
            df = df.dropna(subset=[self.metric_value_field])
        
        # Aggregate by interval if requested in config
        if self.aggregate_interval and self.timestamp_field in df.columns:
            if self.compute_error_rate:
                df = self._aggregate_error_rate(df, self.aggregate_interval)
            else:
                df = self._aggregate_by_interval(df, self.aggregate_interval)
        
        # Remove outliers if requested
        if remove_outliers and self.metric_value_field in df.columns:
            df = self._remove_outliers_iqr(df, self.metric_value_field)
        
        # Sort by timestamp
        if self.timestamp_field in df.columns:
            df = df.sort_values(by=self.timestamp_field).reset_index(drop=True)
        
        return df
    
    def _parse_aggregation_interval(self, interval_str: str) -> str:
        """
        Parse aggregation interval string to pandas frequency string
        
        Args:
            interval_str: Interval string like 'H', '1H', '15min', '30S', '1D'
            
        Returns:
            Pandas frequency string
        """
        # Маппинг единиц измерения
        # Используем 'h' вместо 'H' для совместимости с новыми версиями pandas
        unit_mapping = {
            'S': 'S',      # секунды
            's': 'S',
            'sec': 'S',
            'min': 'min',  # минуты
            'm': 'min',
            'H': 'h',      # часы (H устарело, используем h)
            'h': 'h',
            'hour': 'h',
            'D': 'D',      # дни
            'd': 'D',
            'day': 'D'
        }
        
        # Если просто единица измерения (например, 'H', 'min')
        if interval_str in unit_mapping:
            return unit_mapping[interval_str]
        
        # Если число + единица (например, '1H', '15min', '30S')
        match = re.match(r'^(\d+)(\w+)$', interval_str)
        if match:
            number = match.group(1)
            unit = match.group(2)
            if unit in unit_mapping:
                return number + unit_mapping[unit]
        
        # Если не распознано, возвращаем как есть (pandas может понять)
        return interval_str
    
    def _aggregate_by_interval(self, df: pd.DataFrame, interval: str) -> pd.DataFrame:
        """
        Aggregate data by specified interval - round timestamps and sum metric values
        
        Args:
            df: DataFrame with measurements
            interval: Pandas frequency string (e.g., 'H', '15min', '1D')
            
        Returns:
            DataFrame aggregated by interval
        """
        if df.empty or self.timestamp_field not in df.columns:
            return df
        
        df = df.copy()
        
        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(df[self.timestamp_field]):
            df[self.timestamp_field] = pd.to_datetime(df[self.timestamp_field])
        else:
            # Check if timestamps are incorrectly converted (1970 year)
            # This can happen if YDB returns microseconds but they were interpreted as nanoseconds
            if len(df) > 0:
                first_ts = df[self.timestamp_field].iloc[0]
                if isinstance(first_ts, pd.Timestamp) and first_ts.year < 2000:
                    # Fix: use nanoseconds value as microseconds
                    ns_values = df[self.timestamp_field].astype('int64')
                    df[self.timestamp_field] = pd.to_datetime(ns_values, unit='us')
        
        # Round timestamp to specified interval
        df[self.timestamp_field] = df[self.timestamp_field].dt.floor(interval)
        
        # Group by timestamp, metric_name, and context fields
        groupby_cols = [self.timestamp_field, self.metric_name_field] + self.context_fields
        groupby_cols = [col for col in groupby_cols if col in df.columns]
        
        # Aggregate: determine method per metric group
        # For count metrics (error_count, etc.) use sum, for others (duration_ms, etc.) use mean
        # Group by metric_name first, then aggregate each metric separately
        if self.metric_name_field in df.columns:
            result_dfs = []
            for metric_name, metric_df in df.groupby(self.metric_name_field):
                use_sum = 'count' in str(metric_name).lower()
                agg_method = 'sum' if use_sum else 'mean'
                
                agg_dict = {self.metric_value_field: agg_method}
                other_cols = [col for col in metric_df.columns 
                             if col not in groupby_cols + [self.metric_value_field]]
                for col in other_cols:
                    agg_dict[col] = 'first'
                
                metric_aggregated = metric_df.groupby(groupby_cols, as_index=False).agg(agg_dict)
                result_dfs.append(metric_aggregated)
            
            df_aggregated = pd.concat(result_dfs, ignore_index=True) if result_dfs else pd.DataFrame()
        else:
            # Fallback: use mean if no metric_name field
            agg_dict = {self.metric_value_field: 'mean'}
            other_cols = [col for col in df.columns if col not in groupby_cols + [self.metric_value_field]]
            for col in other_cols:
                agg_dict[col] = 'first'
            df_aggregated = df.groupby(groupby_cols, as_index=False).agg(agg_dict)
        
        return df_aggregated.reset_index(drop=True)
    
    def _aggregate_error_rate(self, df: pd.DataFrame, interval: str) -> pd.DataFrame:
        """
        Aggregate data by specified interval and compute error rate (percentage of errors)
        
        Args:
            df: DataFrame with measurements (must have 'status' column)
            interval: Pandas frequency string (e.g., 'H', '15min', '1D')
            
        Returns:
            DataFrame aggregated by interval with error_rate as metric_value
        """
        if df.empty or self.timestamp_field not in df.columns:
            return df
        
        if 'status' not in df.columns:
            raise ValueError("Column 'status' is required for error_rate computation")
        
        df = df.copy()
        
        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(df[self.timestamp_field]):
            df[self.timestamp_field] = pd.to_datetime(df[self.timestamp_field])
        else:
            # Check if timestamps are incorrectly converted (1970 year)
            # This can happen if YDB returns microseconds but they were interpreted as nanoseconds
            if len(df) > 0:
                first_ts = df[self.timestamp_field].iloc[0]
                if isinstance(first_ts, pd.Timestamp) and first_ts.year < 2000:
                    # Fix: use nanoseconds value as microseconds
                    ns_values = df[self.timestamp_field].astype('int64')
                    df[self.timestamp_field] = pd.to_datetime(ns_values, unit='us')
        
        # Round timestamp to specified interval
        df[self.timestamp_field] = df[self.timestamp_field].dt.floor(interval)
        
        # Group by timestamp, metric_name, and context fields
        groupby_cols = [self.timestamp_field, self.metric_name_field] + self.context_fields
        groupby_cols = [col for col in groupby_cols if col in df.columns]
        
        # Compute error rate for each group
        def compute_error_rate(group):
            total = len(group)
            errors = len(group[group['status'] == 'error'])
            error_rate = (errors / total * 100.0) if total > 0 else 0.0
            return error_rate
        
        # Aggregate
        result_rows = []
        for group_key, group_df in df.groupby(groupby_cols):
            error_rate = compute_error_rate(group_df)
            
            # Get first row for other columns
            first_row = group_df.iloc[0].to_dict()
            first_row[self.metric_value_field] = error_rate
            # Remove status column as it's not needed after aggregation
            if 'status' in first_row:
                del first_row['status']
            
            result_rows.append(first_row)
        
        df_aggregated = pd.DataFrame(result_rows)
        
        return df_aggregated.reset_index(drop=True)
    
    def _remove_outliers_iqr(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Remove outliers using Interquartile Range (IQR) method
        
        Args:
            df: DataFrame
            column: Column name to check for outliers
            
        Returns:
            DataFrame with outliers removed
        """
        if column not in df.columns:
            return df
        
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        
        # Define outlier bounds
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        # Filter outliers
        mask = (df[column] >= lower_bound) & (df[column] <= upper_bound)
        return df[mask].reset_index(drop=True)
    
    def prepare_time_series(self, df: pd.DataFrame) -> pd.Series:
        """
        Prepare time series from DataFrame
        
        Args:
            df: DataFrame with measurements
            
        Returns:
            Series with timestamp as index and metric_value as values
        """
        if df.empty or self.timestamp_field not in df.columns or self.metric_value_field not in df.columns:
            return pd.Series(dtype=float)
        
        # Set timestamp as index
        series = df.set_index(self.timestamp_field)[self.metric_value_field]
        
        # Sort by timestamp
        series = series.sort_index()
        
        return series
    
    def _convert_to_native_type(self, value: Any) -> Any:
        """
        Convert numpy/pandas types to native Python types for JSON serialization
        
        Args:
            value: Value that may be numpy/pandas type
            
        Returns:
            Native Python type
        """
        if isinstance(value, (np.integer, np.intc, np.intp, np.int8,
                              np.int16, np.int32, np.int64, np.uint8, np.uint16,
                              np.uint32, np.uint64)):
            return int(value)
        elif isinstance(value, (np.floating, np.float16, np.float32, np.float64)):
            return float(value)
        elif isinstance(value, np.bool_):
            return bool(value)
        elif isinstance(value, np.ndarray):
            return value.tolist()
        elif pd.isna(value):
            return None
        else:
            return value
    
    def compute_context_hash(self, context_values: Dict[str, Any]) -> str:
        """
        Compute hash for context values
        
        Args:
            context_values: Dictionary of context field -> value
            
        Returns:
            Hexadecimal hash string
        """
        # Convert numpy/pandas types to native Python types for JSON serialization
        serializable_context = {
            k: self._convert_to_native_type(v) for k, v in context_values.items()
        }
        
        # Create a sorted representation of context
        context_str = json.dumps(serializable_context, sort_keys=True)
        
        # Compute hash
        hash_obj = hashlib.md5(context_str.encode('utf-8'))
        return hash_obj.hexdigest()
    
    def extract_context_from_group_key(self, group_key: Tuple[str, ...]) -> Dict[str, Any]:
        """
        Extract context dictionary from group key
        
        Args:
            group_key: Tuple of (metric_name, *context_values)
            
        Returns:
            Dictionary mapping context fields to values
        """
        if len(group_key) < 1:
            return {}
        
        # First element is metric_name, rest are context values
        context_values = {}
        
        if len(group_key) > 1:
            for i, field in enumerate(self.context_fields, start=1):
                if i < len(group_key):
                    context_values[field] = group_key[i]
        
        return context_values
    
    def validate_group_data(self, df: pd.DataFrame, min_points: int = 3) -> bool:
        """
        Validate that group has enough data points for analysis
        
        Args:
            df: DataFrame to validate
            min_points: Minimum number of data points required
            
        Returns:
            True if valid, False otherwise
        """
        if df.empty:
            return False
        
        if len(df) < min_points:
            return False
        
        # Check that we have metric values
        if self.metric_value_field not in df.columns:
            return False
        
        # Check that we have at least some non-null values
        if df[self.metric_value_field].isna().all():
            return False
        
        return True

