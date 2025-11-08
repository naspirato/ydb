#!/usr/bin/env python3

import pandas as pd
import sys
import os
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

# Add parent directory to path to import ydb_wrapper
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from ydb_wrapper import YDBWrapper


class DataAccess:
    """Data access layer for loading measurements from YDB"""
    
    def __init__(self, ydb_wrapper: YDBWrapper, config: Dict[str, Any]):
        """
        Initialize data access layer
        
        Args:
            ydb_wrapper: YDBWrapper instance for YDB operations
            config: Configuration dictionary
        """
        self.ydb_wrapper = ydb_wrapper
        self.config = config
        self.data_source = config['data_source']['ydb']
        self.query = self.data_source['query']
    
    def load_measurements(self, start_ts: Optional[datetime] = None, 
                         end_ts: Optional[datetime] = None) -> pd.DataFrame:
        """
        Load measurements from YDB based on configuration
        
        Args:
            start_ts: Optional start timestamp for filtering
            end_ts: Optional end timestamp for filtering
            
        Returns:
            DataFrame with measurements
        """
        # Build query with optional timestamp parameters
        query = self._build_query(start_ts, end_ts)
        
        # Execute query through ydb_wrapper
        query_name = self.config.get('job', {}).get('name', 'analytics_query')
        results = self.ydb_wrapper.execute_scan_query(query, query_name=query_name)
        
        if not results:
            return pd.DataFrame()
        
        # Convert YDB row objects to dictionaries for pandas
        # YDB row objects don't have 'ndim' attribute that pandas expects
        # Also convert bytes to strings as YDB may return string columns as bytes
        dict_results = []
        for row in results:
            row_dict = dict(row)
            # Convert bytes values to strings
            for key, value in row_dict.items():
                if isinstance(value, bytes):
                    row_dict[key] = value.decode('utf-8')
            dict_results.append(row_dict)
        
        # Convert to DataFrame
        df = pd.DataFrame(dict_results)
        
        # Validate required columns
        self._validate_dataframe(df)
        
        # Convert timestamp field to datetime if it's not already
        timestamp_field = self.config['timestamp_field']
        if timestamp_field in df.columns and len(df) > 0:
            # Check if it's already a datetime-like type
            if pd.api.types.is_datetime64_any_dtype(df[timestamp_field]):
                # Already datetime, but check if it's incorrectly converted (1970 year)
                sample_ts = df[timestamp_field].iloc[0]
                if isinstance(sample_ts, pd.Timestamp) and sample_ts.year < 2000:
                    # Timestamps were incorrectly converted to 1970
                    # This happens when YDB returns microseconds but pandas/YDB driver interprets as nanoseconds
                    # To fix: use the nanoseconds value as microseconds (they are the same number)
                    ns_values = df[timestamp_field].astype('int64')
                    df[timestamp_field] = pd.to_datetime(ns_values, unit='us')
            elif pd.api.types.is_integer_dtype(df[timestamp_field]) or pd.api.types.is_float_dtype(df[timestamp_field]):
                # If it's a number, try to determine the unit
                # YDB Timestamp type is microseconds since epoch
                sample_values = df[timestamp_field].dropna().head(10)
                if len(sample_values) > 0:
                    # Check value ranges to determine unit
                    min_val = sample_values.min()
                    max_val = sample_values.max()
                    
                    # Microseconds for 2000-01-01 = 946684800000000
                    # Microseconds for 2100-01-01 = 4102444800000000
                    min_reasonable_us = 946684800000000  # 2000-01-01 in microseconds
                    max_reasonable_us = 4102444800000000  # 2100-01-01 in microseconds
                    
                    # Check if values are in microseconds range
                    if min_val >= min_reasonable_us and max_val <= max_reasonable_us:
                        # Definitely microseconds
                        df[timestamp_field] = pd.to_datetime(df[timestamp_field], unit='us')
                        # Verify conversion was correct
                        if df[timestamp_field].iloc[0].year < 2000:
                            # Fix: use nanoseconds value as microseconds
                            ns_values = df[timestamp_field].astype('int64')
                            df[timestamp_field] = pd.to_datetime(ns_values, unit='us')
                    # Check if values are in milliseconds range
                    elif min_val >= 946684800000 and max_val <= 4102444800000:
                        df[timestamp_field] = pd.to_datetime(df[timestamp_field], unit='ms')
                        if df[timestamp_field].iloc[0].year < 2000:
                            ns_values = df[timestamp_field].astype('int64')
                            df[timestamp_field] = pd.to_datetime(ns_values, unit='us')
                    # Check if values are in seconds range
                    elif min_val >= 946684800 and max_val <= 4102444800:
                        df[timestamp_field] = pd.to_datetime(df[timestamp_field], unit='s')
                        if df[timestamp_field].iloc[0].year < 2000:
                            ns_values = df[timestamp_field].astype('int64')
                            df[timestamp_field] = pd.to_datetime(ns_values, unit='us')
                    else:
                        # Try microseconds first (most common for YDB)
                        try:
                            df[timestamp_field] = pd.to_datetime(df[timestamp_field], unit='us')
                            if df[timestamp_field].iloc[0].year < 2000:
                                ns_values = df[timestamp_field].astype('int64')
                                df[timestamp_field] = pd.to_datetime(ns_values, unit='us')
                        except:
                            df[timestamp_field] = pd.to_datetime(df[timestamp_field])
                else:
                    # No data, try microseconds (YDB default)
                    df[timestamp_field] = pd.to_datetime(df[timestamp_field], unit='us')
            else:
                # String or other type, use default conversion
                df[timestamp_field] = pd.to_datetime(df[timestamp_field])
        elif timestamp_field in df.columns:
            # Empty dataframe, just ensure column type
            df[timestamp_field] = pd.to_datetime(df[timestamp_field])
        
        # Sort by timestamp
        df = df.sort_values(by=timestamp_field).reset_index(drop=True)
        
        return df
    
    def _build_query(self, start_ts: Optional[datetime] = None, 
                    end_ts: Optional[datetime] = None) -> str:
        """
        Build SQL query with optional timestamp parameters
        
        Args:
            start_ts: Optional start timestamp
            end_ts: Optional end timestamp
            
        Returns:
            SQL query string
        """
        query = self.query
        
        # If query already has WHERE clause, we might need to extend it
        # For simplicity, we'll assume the query in config is complete
        # In a more advanced version, we could parse and modify the WHERE clause
        
        # If timestamps are provided and query doesn't have them, add them
        # This is a simple implementation - in production you might want SQL parsing
        if start_ts or end_ts:
            timestamp_field = self.config['timestamp_field']
            
            # Format timestamps for YDB
            conditions = []
            if start_ts:
                ydb_start = start_ts.strftime('%Y-%m-%dT%H:%M:%SZ')
                conditions.append(f"{timestamp_field} >= Datetime(\"{ydb_start}\")")
            
            if end_ts:
                ydb_end = end_ts.strftime('%Y-%m-%dT%H:%M:%SZ')
                conditions.append(f"{timestamp_field} <= Datetime(\"{ydb_end}\")")
            
            if conditions:
                condition_str = " AND ".join(conditions)
                # Simple check if WHERE already exists
                query_upper = query.upper()
                if 'WHERE' in query_upper:
                    # Append to existing WHERE clause
                    query = f"{query} AND {condition_str}"
                else:
                    # Add WHERE clause
                    query = f"{query} WHERE {condition_str}"
        
        return query
    
    def _validate_dataframe(self, df: pd.DataFrame):
        """
        Validate that DataFrame has all required columns
        
        Args:
            df: DataFrame to validate
            
        Raises:
            ValueError: If required columns are missing
        """
        required_fields = (
            [self.config['timestamp_field']] +
            self.config['context_fields'] +
            self.config['metric_fields']
        )
        
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        if missing_fields:
            raise ValueError(
                f"Missing required fields in query results: {', '.join(missing_fields)}. "
                f"Available fields: {', '.join(df.columns)}"
            )
    
    def get_data_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get summary statistics about loaded data
        
        Args:
            df: DataFrame with measurements
            
        Returns:
            Dictionary with summary statistics
        """
        if df.empty:
            return {
                'total_rows': 0,
                'date_range': None,
                'metrics': [],
                'context_combinations': 0
            }
        
        timestamp_field = self.config['timestamp_field']
        metric_name_field = self.config['metric_fields'][0]  # First metric field is metric_name
        
        summary = {
            'total_rows': len(df),
            'date_range': {
                'start': df[timestamp_field].min().isoformat() if timestamp_field in df.columns else None,
                'end': df[timestamp_field].max().isoformat() if timestamp_field in df.columns else None
            },
            'metrics': df[metric_name_field].unique().tolist() if metric_name_field in df.columns else [],
            'context_combinations': 0
        }
        
        # Count unique context combinations
        if self.config['context_fields']:
            context_cols = [col for col in self.config['context_fields'] if col in df.columns]
            if context_cols:
                summary['context_combinations'] = df[context_cols].drop_duplicates().shape[0]
        
        return summary

