#!/usr/bin/env python3

"""
Context tracker for detecting new and disappeared contexts
with configurable absence detection rules
"""

import pandas as pd
from typing import Dict, Any, Set, Tuple, Optional, List
from datetime import datetime, timedelta
from collections import defaultdict


class ContextTracker:
    """Track context appearance/disappearance with configurable rules"""
    
    def __init__(self, config: Dict[str, Any], preprocessing):
        """
        Initialize context tracker
        
        Args:
            config: Configuration dictionary
            preprocessing: Preprocessing instance for grouping
        """
        self.config = config
        self.preprocessing = preprocessing
        self.context_tracking_config = config.get('context_tracking', {})
        self.track_new = self.context_tracking_config.get('track_new_contexts', False)
        self.track_disappeared = self.context_tracking_config.get('track_disappeared_contexts', False)
        self.context_change_rules = self.context_tracking_config.get('context_change_rules', {})
    
    def detect_context_changes(self, df_all: pd.DataFrame,
                              event_start_ts: Optional[datetime],
                              event_end_ts: Optional[datetime]) -> Dict[str, Any]:
        """
        Detect new and disappeared contexts with configurable rules
        
        Args:
            df_all: All data (for historical comparison)
            event_start_ts: Start of event window (None = analyze all)
            event_end_ts: End of event window (None = analyze all)
            
        Returns:
            Dictionary with:
            - 'new': set of new context keys
            - 'disappeared': set of disappeared context keys
            - 'new_with_rules': dict mapping context_key -> rule config
            - 'disappeared_with_rules': dict mapping context_key -> rule config
        """
        result = {
            'new': set(),
            'disappeared': set(),
            'new_with_rules': {},
            'disappeared_with_rules': {}
        }
        
        if not (self.track_new or self.track_disappeared):
            return result
        
        timestamp_field = self.config.get('timestamp_field')
        if not timestamp_field or timestamp_field not in df_all.columns:
            return result
        
        # If no event window specified, use all data
        if event_start_ts is None or event_end_ts is None:
            # Compare first half vs second half
            if len(df_all) > 0:
                sorted_df = df_all.sort_values(by=timestamp_field)
                mid_point = len(sorted_df) // 2
                event_start_ts = sorted_df.iloc[mid_point][timestamp_field]
                event_end_ts = sorted_df.iloc[-1][timestamp_field]
            else:
                return result
        
        # Split data into before and in window
        df_before = df_all[df_all[timestamp_field] < event_start_ts].copy()
        df_in_window = df_all[
            (df_all[timestamp_field] >= event_start_ts) &
            (df_all[timestamp_field] <= event_end_ts)
        ].copy()
        
        if df_before.empty and df_in_window.empty:
            return result
        
        # Group contexts
        grouped_before = self.preprocessing.group_by_context(df_before) if not df_before.empty else {}
        grouped_in_window = self.preprocessing.group_by_context(df_in_window) if not df_in_window.empty else {}
        
        contexts_before = set(grouped_before.keys())
        contexts_in_window = set(grouped_in_window.keys())
        
        # Detect new contexts
        if self.track_new:
            new_contexts = contexts_in_window - contexts_before
            result['new'] = new_contexts
            
            # Apply rules for new contexts
            new_rules = self.context_change_rules.get('new_context_metrics', {})
            for context_key in new_contexts:
                metric_name = context_key[0] if isinstance(context_key, tuple) and len(context_key) > 0 else None
                if metric_name and metric_name in new_rules:
                    result['new_with_rules'][context_key] = new_rules[metric_name]
        
        # Detect disappeared contexts with rules
        if self.track_disappeared:
            disappeared_rules = self.context_change_rules.get('disappeared_context_metrics', {})
            
            for context_key in contexts_before:
                metric_name = context_key[0] if isinstance(context_key, tuple) and len(context_key) > 0 else None
                
                if metric_name and metric_name in disappeared_rules:
                    rule_config = disappeared_rules[metric_name]
                    
                    # Check if context truly disappeared according to rules
                    if self._check_context_disappeared(
                        context_key, grouped_before, grouped_in_window,
                        df_all, timestamp_field, event_start_ts, event_end_ts,
                        rule_config
                    ):
                        result['disappeared'].add(context_key)
                        result['disappeared_with_rules'][context_key] = rule_config
                else:
                    # No rule - simple check: was before, not in window
                    if context_key not in contexts_in_window:
                        result['disappeared'].add(context_key)
        
        return result
    
    def _check_context_disappeared(self, context_key: Tuple,
                                   grouped_before: Dict,
                                   grouped_in_window: Dict,
                                   df_all: pd.DataFrame,
                                   timestamp_field: str,
                                   event_start_ts: datetime,
                                   event_end_ts: datetime,
                                   rule_config: Dict[str, Any]) -> bool:
        """
        Check if context truly disappeared according to rules
        
        Args:
            context_key: Context key to check
            grouped_before: Grouped data before window
            grouped_in_window: Grouped data in window
            df_all: All data
            timestamp_field: Timestamp field name
            event_start_ts: Start of event window
            event_end_ts: End of event window
            rule_config: Rule configuration for this metric
            
        Returns:
            True if context disappeared according to rules
        """
        # Simple check: if context is in window, it didn't disappear
        if context_key in grouped_in_window:
            return False
        
        # Check minimum historical points requirement
        min_historical_points = rule_config.get('min_historical_points', 1)
        if context_key in grouped_before:
            historical_df = grouped_before[context_key]
            if len(historical_df) < min_historical_points:
                # Not enough historical data - don't consider it as disappeared
                return False
        
        # Get data for this context in the event window
        context_df = df_all[
            (df_all[timestamp_field] >= event_start_ts) &
            (df_all[timestamp_field] <= event_end_ts)
        ].copy()
        
        # Filter to this specific context
        # We need to match all context fields
        context_values = self.preprocessing.extract_context_from_group_key(context_key)
        for field, value in context_values.items():
            if field in context_df.columns:
                context_df = context_df[context_df[field] == value]
        
        # Check if we have metric_name field to filter
        metric_name = context_key[0] if isinstance(context_key, tuple) and len(context_key) > 0 else None
        metric_name_field = self.config.get('metric_fields', [None])[0]
        if metric_name and metric_name_field and metric_name_field in context_df.columns:
            context_df = context_df[context_df[metric_name_field] == metric_name]
        
        if context_df.empty:
            # No data in window - check absence rules
            return self._check_absence_rules(
                context_key, grouped_before, df_all,
                timestamp_field, event_start_ts, event_end_ts,
                rule_config
            )
        
        return False
    
    def _check_absence_rules(self, context_key: Tuple,
                            grouped_before: Dict,
                            df_all: pd.DataFrame,
                            timestamp_field: str,
                            event_start_ts: datetime,
                            event_end_ts: datetime,
                            rule_config: Dict[str, Any]) -> bool:
        """
        Check absence rules (min_absence_points or min_absence_duration_minutes)
        
        Args:
            context_key: Context key
            grouped_before: Historical grouped data
            df_all: All data
            timestamp_field: Timestamp field
            event_start_ts: Start of event window
            event_end_ts: End of event window
            rule_config: Rule configuration
            
        Returns:
            True if absence rules are satisfied
        """
        # Get historical data for this context
        if context_key not in grouped_before:
            return False
        
        historical_df = grouped_before[context_key].copy()
        if historical_df.empty:
            return False
        
        # Get last occurrence time
        last_occurrence = historical_df[timestamp_field].max()
        
        # Check if we have enough absence
        min_absence_duration = rule_config.get('min_absence_duration_minutes')
        min_absence_points = rule_config.get('min_absence_points')
        absence_type = rule_config.get('absence_type', 'consecutive')
        
        # If duration is specified, use it
        if min_absence_duration is not None:
            absence_duration = (event_end_ts - last_occurrence).total_seconds() / 60
            return absence_duration >= min_absence_duration
        
        # Otherwise use points-based check
        if min_absence_points is None:
            # No rule specified - default to 1 point
            min_absence_points = 1
        
        # Get aggregation interval from config
        aggregate_by = self.config.get('data_source', {}).get('aggregate_by')
        
        if aggregate_by:
            # We have aggregation - need to check points in aggregated data
            # Create a time series from event_start_ts to event_end_ts
            # and check how many points are missing
            
            # Parse aggregation interval
            interval = self._parse_aggregation_interval(aggregate_by)
            
            # Generate expected time points
            expected_points = pd.date_range(
                start=event_start_ts,
                end=event_end_ts,
                freq=interval
            )
            
            # Get actual data points in window for this context
            context_values = self.preprocessing.extract_context_from_group_key(context_key)
            metric_name = context_key[0] if isinstance(context_key, tuple) and len(context_key) > 0 else None
            metric_name_field = self.config.get('metric_fields', [None])[0]
            
            window_df = df_all[
                (df_all[timestamp_field] >= event_start_ts) &
                (df_all[timestamp_field] <= event_end_ts)
            ].copy()
            
            # Filter to this context
            for field, value in context_values.items():
                if field in window_df.columns:
                    window_df = window_df[window_df[field] == value]
            
            if metric_name and metric_name_field and metric_name_field in window_df.columns:
                window_df = window_df[window_df[metric_name_field] == metric_name]
            
            if window_df.empty:
                # No data at all in window
                if absence_type == 'consecutive':
                    # Check if we have enough consecutive missing points from the end
                    # Count from last_occurrence to event_end_ts
                    missing_points = pd.date_range(
                        start=last_occurrence + pd.Timedelta(interval),
                        end=event_end_ts,
                        freq=interval
                    )
                    return len(missing_points) >= min_absence_points
                else:
                    # Total absence
                    missing_points = pd.date_range(
                        start=event_start_ts,
                        end=event_end_ts,
                        freq=interval
                    )
                    return len(missing_points) >= min_absence_points
            
            # We have some data - check consecutive absence
            if absence_type == 'consecutive':
                # Find the longest consecutive absence from the end
                window_df = window_df.sort_values(by=timestamp_field)
                window_df[timestamp_field] = pd.to_datetime(window_df[timestamp_field])
                
                # Round timestamps to aggregation interval
                window_df[timestamp_field] = window_df[timestamp_field].dt.floor(interval)
                actual_points = set(window_df[timestamp_field].unique())
                
                # Check consecutive absence from the end
                consecutive_absence = 0
                current_time = event_end_ts
                
                while current_time >= event_start_ts:
                    current_rounded = pd.Timestamp(current_time).floor(interval)
                    if current_rounded not in actual_points:
                        consecutive_absence += 1
                    else:
                        break
                    current_time = current_time - pd.Timedelta(interval)
                
                return consecutive_absence >= min_absence_points
            else:
                # Total absence - count missing points
                all_points = pd.date_range(start=event_start_ts, end=event_end_ts, freq=interval)
                window_df[timestamp_field] = pd.to_datetime(window_df[timestamp_field])
                window_df[timestamp_field] = window_df[timestamp_field].dt.floor(interval)
                actual_points = set(window_df[timestamp_field].unique())
                missing_count = len(all_points) - len(actual_points.intersection(set(all_points)))
                return missing_count >= min_absence_points
        
        else:
            # No aggregation - use time-based check
            # Check if enough time has passed since last occurrence
            time_since_last = (event_end_ts - last_occurrence).total_seconds() / 60
            
            # Estimate points based on median interval in historical data
            if len(historical_df) > 1:
                historical_df = historical_df.sort_values(by=timestamp_field)
                intervals = historical_df[timestamp_field].diff().dropna()
                median_interval_minutes = intervals.median().total_seconds() / 60 if len(intervals) > 0 else 60
                estimated_points = time_since_last / median_interval_minutes
                return estimated_points >= min_absence_points
            
            return False
    
    def _parse_aggregation_interval(self, interval_str: str) -> str:
        """Parse aggregation interval string to pandas frequency"""
        import re
        
        unit_mapping = {
            'S': 'S', 's': 'S', 'sec': 'S',
            'min': 'min', 'm': 'min',
            'H': 'h', 'h': 'h', 'hour': 'h',
            'D': 'D', 'd': 'D', 'day': 'D'
        }
        
        if interval_str in unit_mapping:
            return unit_mapping[interval_str]
        
        match = re.match(r'^(\d+)(\w+)$', interval_str)
        if match:
            number = match.group(1)
            unit = match.group(2)
            if unit in unit_mapping:
                return number + unit_mapping[unit]
        
        return interval_str

