#!/usr/bin/env python3

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import warnings

# Optional imports for advanced methods
try:
    from adtk.detector import LevelShiftAD, VolatilityShiftAD
    ADTK_AVAILABLE = True
except ImportError:
    ADTK_AVAILABLE = False

try:
    from pyod.models.iforest import IForest
    PYOD_AVAILABLE = True
except ImportError:
    PYOD_AVAILABLE = False


class EventDetector:
    """Detect events in time series data (degradations, improvements, threshold shifts)"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize event detector
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.events_config = config['events']
        self.analytics_config = config['analytics']
        self.detect_types = self.events_config['detect']
        self.min_event_duration = timedelta(
            minutes=self.events_config.get('min_event_duration_minutes', 30)
        )
        # Get default parameters
        self.min_absolute_change_default = self.analytics_config.get('min_absolute_change', 0)
        self.min_relative_change_default = self.analytics_config.get('min_relative_change', 0.0)
        self.hysteresis_points = self.analytics_config.get('hysteresis_points', 3)
        
        # Get metric-specific parameters
        self.metric_specific_params = self.analytics_config.get('metric_specific_params', {})
        
        # Metric direction: how to interpret metric changes
        # "negative" - больше = хуже (duration_ms, error_rate) - рост = degradation, падение = improvement
        # "positive" - больше = лучше (throughput, success_rate) - рост = improvement, падение = degradation
        metric_direction_config = config.get('metric_direction', {})
        self.metric_direction_default = metric_direction_config.get('default', 'negative')
        self.metric_direction_map = {k: v for k, v in metric_direction_config.items() if k != 'default'}
    
    def _get_metric_params(self, metric_name: str) -> tuple:
        """
        Get detection parameters for specific metric
        
        Returns:
            (min_absolute_change, min_relative_change) tuple
        """
        if metric_name and metric_name in self.metric_specific_params:
            params = self.metric_specific_params[metric_name]
            min_abs = params.get('min_absolute_change', self.min_absolute_change_default)
            min_rel = params.get('min_relative_change', self.min_relative_change_default)
            return (min_abs, min_rel)
        return (self.min_absolute_change_default, self.min_relative_change_default)
    
    def _get_metric_direction(self, metric_name: str) -> str:
        """Get metric direction (negative or positive)"""
        return self.metric_direction_map.get(metric_name, self.metric_direction_default)
    
    def detect_events(self, series: pd.Series, baseline_result: Dict[str, Any], metric_name: str = None) -> List[Dict[str, Any]]:
        """
        Detect events in time series
        
        Args:
            series: Time series data
            baseline_result: Result from baseline calculator
            metric_name: Name of the metric (for direction detection)
            
        Returns:
            List of detected events
        """
        if series.empty or len(series) < 2:
            return []
        
        # Get metric direction and parameters
        metric_direction = self._get_metric_direction(metric_name) if metric_name else self.metric_direction_default
        min_abs_change, min_rel_change = self._get_metric_params(metric_name)
        
        events = []
        
        # Detect different event types
        # For negative metrics: above upper = degradation, below lower = improvement
        # For positive metrics: above upper = improvement, below lower = degradation
        if 'degradation_start' in self.detect_types:
            if metric_direction == 'negative':
                # Negative metric: above upper threshold = degradation
                degradation_events = self._detect_degradation_above_threshold(series, baseline_result, min_abs_change, min_rel_change)
            else:
                # Positive metric: below lower threshold = degradation
                degradation_events = self._detect_degradation_below_threshold(series, baseline_result, min_abs_change, min_rel_change)
            events.extend(degradation_events)
        
        if 'improvement_start' in self.detect_types:
            if metric_direction == 'negative':
                # Negative metric: below lower threshold = improvement
                improvement_events = self._detect_improvement_below_threshold(series, baseline_result, min_abs_change, min_rel_change)
            else:
                # Positive metric: above upper threshold = improvement
                improvement_events = self._detect_improvement_above_threshold(series, baseline_result, min_abs_change, min_rel_change)
            events.extend(improvement_events)
        
        if 'threshold_shift' in self.detect_types:
            shift_events = self._detect_threshold_shift(series, baseline_result)
            events.extend(shift_events)
        
        # Filter events by duration and significance
        filtered_events = self._filter_events(events, series)
        
        return filtered_events
    
    def _detect_degradation_below_threshold(self, series: pd.Series, baseline_result: Dict[str, Any], 
                                           min_abs_change: float = None, min_rel_change: float = None) -> List[Dict[str, Any]]:
        """Detect degradation events (values below lower threshold) - for positive metrics"""
        if min_abs_change is None:
            min_abs_change = self.min_absolute_change_default
        if min_rel_change is None:
            min_rel_change = self.min_relative_change_default
        events = []
        
        lower_threshold = baseline_result.get('lower_threshold')
        if lower_threshold is None:
            return events
        
        baseline_series = baseline_result.get('baseline_series', pd.Series())
        if baseline_series.empty:
            return events
        
        # Find points below threshold
        below_threshold = series < lower_threshold
        
        # Debug: check how many points are below threshold
        num_below = below_threshold.sum()
        
        # Find continuous segments
        segments = self._find_continuous_segments(below_threshold)
        
        # Debug: if we have points below threshold but no segments, log it
        if num_below > 0 and len(segments) == 0:
            # Points are too scattered - this is expected for very sparse data
            pass
        
        for segment_start, segment_end in segments:
            # Check if segment is significant
            segment_values = series.iloc[segment_start:segment_end+1]
            baseline_at_start = baseline_series.iloc[segment_start] if segment_start < len(baseline_series) else None
            
            if baseline_at_start is None:
                continue
            
            change_absolute = float(segment_values.mean() - baseline_at_start)
            change_relative = abs(change_absolute / baseline_at_start) if baseline_at_start != 0 else 0
            
            # Check significance
            if (abs(change_absolute) >= min_abs_change and 
                change_relative >= min_rel_change):
                
                event = {
                    'event_type': 'degradation_start',
                    'event_start_time': series.index[segment_start],
                    'event_end_time': series.index[segment_end],
                    'severity': self._calculate_severity(change_absolute, change_relative, 'degradation'),
                    'baseline_before': float(baseline_at_start),
                    'baseline_after': float(baseline_series.iloc[min(segment_end, len(baseline_series)-1)]),
                    'threshold_before': float(lower_threshold),
                    'threshold_after': float(lower_threshold),
                    'change_absolute': change_absolute,
                    'change_relative': change_relative,
                    'current_value': float(segment_values.mean()),
                }
                events.append(event)
        
        return events
    
    def _detect_degradation_above_threshold(self, series: pd.Series, baseline_result: Dict[str, Any],
                                           min_abs_change: float = None, min_rel_change: float = None) -> List[Dict[str, Any]]:
        """Detect degradation events (values above upper threshold) - for negative metrics"""
        if min_abs_change is None:
            min_abs_change = self.min_absolute_change_default
        if min_rel_change is None:
            min_rel_change = self.min_relative_change_default
        events = []
        
        upper_threshold = baseline_result.get('upper_threshold')
        if upper_threshold is None:
            return events
        
        baseline_series = baseline_result.get('baseline_series', pd.Series())
        if baseline_series.empty:
            return events
        
        # Find points above threshold
        above_threshold = series > upper_threshold
        
        # Debug: check how many points are above threshold
        num_above = above_threshold.sum()
        
        # Find continuous segments
        segments = self._find_continuous_segments(above_threshold)
        
        # Debug: if we have points above threshold but no segments, log it
        if num_above > 0 and len(segments) == 0:
            # Points are too scattered - this is expected for very sparse data
            pass
        
        for segment_start, segment_end in segments:
            # Check if segment is significant
            segment_values = series.iloc[segment_start:segment_end+1]
            baseline_at_start = baseline_series.iloc[segment_start] if segment_start < len(baseline_series) else None
            
            if baseline_at_start is None:
                continue
            
            change_absolute = float(segment_values.mean() - baseline_at_start)
            change_relative = abs(change_absolute / baseline_at_start) if baseline_at_start != 0 else 0
            
            # Check significance
            if (abs(change_absolute) >= min_abs_change and 
                change_relative >= min_rel_change):
                
                event = {
                    'event_type': 'degradation_start',
                    'event_start_time': series.index[segment_start],
                    'event_end_time': series.index[segment_end],
                    'severity': self._calculate_severity(change_absolute, change_relative, 'degradation'),
                    'baseline_before': float(baseline_at_start),
                    'baseline_after': float(baseline_series.iloc[min(segment_end, len(baseline_series)-1)]),
                    'threshold_before': float(upper_threshold),
                    'threshold_after': float(upper_threshold),
                    'change_absolute': change_absolute,
                    'change_relative': change_relative,
                    'current_value': float(segment_values.mean()),
                }
                events.append(event)
        
        return events
    
    def _detect_improvement_below_threshold(self, series: pd.Series, baseline_result: Dict[str, Any],
                                           min_abs_change: float = None, min_rel_change: float = None) -> List[Dict[str, Any]]:
        """Detect improvement events (values significantly below baseline/norm) - for negative metrics"""
        if min_abs_change is None:
            min_abs_change = self.min_absolute_change_default
        if min_rel_change is None:
            min_rel_change = self.min_relative_change_default
        events = []
        
        baseline_series = baseline_result.get('baseline_series', pd.Series())
        if baseline_series.empty:
            return events
        
        # For improvement: detect values significantly below baseline (norm)
        # This catches improvements relative to normal performance, not just below lower_threshold
        # Align baseline with series index
        baseline_values = baseline_series.reindex(series.index, method='nearest')
        
        # Calculate improvement threshold: baseline - min_absolute_change
        # OR baseline * (1 - min_relative_change) - whichever is more restrictive
        improvement_threshold_abs = baseline_values - min_abs_change
        improvement_threshold_rel = baseline_values * (1 - min_rel_change)
        # Use the more restrictive (lower) threshold
        improvement_threshold = pd.concat([improvement_threshold_abs, improvement_threshold_rel], axis=1).min(axis=1)
        
        # Find points significantly below baseline (improvement)
        below_baseline_improvement = series < improvement_threshold
        
        # Find continuous segments
        segments = self._find_continuous_segments(below_baseline_improvement)
        
        for segment_start, segment_end in segments:
            segment_values = series.iloc[segment_start:segment_end+1]
            baseline_at_start = baseline_series.iloc[segment_start] if segment_start < len(baseline_series) else None
            
            if baseline_at_start is None:
                continue
            
            change_absolute = float(segment_values.mean() - baseline_at_start)
            change_relative = abs(change_absolute / baseline_at_start) if baseline_at_start != 0 else 0
            
            # Check significance (change must be negative for improvement in negative metrics)
            if (change_absolute < 0 and  # Must be improvement (lower value)
                abs(change_absolute) >= min_abs_change and 
                change_relative >= min_rel_change):
                
                event = {
                    'event_type': 'improvement_start',
                    'event_start_time': series.index[segment_start],
                    'event_end_time': series.index[segment_end],
                    'severity': self._calculate_severity(change_absolute, change_relative, 'improvement'),
                    'baseline_before': float(baseline_at_start),
                    'baseline_after': float(baseline_series.iloc[min(segment_end, len(baseline_series)-1)]),
                    'threshold_before': float(baseline_result.get('lower_threshold')) if baseline_result.get('lower_threshold') is not None else None,
                    'threshold_after': float(baseline_result.get('lower_threshold')) if baseline_result.get('lower_threshold') is not None else None,
                    'change_absolute': change_absolute,
                    'change_relative': change_relative,
                    'current_value': float(segment_values.mean()),
                }
                events.append(event)
        
        return events
    
    def _detect_improvement_above_threshold(self, series: pd.Series, baseline_result: Dict[str, Any],
                                           min_abs_change: float = None, min_rel_change: float = None) -> List[Dict[str, Any]]:
        """Detect improvement events (values above upper threshold) - for positive metrics"""
        if min_abs_change is None:
            min_abs_change = self.min_absolute_change_default
        if min_rel_change is None:
            min_rel_change = self.min_relative_change_default
        events = []
        
        upper_threshold = baseline_result.get('upper_threshold')
        if upper_threshold is None:
            return events
        
        baseline_series = baseline_result.get('baseline_series', pd.Series())
        if baseline_series.empty:
            return events
        
        # Find points above threshold
        above_threshold = series > upper_threshold
        
        # Debug: check how many points are above threshold
        num_above = above_threshold.sum()
        
        # Find continuous segments
        segments = self._find_continuous_segments(above_threshold)
        
        # Debug: if we have points above threshold but no segments, log it
        if num_above > 0 and len(segments) == 0:
            # Points are too scattered - this is expected for very sparse data
            pass
        
        for segment_start, segment_end in segments:
            # Check if segment is significant
            segment_values = series.iloc[segment_start:segment_end+1]
            baseline_at_start = baseline_series.iloc[segment_start] if segment_start < len(baseline_series) else None
            
            if baseline_at_start is None:
                continue
            
            change_absolute = float(segment_values.mean() - baseline_at_start)
            change_relative = abs(change_absolute / baseline_at_start) if baseline_at_start != 0 else 0
            
            # Check significance
            if (abs(change_absolute) >= min_abs_change and 
                change_relative >= min_rel_change):
                
                event = {
                    'event_type': 'improvement_start',
                    'event_start_time': series.index[segment_start],
                    'event_end_time': series.index[segment_end],
                    'severity': self._calculate_severity(change_absolute, change_relative, 'improvement'),
                    'baseline_before': float(baseline_at_start),
                    'baseline_after': float(baseline_series.iloc[min(segment_end, len(baseline_series)-1)]),
                    'threshold_before': float(upper_threshold),
                    'threshold_after': float(upper_threshold),
                    'change_absolute': change_absolute,
                    'change_relative': change_relative,
                    'current_value': float(segment_values.mean()),
                }
                events.append(event)
        
        return events
    
    def _detect_threshold_shift(self, series: pd.Series, baseline_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect threshold shift events (baseline level shift)"""
        events = []
        
        baseline_series = baseline_result.get('baseline_series', pd.Series())
        window_size = self.window_size
        if baseline_series.empty or len(baseline_series) < window_size * 2:
            return events
        
        # Method 1: Use ADTK if available
        if ADTK_AVAILABLE and 'adtk-levelshift' in baseline_result.get('baseline_method', ''):
            try:
                detector = LevelShiftAD(c=3.0, side='both', window=window_size)
                anomalies = detector.fit_detect(baseline_series)
                
                # Find shift points
                shift_points = baseline_series.index[anomalies].tolist()
                
                for i, shift_time in enumerate(shift_points):
                    if i == 0:
                        continue
                    
                    prev_shift = shift_points[i-1]
                    prev_idx = baseline_series.index.get_loc(prev_shift)
                    curr_idx = baseline_series.index.get_loc(shift_time)
                    
                    baseline_before = float(baseline_series.iloc[prev_idx])
                    baseline_after = float(baseline_series.iloc[curr_idx])
                    
                    change_absolute = baseline_after - baseline_before
                    change_relative = abs(change_absolute / baseline_before) if baseline_before != 0 else 0
                    
                    if (abs(change_absolute) >= self.min_absolute_change_default and 
                        change_relative >= self.min_relative_change_default):
                        
                        event = {
                            'event_type': 'threshold_shift',
                            'event_start_time': prev_shift,
                            'event_end_time': shift_time,
                            'severity': self._calculate_severity(change_absolute, change_relative, 'shift'),
                            'baseline_before': baseline_before,
                            'baseline_after': baseline_after,
                            'threshold_before': None,  # Will be computed
                            'threshold_after': None,  # Will be computed
                            'change_absolute': change_absolute,
                            'change_relative': change_relative,
                            'current_value': baseline_after,
                        }
                        events.append(event)
                
                return events
            except Exception as e:
                warnings.warn(f"ADTK threshold shift detection failed: {e}")
        
        # Method 2: Simple statistical approach
        # Compare baseline in two windows, but only detect significant sustained shifts
        # to avoid detecting every small baseline change
        window_size = min(window_size, len(baseline_series) // 2)
        
        # Track detected shifts to avoid duplicates
        detected_shifts = []
        last_shift_end = -1
        
        # Use larger step to avoid detecting every point
        step = max(1, window_size // 2)  # Check every half-window
        
        for i in range(window_size, len(baseline_series) - window_size, step):
            # Skip if we're too close to the last detected shift
            if i <= last_shift_end + window_size:
                continue
            
            window1 = baseline_series.iloc[i-window_size:i]
            window2 = baseline_series.iloc[i:i+window_size]
            
            mean1 = window1.mean()
            mean2 = window2.mean()
            
            change_absolute = mean2 - mean1
            change_relative = abs(change_absolute / mean1) if mean1 != 0 else 0
            
            # Check if change is significant (use stricter thresholds for threshold_shift)
            # Threshold shift should be more significant than regular degradation/improvement
            min_abs_for_shift = self.min_absolute_change_default * 1.5  # 50% more strict
            min_rel_for_shift = self.min_relative_change_default * 1.5  # 50% more strict
            
            if (abs(change_absolute) >= min_abs_for_shift and 
                change_relative >= min_rel_for_shift):
                
                # Check if this is a sustained shift (not just noise)
                # by looking ahead
                if i + window_size < len(baseline_series):
                    window3 = baseline_series.iloc[i+window_size:i+window_size*2]
                    mean3 = window3.mean()
                    
                    # If window3 is similar to window2, it's a shift
                    # Also check that the shift is significant compared to noise
                    shift_magnitude = abs(mean2 - mean1)
                    noise_level = abs(mean3 - mean2)
                    
                    if abs(mean3 - mean2) < abs(mean2 - mean1) * 0.5 and shift_magnitude > noise_level * 2:
                        event = {
                            'event_type': 'threshold_shift',
                            'event_start_time': baseline_series.index[i-window_size],
                            'event_end_time': baseline_series.index[i+window_size],
                            'severity': self._calculate_severity(change_absolute, change_relative, 'shift'),
                            'baseline_before': float(mean1),
                            'baseline_after': float(mean2),
                            'threshold_before': None,
                            'threshold_after': None,
                            'change_absolute': change_absolute,
                            'change_relative': change_relative,
                            'current_value': float(mean2),
                        }
                        events.append(event)
                        detected_shifts.append(i)
                        last_shift_end = i + window_size
        
        return events
    
    def _find_continuous_segments(self, condition: pd.Series) -> List[tuple]:
        """
        Find continuous segments where condition is True.
        Also handles sparse data by grouping nearby points (within reasonable time gap).
        Supports very sparse data (measurements every few days).
        """
        segments = []
        in_segment = False
        segment_start = None
        
        # Maximum gap between points to consider them part of the same segment
        # For irregular data, we allow gaps up to 3x the median interval
        # For very sparse data (every few days), we use a more generous gap
        if len(condition) > 1 and isinstance(condition.index, pd.DatetimeIndex):
            time_diffs = condition.index.to_series().diff().dropna()
            if len(time_diffs) > 0:
                median_interval = time_diffs.median()
                # For very sparse data (>= 2 days), use 3x median or 7 days, whichever is larger
                # For moderately sparse data (hours to 2 days), use 5x median or 1 day
                # This ensures we can handle measurements every few days
                if median_interval >= timedelta(days=2):
                    # Very sparse: measurements every few days
                    max_gap_candidate = median_interval * 3
                    max_gap_default = timedelta(days=7)
                    max_gap = max(max_gap_candidate, max_gap_default)
                else:
                    # Moderately sparse: measurements every few hours to daily
                    max_gap_candidate = median_interval * 5
                    max_gap_default = timedelta(days=1)
                    max_gap = max(max_gap_candidate, max_gap_default)
            else:
                max_gap = timedelta(days=7)  # Default: 7 days for very sparse data
        else:
            max_gap = None
        
        for i, value in enumerate(condition):
            if value and not in_segment:
                # Start of segment
                segment_start = i
                in_segment = True
            elif not value and in_segment:
                # End of segment
                segments.append((segment_start, i - 1))
                in_segment = False
            elif value and in_segment and max_gap is not None:
                # Check if gap is too large (for sparse data)
                if i > segment_start:
                    gap = condition.index[i] - condition.index[i-1]
                    if gap > max_gap:
                        # Gap too large, end current segment and start new one
                        segments.append((segment_start, i - 1))
                        segment_start = i
        
        # Handle segment that extends to end
        if in_segment:
            segments.append((segment_start, len(condition) - 1))
        
        return segments
    
    def _get_data_frequency(self, series: pd.Series) -> Dict[str, Any]:
        """
        Determine data frequency characteristics
        
        Returns:
            Dict with 'median_interval', 'typical_interval', 'is_sparse', 'is_dense', 'is_very_sparse'
        """
        if len(series) < 2:
            return {
                'median_interval': timedelta(hours=1),
                'typical_interval': timedelta(hours=1),
                'is_sparse': True,
                'is_dense': False,
                'is_very_sparse': False
            }
        
        if not isinstance(series.index, pd.DatetimeIndex):
            return {
                'median_interval': timedelta(hours=1),
                'typical_interval': timedelta(hours=1),
                'is_sparse': True,
                'is_dense': False,
                'is_very_sparse': False
            }
        
        time_diffs = series.index.to_series().diff().dropna()
        if len(time_diffs) == 0:
            return {
                'median_interval': timedelta(hours=1),
                'typical_interval': timedelta(hours=1),
                'is_sparse': True,
                'is_dense': False,
                'is_very_sparse': False
            }
        
        median_interval = time_diffs.median()
        # Use median as typical interval (more robust than mean for irregular data)
        typical_interval = median_interval
        
        # Classify data density
        # Dense: measurements more frequent than every 10 minutes
        # Sparse: measurements less frequent than every hour
        # Very sparse: measurements every 2+ days (e.g., weekly reports, monthly reports)
        is_dense = median_interval <= timedelta(minutes=10)
        is_sparse = median_interval >= timedelta(hours=1)
        is_very_sparse = median_interval >= timedelta(days=2)
        
        return {
            'median_interval': median_interval,
            'typical_interval': typical_interval,
            'is_sparse': is_sparse,
            'is_dense': is_dense,
            'is_very_sparse': is_very_sparse
        }
    
    def _filter_events(self, events: List[Dict[str, Any]], series: pd.Series) -> List[Dict[str, Any]]:
        """
        Filter events by duration and apply hysteresis.
        For sparse/irregular data, we consider both time duration and number of points.
        """
        if not events:
            return []
        
        # Determine data frequency once for all events
        frequency_info = self._get_data_frequency(series)
        typical_interval = frequency_info['typical_interval']
        is_sparse = frequency_info['is_sparse']
        is_dense = frequency_info['is_dense']
        is_very_sparse = frequency_info['is_very_sparse']
        
        filtered = []
        
        for event in events:
            # Check duration
            start_time = event['event_start_time']
            end_time = event['event_end_time']
            
            if isinstance(start_time, pd.Timestamp) and isinstance(end_time, pd.Timestamp):
                duration = end_time - start_time
            else:
                # Try to convert
                try:
                    start_time = pd.Timestamp(start_time)
                    end_time = pd.Timestamp(end_time)
                    duration = end_time - start_time
                except:
                    duration = timedelta(0)
            
            # Get event points
            try:
                event_points = series.loc[start_time:end_time]
                num_points = len(event_points)
            except:
                num_points = 0
            
            # Check if this is a single-point outlier (spike)
            # Use data frequency to determine what constitutes an outlier
            is_single_outlier = False
            
            if num_points == 1:
                # Single point: check if duration is too short relative to data frequency
                # For dense data (every minute): outlier if < 5-10 minutes
                # For sparse data (daily): outlier if < 1-2 days
                # For very sparse data (every few days): outlier if < 3-7 days
                if is_very_sparse:
                    min_duration_for_single_point = max(
                        typical_interval * 2,  # At least 2 typical intervals for very sparse
                        timedelta(days=3)  # Minimum 3 days for very sparse data
                    )
                elif is_dense:
                    min_duration_for_single_point = max(
                        typical_interval * 3,
                        timedelta(minutes=5)
                    )
                else:
                    min_duration_for_single_point = max(
                        typical_interval * 3,
                        timedelta(hours=1)
                    )
                
                if duration < min_duration_for_single_point:
                    is_single_outlier = True
            elif num_points == 2:
                # Two points: check if they're too close and duration is too short
                try:
                    event_points = series.loc[start_time:end_time]
                    if len(event_points) == 2:
                        time_diff = event_points.index[1] - event_points.index[0]
                        # If two points are very close (< 2x typical interval) and duration is short, likely outlier
                        if is_very_sparse:
                            min_duration_for_two_points = max(
                                typical_interval * 3,  # At least 3 typical intervals for very sparse
                                timedelta(days=5)  # Minimum 5 days for very sparse data
                            )
                            # For very sparse data, points can be days apart - that's normal
                            if time_diff < typical_interval * 1.5 and duration < min_duration_for_two_points:
                                is_single_outlier = True
                        elif is_dense:
                            min_duration_for_two_points = max(
                                typical_interval * 5,
                                timedelta(minutes=10)
                            )
                            if time_diff < typical_interval * 2 and duration < min_duration_for_two_points:
                                is_single_outlier = True
                        else:
                            min_duration_for_two_points = max(
                                typical_interval * 5,
                                timedelta(hours=2)
                            )
                            if time_diff < typical_interval * 2 and duration < min_duration_for_two_points:
                                is_single_outlier = True
                    elif len(event_points) < 2:
                        # Less than 2 points found, treat as single point
                        if is_very_sparse:
                            min_duration_for_single_point = max(
                                typical_interval * 2,
                                timedelta(days=3)
                            )
                        elif is_dense:
                            min_duration_for_single_point = max(
                                typical_interval * 3,
                                timedelta(minutes=5)
                            )
                        else:
                            min_duration_for_single_point = max(
                                typical_interval * 3,
                                timedelta(hours=1)
                            )
                        if duration < min_duration_for_single_point:
                            is_single_outlier = True
                except:
                    pass
            
            # For sparse/irregular data: if we have many points, relax duration requirement
            # If we have enough points (>= hysteresis_points * 2), we accept shorter durations
            min_duration = self.min_event_duration
            if num_points >= self.hysteresis_points * 2:
                # For events with many points, reduce duration requirement by 50%
                min_duration = self.min_event_duration * 0.5
            
            # Also relax duration for very significant changes (>= 20% relative change)
            # BUT: only if we have enough points and it's not a single outlier
            event_change_relative = abs(event.get('change_relative', 0))
            if event_change_relative >= 0.20 and not is_single_outlier and num_points >= self.hysteresis_points * 2:
                # Significant changes should be detected even if duration is short
                # But only if we have enough points (not a single outlier)
                min_duration = min_duration * 0.5  # Reduce to 50% (was 30%, too aggressive)
            
            if duration < min_duration:
                # Still check if we have enough points to compensate
                # For very significant changes, accept even with fewer points, but not for single outliers
                required_points = self.hysteresis_points * 3
                if event_change_relative >= 0.20 and not is_single_outlier:
                    # Very significant changes need at least 2x hysteresis_points (not just 1x)
                    required_points = self.hysteresis_points * 2
                if num_points < required_points:
                    continue
            
            # Apply hysteresis: require multiple consecutive points
            # For single outliers, require at least 2 points even if hysteresis_points = 1
            min_points_required = self.hysteresis_points
            if is_single_outlier:
                min_points_required = max(2, self.hysteresis_points)  # At least 2 points for outliers
            if num_points < min_points_required:
                continue
            
            filtered.append(event)
        
        return filtered
    
    def _calculate_severity(self, change_absolute: float, change_relative: float, event_type: str) -> str:
        """Calculate event severity"""
        # Simple severity calculation based on relative change
        if change_relative < 0.05:
            return 'low'
        elif change_relative < 0.15:
            return 'medium'
        else:
            return 'high'
    
    @property
    def window_size(self) -> int:
        """Get window size from config"""
        return self.analytics_config.get('window_size', 7)

