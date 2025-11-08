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
        self.min_absolute_change = self.analytics_config.get('min_absolute_change', 0)
        self.min_relative_change = self.analytics_config.get('min_relative_change', 0.0)
        self.hysteresis_points = self.analytics_config.get('hysteresis_points', 3)
        
        # Metric direction: how to interpret metric changes
        # "negative" - больше = хуже (duration_ms, error_rate) - рост = degradation, падение = improvement
        # "positive" - больше = лучше (throughput, success_rate) - рост = improvement, падение = degradation
        metric_direction_config = config.get('metric_direction', {})
        self.metric_direction_default = metric_direction_config.get('default', 'negative')
        self.metric_direction_map = {k: v for k, v in metric_direction_config.items() if k != 'default'}
    
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
        
        # Get metric direction
        metric_direction = self._get_metric_direction(metric_name) if metric_name else self.metric_direction_default
        
        events = []
        
        # Detect different event types
        # For negative metrics: above upper = degradation, below lower = improvement
        # For positive metrics: above upper = improvement, below lower = degradation
        if 'degradation_start' in self.detect_types:
            if metric_direction == 'negative':
                # Negative metric: above upper threshold = degradation
                degradation_events = self._detect_degradation_above_threshold(series, baseline_result)
            else:
                # Positive metric: below lower threshold = degradation
                degradation_events = self._detect_degradation_below_threshold(series, baseline_result)
            events.extend(degradation_events)
        
        if 'improvement_start' in self.detect_types:
            if metric_direction == 'negative':
                # Negative metric: below lower threshold = improvement
                improvement_events = self._detect_improvement_below_threshold(series, baseline_result)
            else:
                # Positive metric: above upper threshold = improvement
                improvement_events = self._detect_improvement_above_threshold(series, baseline_result)
            events.extend(improvement_events)
        
        if 'threshold_shift' in self.detect_types:
            shift_events = self._detect_threshold_shift(series, baseline_result)
            events.extend(shift_events)
        
        # Filter events by duration and significance
        filtered_events = self._filter_events(events, series)
        
        return filtered_events
    
    def _detect_degradation_below_threshold(self, series: pd.Series, baseline_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect degradation events (values below lower threshold) - for positive metrics"""
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
            if (abs(change_absolute) >= self.min_absolute_change and 
                change_relative >= self.min_relative_change):
                
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
    
    def _detect_degradation_above_threshold(self, series: pd.Series, baseline_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect degradation events (values above upper threshold) - for negative metrics"""
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
            if (abs(change_absolute) >= self.min_absolute_change and 
                change_relative >= self.min_relative_change):
                
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
    
    def _detect_improvement_below_threshold(self, series: pd.Series, baseline_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect improvement events (values below lower threshold) - for negative metrics"""
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
            if (abs(change_absolute) >= self.min_absolute_change and 
                change_relative >= self.min_relative_change):
                
                event = {
                    'event_type': 'improvement_start',
                    'event_start_time': series.index[segment_start],
                    'event_end_time': series.index[segment_end],
                    'severity': self._calculate_severity(change_absolute, change_relative, 'improvement'),
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
    
    def _detect_improvement_above_threshold(self, series: pd.Series, baseline_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect improvement events (values above upper threshold) - for positive metrics"""
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
            if (abs(change_absolute) >= self.min_absolute_change and 
                change_relative >= self.min_relative_change):
                
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
                    
                    if (abs(change_absolute) >= self.min_absolute_change and 
                        change_relative >= self.min_relative_change):
                        
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
            min_abs_for_shift = self.min_absolute_change * 1.5  # 50% more strict
            min_rel_for_shift = self.min_relative_change * 1.5  # 50% more strict
            
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
        """
        segments = []
        in_segment = False
        segment_start = None
        
        # Maximum gap between points to consider them part of the same segment
        # For irregular data, we allow gaps up to 2x the median interval
        if len(condition) > 1 and isinstance(condition.index, pd.DatetimeIndex):
            time_diffs = condition.index.to_series().diff().dropna()
            if len(time_diffs) > 0:
                median_interval = time_diffs.median()
                max_gap = median_interval * 3  # Allow 3x median interval as max gap
            else:
                max_gap = timedelta(hours=1)  # Default: 1 hour
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
    
    def _filter_events(self, events: List[Dict[str, Any]], series: pd.Series) -> List[Dict[str, Any]]:
        """
        Filter events by duration and apply hysteresis.
        For sparse/irregular data, we consider both time duration and number of points.
        """
        if not events:
            return []
        
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
            
            # For sparse/irregular data: if we have many points, relax duration requirement
            # If we have enough points (>= hysteresis_points * 2), we accept shorter durations
            min_duration = self.min_event_duration
            if num_points >= self.hysteresis_points * 2:
                # For events with many points, reduce duration requirement by 50%
                min_duration = self.min_event_duration * 0.5
            
            if duration < min_duration:
                # Still check if we have enough points to compensate
                if num_points < self.hysteresis_points * 3:
                    continue
            
            # Apply hysteresis: require multiple consecutive points
            if self.hysteresis_points > 1:
                if num_points < self.hysteresis_points:
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

