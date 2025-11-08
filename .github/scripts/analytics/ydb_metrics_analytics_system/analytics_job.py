#!/usr/bin/env python3

import sys
import os
import argparse
import time
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from ydb_wrapper import YDBWrapper

# Import analytics modules
from config_loader import ConfigLoader, ConfigError
from data_access import DataAccess
from preprocessing import Preprocessing
from baseline_calculator import BaselineCalculator
from event_detector import EventDetector
from persistence import Persistence
from visualization import EventVisualizer
from summary_report import SummaryReportGenerator


class AnalyticsJob:
    """Main orchestrator for analytics pipeline"""
    
    def __init__(self, config_path: str, dry_run: bool = False, event_deepness: Optional[str] = None):
        """
        Initialize analytics job
        
        Args:
            config_path: Path to YAML configuration file
            dry_run: If True, don't write to YDB
            event_deepness: Optional time window for event analysis (e.g., "7d", "30d", "1h", "2w")
                          Only events within this window will be analyzed
        """
        self.config_path = config_path
        self.dry_run = dry_run
        self.event_deepness = event_deepness
        
        # Load configuration
        try:
            self.config_loader = ConfigLoader(config_path)
            self.config = self.config_loader.get_config()
        except ConfigError as e:
            print(f"Configuration error: {e}", file=sys.stderr)
            sys.exit(1)
        
        # Override dry_run from config if specified
        if 'output' in self.config and 'dry_run' in self.config['output']:
            self.dry_run = self.config['output']['dry_run'] or self.dry_run
        
        # Initialize YDB wrapper
        self.ydb_wrapper = YDBWrapper(silent=False, use_local_config=True)
        
        # Initialize components
        self.data_access = DataAccess(self.ydb_wrapper, self.config)
        self.preprocessing = Preprocessing(self.config)
        self.baseline_calculator = BaselineCalculator(self.config)
        self.event_detector = EventDetector(self.config)
        
        # Initialize persistence only if not dry_run
        if not self.dry_run:
            self.persistence = Persistence(self.ydb_wrapper, self.config)
        else:
            self.persistence = None
        
        # Runtime tracking
        self.start_time = time.time()
        self.max_runtime_minutes = None
        if 'runtime' in self.config and 'max_runtime_minutes' in self.config['runtime']:
            self.max_runtime_minutes = self.config['runtime']['max_runtime_minutes']
    
    def run(self):
        """Run the analytics pipeline"""
        job_name = self.config.get('job', {}).get('name', 'analytics_job')
        print(f"Starting analytics job: {job_name}")
        
        try:
            # Step 1: Load data
            print("Step 1: Loading measurements from YDB...")
            
            # Always load ALL data for baseline calculation (stable baseline on historical data)
            df_all = self.data_access.load_measurements(start_ts=None, end_ts=None)
            
            # Calculate time window for event analysis if event_deepness is specified
            event_start_ts = None
            event_end_ts = None
            if self.event_deepness:
                event_end_ts = datetime.now()
                event_start_ts = self._parse_event_deepness(self.event_deepness, event_end_ts)
                print(f"  Analyzing events in window: {event_start_ts.strftime('%Y-%m-%d %H:%M:%S')} to {event_end_ts.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"  Baseline computed on all available data (for stability)")
            
            if df_all.empty:
                print("Warning: No data loaded from YDB")
                return
            
            summary = self.data_access.get_data_summary(df_all)
            print(f"Loaded {summary['total_rows']} rows for baseline calculation")
            print(f"Metrics: {', '.join(summary['metrics'])}")
            print(f"Context combinations: {summary['context_combinations']}")
            
            # Check runtime
            self._check_runtime()
            
            # Step 2: Preprocess and group data (on ALL data for baseline)
            print("\nStep 2: Preprocessing and grouping data...")
            df_cleaned = self.preprocessing.clean_data(df_all, remove_outliers=True)
            grouped_data = self.preprocessing.group_by_context(df_cleaned)
            
            print(f"Grouped into {len(grouped_data)} metric×context combinations")
            
            # For error detection: detect appearance/disappearance of error types
            # Compare contexts before and after event_deepness window
            new_error_contexts = set()
            disappeared_error_contexts = set()
            if self.event_deepness and event_start_ts and event_end_ts:
                timestamp_field = self.config.get('timestamp_field')
                if timestamp_field:
                    # Get contexts before event window (historical)
                    df_before = df_cleaned[df_cleaned[timestamp_field] < event_start_ts]
                    contexts_before = set()
                    if not df_before.empty:
                        grouped_before = self.preprocessing.group_by_context(df_before)
                        contexts_before = set(grouped_before.keys())
                    
                    # Get contexts in event window
                    df_in_window = df_cleaned[
                        (df_cleaned[timestamp_field] >= event_start_ts) & 
                        (df_cleaned[timestamp_field] <= event_end_ts)
                    ]
                    contexts_in_window = set()
                    if not df_in_window.empty:
                        grouped_in_window = self.preprocessing.group_by_context(df_in_window)
                        contexts_in_window = set(grouped_in_window.keys())
                    
                    # Detect new error types (appeared in window but not before)
                    new_error_contexts = contexts_in_window - contexts_before
                    # Detect disappeared error types (were before but not in window)
                    disappeared_error_contexts = contexts_before - contexts_in_window
                    
                    if new_error_contexts:
                        print(f"  ⚠ Detected {len(new_error_contexts)} new error types (appeared in event window)")
                    if disappeared_error_contexts:
                        print(f"  ✓ Detected {len(disappeared_error_contexts)} disappeared error types (gone in event window)")
            
            # Step 3: Process each group
            print("\nStep 3: Computing baselines and detecting events...")
            all_thresholds = []
            all_events = []
            # Store visualization data for groups with events
            visualization_data = []
            
            processed = 0
            for group_key, group_df in grouped_data.items():
                # Check runtime
                self._check_runtime()
                
                # Validate group has enough data
                min_points = self.config.get('analytics', {}).get('min_data_points', 3)
                has_enough_data = self.preprocessing.validate_group_data(group_df, min_points=min_points)
                
                # Extract metric name and context
                metric_name = group_key[0]
                context_values = self.preprocessing.extract_context_from_group_key(group_key)
                context_hash = self.preprocessing.compute_context_hash(context_values)
                context_json = self._context_to_json(context_values)
                
                # Check if this is a new error type (appeared in event window)
                is_new_error = group_key in new_error_contexts
                is_disappeared_error = group_key in disappeared_error_contexts
                
                if not has_enough_data:
                    # For error detection: if it's a new error type, create event even without baseline
                    if is_new_error and metric_name == 'error_count':
                        # Create "new error appeared" event
                        timestamp_field = self.config.get('timestamp_field')
                        if timestamp_field in group_df.columns:
                            first_error_time = group_df[timestamp_field].min()
                            error_count = len(group_df)
                            
                            event_data = {
                                'timestamp': first_error_time,
                                'metric_name': metric_name,
                                'context_hash': context_hash,
                                'context_json': context_json,
                                'event_type': 'degradation_start',  # New error = degradation
                                'event_start_time': first_error_time,
                                'event_end_time': first_error_time,
                                'severity': 'high',
                                'baseline_before': 0.0,  # No baseline (new error)
                                'baseline_after': 0.0,
                                'threshold_before': None,
                                'threshold_after': None,
                                'change_absolute': float(error_count),
                                'change_relative': float('inf') if error_count > 0 else 0.0,  # Infinite relative change (0 -> N)
                                'current_value': float(error_count),
                            }
                            all_events.append(event_data)
                            print(f"  ⚠ New error type detected: {context_json[:80]}... ({error_count} errors)")
                        continue
                    else:
                        print(f"Skipping group {group_key}: insufficient data (need at least {min_points} points, got {len(group_df)})")
                        continue
                
                # Prepare time series from ALL data (for stable baseline)
                series_all = self.preprocessing.prepare_time_series(group_df)
                
                if series_all.empty:
                    continue
                
                # Compute baseline and thresholds on ALL data
                baseline_result = self.baseline_calculator.compute_baseline_and_thresholds(series_all)
                
                # Filter series for event detection if event_deepness is specified
                series_for_events = series_all
                if self.event_deepness and event_start_ts and event_end_ts:
                    timestamp_field = self.config.get('timestamp_field')
                    if timestamp_field:
                        # Filter series to event window
                        mask = (series_all.index >= event_start_ts) & (series_all.index <= event_end_ts)
                        series_for_events = series_all[mask]
                        if len(series_for_events) == 0:
                            # No data in event window - check if error disappeared
                            if is_disappeared_error and metric_name == 'error_count':
                                # Get last error time before window
                                df_before_window = group_df[group_df[timestamp_field] < event_start_ts]
                                if not df_before_window.empty:
                                    last_error_time = df_before_window[timestamp_field].max()
                                    
                                    event_data = {
                                        'timestamp': event_start_ts,
                                        'metric_name': metric_name,
                                        'context_hash': context_hash,
                                        'context_json': context_json,
                                        'event_type': 'improvement_start',  # Error disappeared = improvement
                                        'event_start_time': event_start_ts,
                                        'event_end_time': event_end_ts,
                                        'severity': 'medium',
                                        'baseline_before': float(series_all.iloc[-1]) if not series_all.empty else 0.0,
                                        'baseline_after': 0.0,
                                        'threshold_before': baseline_result.get('lower_threshold'),
                                        'threshold_after': None,
                                        'change_absolute': -float(series_all.iloc[-1]) if not series_all.empty else 0.0,
                                        'change_relative': 1.0 if not series_all.empty and series_all.iloc[-1] > 0 else 0.0,  # 100% decrease
                                        'current_value': 0.0,
                                    }
                                    all_events.append(event_data)
                                    print(f"  ✓ Error type disappeared: {context_json[:80]}... (last seen: {last_error_time})")
                            continue
                
                # Prepare threshold data for saving
                threshold_data = {
                    'timestamp': baseline_result['timestamp'],
                    'metric_name': metric_name,
                    'context_hash': context_hash,
                    'context_json': context_json,
                    'baseline_value': baseline_result['baseline_value'],
                    'upper_threshold': baseline_result['upper_threshold'],
                    'lower_threshold': baseline_result['lower_threshold'],
                    'baseline_method': baseline_result['baseline_method'],
                    'window_size': baseline_result['window_size'],
                    'sensitivity': baseline_result['sensitivity'],
                    'adaptive_threshold': baseline_result['adaptive_threshold'],
                }
                all_thresholds.append(threshold_data)
                
                # Detect events only in the filtered window (pass metric_name for direction detection)
                events = self.event_detector.detect_events(series_for_events, baseline_result, metric_name=metric_name)
                
                # For new error types: if no events detected but it's a new error, create event anyway
                # This ensures we always detect appearance of new error types
                if is_new_error and metric_name == 'error_count' and len(events) == 0:
                    # New error type appeared but no event detected by normal logic
                    # Create event to mark appearance of new error type
                    if not series_for_events.empty:
                        first_error_time = series_for_events.index[0]
                        # For aggregated data, sum all values; for raw data, use first value
                        error_count = float(series_for_events.sum())
                        
                        event_data = {
                            'timestamp': first_error_time,
                            'metric_name': metric_name,
                            'context_hash': context_hash,
                            'context_json': context_json,
                            'event_type': 'degradation_start',  # New error = degradation
                            'event_start_time': first_error_time,
                            'event_end_time': series_for_events.index[-1] if len(series_for_events) > 1 else first_error_time,
                            'severity': 'high',
                            'baseline_before': 0.0,  # No baseline before (new error)
                            'baseline_after': float(baseline_result.get('baseline_value', 0.0)),
                            'threshold_before': None,
                            'threshold_after': baseline_result.get('upper_threshold'),
                            'change_absolute': error_count,
                            'change_relative': float('inf') if error_count > 0 else 0.0,  # Infinite relative change (0 -> N)
                            'current_value': error_count,
                        }
                        events.append(event_data)
                        print(f"  ⚠ New error type detected (with baseline): {context_json[:80]}... ({error_count} errors)")
                
                # Debug logging for event detection
                if self.config.get('output', {}).get('log_to_console', False):
                    baseline_val = baseline_result.get('baseline_value')
                    upper = baseline_result.get('upper_threshold')
                    lower = baseline_result.get('lower_threshold')
                    latest_value = float(series_for_events.iloc[-1]) if not series_for_events.empty else None
                    
                    # Count points outside thresholds (in event window)
                    above_upper = (series_for_events > upper).sum() if upper is not None else 0
                    below_lower = (series_for_events < lower).sum() if lower is not None else 0
                    
                    # Check why events might not be detected
                    if len(events) == 0 and (above_upper > 0 or below_lower > 0):
                        # There are points outside thresholds but no events detected
                        min_duration = self.config.get('events', {}).get('min_event_duration_minutes', 30)
                        # Get metric-specific parameters
                        metric_specific_params = self.config.get('analytics', {}).get('metric_specific_params', {})
                        if metric_name in metric_specific_params:
                            min_abs = metric_specific_params[metric_name].get('min_absolute_change', 
                                                                              self.config.get('analytics', {}).get('min_absolute_change', 0))
                            min_rel = metric_specific_params[metric_name].get('min_relative_change',
                                                                              self.config.get('analytics', {}).get('min_relative_change', 0.0))
                        else:
                            min_abs = self.config.get('analytics', {}).get('min_absolute_change', 0)
                            min_rel = self.config.get('analytics', {}).get('min_relative_change', 0.0)
                        
                        if above_upper > 0:
                            max_value = float(series_for_events.max())
                            change_abs = max_value - baseline_val if baseline_val else 0
                            change_rel = abs(change_abs / baseline_val) if baseline_val != 0 else 0
                            print(f"  ⚠ Group {metric_name}/{context_json[:50]}...: "
                                  f"{above_upper} points above upper ({max_value:.1f} > {upper:.1f}), "
                                  f"but no events (change: {change_abs:.1f} abs, {change_rel*100:.1f}% rel, "
                                  f"min required: {min_abs} abs, {min_rel*100:.1f}% rel, "
                                  f"duration filter: {min_duration}min)")
                        
                        if below_lower > 0:
                            min_value = float(series_for_events.min())
                            change_abs = min_value - baseline_val if baseline_val else 0
                            change_rel = abs(change_abs / baseline_val) if baseline_val != 0 else 0
                            print(f"  ⚠ Group {metric_name}/{context_json[:50]}...: "
                                  f"{below_lower} points below lower ({min_value:.1f} < {lower:.1f}), "
                                  f"but no events (change: {change_abs:.1f} abs, {change_rel*100:.1f}% rel, "
                                  f"min required: {min_abs} abs, {min_rel*100:.1f}% rel, "
                                  f"duration filter: {min_duration}min)")
                
                # Prepare event data for saving
                for event in events:
                    event_data = {
                        'timestamp': event.get('event_start_time', baseline_result['timestamp']),
                        'metric_name': metric_name,
                        'context_hash': context_hash,
                        'context_json': context_json,
                        'event_type': event['event_type'],
                        'event_start_time': event['event_start_time'],
                        'event_end_time': event.get('event_end_time'),
                        'severity': event.get('severity'),
                        'baseline_before': event.get('baseline_before'),
                        'baseline_after': event.get('baseline_after'),
                        'threshold_before': event.get('threshold_before'),
                        'threshold_after': event.get('threshold_after'),
                        'change_absolute': event.get('change_absolute'),
                        'change_relative': event.get('change_relative'),
                        'current_value': event.get('current_value'),
                    }
                    all_events.append(event_data)
                
                # Store visualization data if there are events
                # Use series_all for visualization to show full context, but events are only in the window
                if events:
                    visualization_data.append({
                        'metric_name': metric_name,
                        'context_hash': context_hash,
                        'context_json': context_json,
                        'series': series_all,  # Show full series for context
                        'baseline_result': baseline_result,
                        'events': events,  # Events are only in the filtered window
                    })
                
                processed += 1
                if processed % 10 == 0:
                    print(f"Processed {processed}/{len(grouped_data)} groups...")
            
            print(f"\nComputed baselines for {len(all_thresholds)} groups")
            print(f"Detected {len(all_events)} events")
            
            # Step 4: Save results
            if not self.dry_run:
                print("\nStep 4: Saving results to YDB...")
                
                # Ensure tables exist
                self.persistence.ensure_tables_exist()
                
                # Save thresholds
                if all_thresholds:
                    self.persistence.save_thresholds(all_thresholds)
                    print(f"Saved {len(all_thresholds)} threshold records")
                
                # Save events
                if all_events:
                    self.persistence.save_events(all_events)
                    print(f"Saved {len(all_events)} event records")
            else:
                print("\nStep 4: Dry-run mode - saving to local files...")
                print(f"Would save {len(all_thresholds)} threshold records")
                print(f"Would save {len(all_events)} event records")
                
                # Save to local files
                self._save_dry_run_results(all_events, all_thresholds, visualization_data)
            
            # Summary
            elapsed = time.time() - self.start_time
            print(f"\nAnalytics job completed successfully in {elapsed:.2f} seconds")
            
        except KeyboardInterrupt:
            print("\nJob interrupted by user")
            sys.exit(1)
        except Exception as e:
            print(f"\nError during analytics job: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def _check_runtime(self):
        """Check if job has exceeded maximum runtime"""
        if self.max_runtime_minutes is None:
            return
        
        elapsed_minutes = (time.time() - self.start_time) / 60
        if elapsed_minutes >= self.max_runtime_minutes:
            raise RuntimeError(
                f"Job exceeded maximum runtime of {self.max_runtime_minutes} minutes"
            )
    
    def _parse_event_deepness(self, deepness_str: str, reference_time: datetime) -> datetime:
        """
        Parse event deepness string (e.g., "7d", "30d", "1h", "2w") and calculate start timestamp
        
        Args:
            deepness_str: String like "7d", "30d", "1h", "2w", "1m"
            reference_time: Reference time (usually now) to subtract from
            
        Returns:
            Start timestamp (reference_time - deepness)
        """
        import re
        
        # Parse pattern: number + unit (d=days, h=hours, w=weeks, m=minutes)
        pattern = r'^(\d+)([dhwms])$'
        match = re.match(pattern, deepness_str.lower())
        
        if not match:
            raise ValueError(
                f"Invalid event_deepness format: {deepness_str}. "
                f"Expected format: number + unit (e.g., '7d', '30d', '1h', '2w', '30m'). "
                f"Units: d=days, h=hours, w=weeks, m=minutes, s=seconds"
            )
        
        value = int(match.group(1))
        unit = match.group(2)
        
        if unit == 'd':
            delta = timedelta(days=value)
        elif unit == 'h':
            delta = timedelta(hours=value)
        elif unit == 'w':
            delta = timedelta(weeks=value)
        elif unit == 'm':
            delta = timedelta(minutes=value)
        elif unit == 's':
            delta = timedelta(seconds=value)
        else:
            raise ValueError(f"Unknown unit: {unit}. Use d, h, w, m, or s")
        
        return reference_time - delta
    
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
    
    def _context_to_json(self, context_values: Dict[str, Any]) -> str:
        """Convert context dictionary to JSON string"""
        try:
            # Convert numpy/pandas types to native Python types, then handle other non-serializable values
            serializable = {}
            for k, v in context_values.items():
                # First convert numpy/pandas types
                v = self._convert_to_native_type(v)
                # Then check if it's a basic serializable type
                if isinstance(v, (str, int, float, bool, type(None))):
                    serializable[k] = v
                else:
                    # Fallback to string for anything else
                    serializable[k] = str(v)
            return json.dumps(serializable, sort_keys=True)
        except Exception:
            return "{}"
    
    def _save_dry_run_results(self, events: List[Dict[str, Any]], thresholds: List[Dict[str, Any]], 
                              visualization_data: List[Dict[str, Any]] = None):
        """
        Save events and thresholds to local JSON files in dry-run mode
        
        Args:
            events: List of event dictionaries
            thresholds: List of threshold dictionaries
            visualization_data: List of dicts with series, baseline_result, events for visualization
        """
        # Create output directory if it doesn't exist
        output_dir = os.path.join(os.path.dirname(__file__), 'dry_run_output')
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate timestamp for filenames
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        job_name = self.config.get('job', {}).get('name', 'analytics')
        
        # Convert data to JSON-serializable format
        def make_serializable(obj):
            """Recursively convert object to JSON-serializable format"""
            if isinstance(obj, dict):
                return {k: make_serializable(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [make_serializable(item) for item in obj]
            elif isinstance(obj, (pd.Timestamp, datetime)):
                return obj.isoformat()
            else:
                return self._convert_to_native_type(obj)
        
        # Save events
        if events:
            events_file = os.path.join(output_dir, f'{job_name}_events_{timestamp}.json')
            events_serializable = make_serializable(events)
            with open(events_file, 'w', encoding='utf-8') as f:
                json.dump(events_serializable, f, indent=2, ensure_ascii=False)
            print(f"  ✓ Saved {len(events)} events to {events_file}")
        else:
            print(f"  ℹ No events to save")
        
        # Save thresholds
        if thresholds:
            thresholds_file = os.path.join(output_dir, f'{job_name}_thresholds_{timestamp}.json')
            thresholds_serializable = make_serializable(thresholds)
            with open(thresholds_file, 'w', encoding='utf-8') as f:
                json.dump(thresholds_serializable, f, indent=2, ensure_ascii=False)
            print(f"  ✓ Saved {len(thresholds)} thresholds to {thresholds_file}")
        else:
            print(f"  ℹ No thresholds to save")
        
        # Also save a summary CSV for easier viewing
        if events:
            events_csv = os.path.join(output_dir, f'{job_name}_events_{timestamp}.csv')
            try:
                events_df = pd.DataFrame(events_serializable)
                # Convert timestamp columns
                for col in ['timestamp', 'event_start_time', 'event_end_time']:
                    if col in events_df.columns:
                        events_df[col] = pd.to_datetime(events_df[col], errors='coerce')
                events_df.to_csv(events_csv, index=False)
                print(f"  ✓ Saved events summary to {events_csv}")
            except Exception as e:
                print(f"  ⚠ Could not save CSV: {e}")
        
        if thresholds:
            thresholds_csv = os.path.join(output_dir, f'{job_name}_thresholds_{timestamp}.csv')
            try:
                thresholds_df = pd.DataFrame(thresholds_serializable)
                # Convert timestamp column
                if 'timestamp' in thresholds_df.columns:
                    thresholds_df['timestamp'] = pd.to_datetime(thresholds_df['timestamp'], errors='coerce')
                thresholds_df.to_csv(thresholds_csv, index=False)
                print(f"  ✓ Saved thresholds summary to {thresholds_csv}")
            except Exception as e:
                print(f"  ⚠ Could not save CSV: {e}")
        
        # Generate visualizations for groups with events
        if visualization_data:
            try:
                EventVisualizer.generate_visualizations(visualization_data, output_dir, timestamp, job_name)
            except Exception as e:
                print(f"  ⚠ Could not generate visualizations: {e}")
                import traceback
                traceback.print_exc()
        
        # Generate summary HTML report
        if events:
            try:
                SummaryReportGenerator.generate_summary_html(events, visualization_data, output_dir, timestamp, job_name)
            except Exception as e:
                print(f"  ⚠ Could not generate summary HTML: {e}")
                import traceback
                traceback.print_exc()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Run YDB metrics analytics job")
    parser.add_argument(
        '--config',
        required=True,
        help='Path to YAML configuration file'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without writing to YDB'
    )
    parser.add_argument(
        '--event-deepness',
        type=str,
        default=None,
        help='Time window for event analysis (e.g., "7d", "30d", "1h", "2w"). '
             'Only events within this window will be analyzed. '
             'Units: d=days, h=hours, w=weeks, m=minutes, s=seconds. '
             'If not specified, analyzes all available data.'
    )
    
    args = parser.parse_args()
    
    job = AnalyticsJob(args.config, dry_run=args.dry_run, event_deepness=args.event_deepness)
    job.run()


if __name__ == "__main__":
    main()

