#!/usr/bin/env python3

import os
import yaml
from typing import Dict, Any, List, Optional
from pathlib import Path


class ConfigError(Exception):
    """Exception raised for configuration errors"""
    pass


class ConfigLoader:
    """Load and validate YAML configuration files for analytics jobs"""
    
    REQUIRED_SECTIONS = ['job', 'data_source', 'context_fields', 'metric_fields', 
                        'timestamp_field', 'analytics', 'events', 'thresholds', 'output']
    # Optional sections
    OPTIONAL_SECTIONS = ['context_tracking', 'metric_direction', 'runtime']
    
    REQUIRED_JOB_FIELDS = ['name']
    REQUIRED_DATA_SOURCE_FIELDS = ['ydb']
    REQUIRED_ANALYTICS_FIELDS = ['baseline_method', 'window_size', 'sensitivity']
    REQUIRED_EVENTS_FIELDS = ['detect']
    REQUIRED_OUTPUT_FIELDS = ['write_to_ydb', 'log_to_console', 'dry_run']
    
    VALID_BASELINE_METHODS = ['rolling_mean', 'zscore', 'median', 'prophet', 'adtk-levelshift']
    VALID_EVENT_TYPES = ['degradation_start', 'improvement_start', 'threshold_shift']
    
    def __init__(self, config_path: str):
        """
        Initialize config loader
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            raise ConfigError(f"Configuration file not found: {config_path}")
        
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            if config is None:
                raise ConfigError("Configuration file is empty")
            
            # Substitute environment variables
            config = self._substitute_env_vars(config)
            
            return config
        except yaml.YAMLError as e:
            raise ConfigError(f"Invalid YAML syntax: {e}")
        except Exception as e:
            raise ConfigError(f"Failed to load configuration: {e}")
    
    def _substitute_env_vars(self, obj: Any) -> Any:
        """Recursively substitute environment variables in config values"""
        if isinstance(obj, dict):
            return {k: self._substitute_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._substitute_env_vars(item) for item in obj]
        elif isinstance(obj, str):
            # Support ${VAR_NAME} or $VAR_NAME syntax
            if obj.startswith('${') and obj.endswith('}'):
                var_name = obj[2:-1]
                return os.environ.get(var_name, obj)
            elif obj.startswith('$') and len(obj) > 1:
                var_name = obj[1:]
                return os.environ.get(var_name, obj)
            return obj
        else:
            return obj
    
    def _validate_config(self):
        """Validate configuration structure and values"""
        # Check required sections
        for section in self.REQUIRED_SECTIONS:
            if section not in self.config:
                raise ConfigError(f"Missing required section: {section}")
        
        # Validate job section
        self._validate_job()
        
        # Validate data_source section
        self._validate_data_source()
        
        # Validate context_fields and metric_fields
        self._validate_fields()
        
        # Validate analytics section
        self._validate_analytics()
        
        # Validate events section
        self._validate_events()
        
        # Validate thresholds section
        self._validate_thresholds()
        
        # Validate output section
        self._validate_output()
        
        # Validate runtime section if present
        if 'runtime' in self.config:
            self._validate_runtime()
        
        # Validate context_tracking section if present
        if 'context_tracking' in self.config:
            self._validate_context_tracking()
        
        # Validate metric_direction section if present
        if 'metric_direction' in self.config:
            self._validate_metric_direction()
    
    def _validate_job(self):
        """Validate job section"""
        job = self.config['job']
        for field in self.REQUIRED_JOB_FIELDS:
            if field not in job:
                raise ConfigError(f"Missing required field in job section: {field}")
        
        if not isinstance(job['name'], str) or not job['name']:
            raise ConfigError("job.name must be a non-empty string")
    
    def _validate_data_source(self):
        """Validate data_source section"""
        data_source = self.config['data_source']
        if 'ydb' not in data_source:
            raise ConfigError("data_source.ydb is required")
        
        ydb_config = data_source['ydb']
        if 'query' not in ydb_config:
            raise ConfigError("data_source.ydb.query is required")
        
        if not isinstance(ydb_config['query'], str) or not ydb_config['query'].strip():
            raise ConfigError("data_source.ydb.query must be a non-empty SQL query string")
    
    def _validate_fields(self):
        """Validate context_fields, metric_fields, and timestamp_field"""
        # Validate context_fields
        if not isinstance(self.config['context_fields'], list):
            raise ConfigError("context_fields must be a list")
        if len(self.config['context_fields']) == 0:
            raise ConfigError("context_fields cannot be empty")
        for field in self.config['context_fields']:
            if not isinstance(field, str):
                raise ConfigError("All context_fields must be strings")
        
        # Validate metric_fields
        if not isinstance(self.config['metric_fields'], list):
            raise ConfigError("metric_fields must be a list")
        if len(self.config['metric_fields']) < 2:
            raise ConfigError("metric_fields must contain at least 2 fields (metric_name and metric_value)")
        for field in self.config['metric_fields']:
            if not isinstance(field, str):
                raise ConfigError("All metric_fields must be strings")
        
        # Validate timestamp_field
        if not isinstance(self.config['timestamp_field'], str):
            raise ConfigError("timestamp_field must be a string")
        if not self.config['timestamp_field']:
            raise ConfigError("timestamp_field cannot be empty")
    
    def _validate_analytics(self):
        """Validate analytics section"""
        analytics = self.config['analytics']
        
        # Check required fields
        for field in self.REQUIRED_ANALYTICS_FIELDS:
            if field not in analytics:
                raise ConfigError(f"Missing required field in analytics section: {field}")
        
        # Validate baseline_method
        baseline_method = analytics['baseline_method']
        if baseline_method not in self.VALID_BASELINE_METHODS:
            raise ConfigError(
                f"Invalid baseline_method: {baseline_method}. "
                f"Valid methods: {', '.join(self.VALID_BASELINE_METHODS)}"
            )
        
        # Validate window_size
        window_size = analytics['window_size']
        if not isinstance(window_size, int) or window_size < 1:
            raise ConfigError("analytics.window_size must be a positive integer")
        
        # Validate sensitivity
        sensitivity = analytics['sensitivity']
        if not isinstance(sensitivity, (int, float)) or sensitivity <= 0:
            raise ConfigError("analytics.sensitivity must be a positive number")
        
        # Validate optional fields
        if 'min_absolute_change' in analytics:
            if not isinstance(analytics['min_absolute_change'], (int, float)):
                raise ConfigError("analytics.min_absolute_change must be a number")
        
        if 'min_relative_change' in analytics:
            min_rel = analytics['min_relative_change']
            if not isinstance(min_rel, (int, float)) or min_rel < 0 or min_rel > 1:
                raise ConfigError("analytics.min_relative_change must be a number between 0 and 1")
        
        if 'hysteresis_points' in analytics:
            if not isinstance(analytics['hysteresis_points'], int) or analytics['hysteresis_points'] < 0:
                raise ConfigError("analytics.hysteresis_points must be a non-negative integer")
        
        if 'adaptive_threshold' in analytics:
            if not isinstance(analytics['adaptive_threshold'], bool):
                raise ConfigError("analytics.adaptive_threshold must be a boolean")
    
    def _validate_events(self):
        """Validate events section"""
        events = self.config['events']
        
        if 'detect' not in events:
            raise ConfigError("events.detect is required")
        
        detect = events['detect']
        if not isinstance(detect, list):
            raise ConfigError("events.detect must be a list")
        
        if len(detect) == 0:
            raise ConfigError("events.detect cannot be empty")
        
        for event_type in detect:
            if event_type not in self.VALID_EVENT_TYPES:
                raise ConfigError(
                    f"Invalid event type: {event_type}. "
                    f"Valid types: {', '.join(self.VALID_EVENT_TYPES)}"
                )
        
        if 'min_event_duration_minutes' in events:
            duration = events['min_event_duration_minutes']
            if not isinstance(duration, (int, float)) or duration < 0:
                raise ConfigError("events.min_event_duration_minutes must be a non-negative number")
    
    def _validate_thresholds(self):
        """Validate thresholds section"""
        thresholds = self.config['thresholds']
        
        if 'keep_history' in thresholds:
            if not isinstance(thresholds['keep_history'], bool):
                raise ConfigError("thresholds.keep_history must be a boolean")
    
    def _validate_output(self):
        """Validate output section"""
        output = self.config['output']
        
        for field in self.REQUIRED_OUTPUT_FIELDS:
            if field not in output:
                raise ConfigError(f"Missing required field in output section: {field}")
        
        for field in ['write_to_ydb', 'log_to_console', 'dry_run']:
            if not isinstance(output[field], bool):
                raise ConfigError(f"output.{field} must be a boolean")
    
    def _validate_runtime(self):
        """Validate runtime section"""
        runtime = self.config['runtime']
        
        if 'timezone' in runtime:
            if not isinstance(runtime['timezone'], str):
                raise ConfigError("runtime.timezone must be a string")
        
        if 'max_runtime_minutes' in runtime:
            max_runtime = runtime['max_runtime_minutes']
            if not isinstance(max_runtime, (int, float)) or max_runtime <= 0:
                raise ConfigError("runtime.max_runtime_minutes must be a positive number")
    
    def _validate_context_tracking(self):
        """Validate context_tracking section"""
        context_tracking = self.config['context_tracking']
        
        if 'track_new_contexts' in context_tracking:
            if not isinstance(context_tracking['track_new_contexts'], bool):
                raise ConfigError("context_tracking.track_new_contexts must be a boolean")
        
        if 'track_disappeared_contexts' in context_tracking:
            if not isinstance(context_tracking['track_disappeared_contexts'], bool):
                raise ConfigError("context_tracking.track_disappeared_contexts must be a boolean")
        
        if 'context_change_rules' in context_tracking:
            rules = context_tracking['context_change_rules']
            
            # Validate new_context_metrics
            if 'new_context_metrics' in rules:
                for metric_name, rule_config in rules['new_context_metrics'].items():
                    if not isinstance(rule_config, dict):
                        raise ConfigError(f"context_tracking.context_change_rules.new_context_metrics.{metric_name} must be a dictionary")
                    
                    if 'event_type' in rule_config:
                        if rule_config['event_type'] not in self.VALID_EVENT_TYPES:
                            raise ConfigError(f"Invalid event_type in new_context_metrics.{metric_name}: {rule_config['event_type']}")
            
            # Validate disappeared_context_metrics
            if 'disappeared_context_metrics' in rules:
                for metric_name, rule_config in rules['disappeared_context_metrics'].items():
                    if not isinstance(rule_config, dict):
                        raise ConfigError(f"context_tracking.context_change_rules.disappeared_context_metrics.{metric_name} must be a dictionary")
                    
                    if 'event_type' in rule_config:
                        if rule_config['event_type'] not in self.VALID_EVENT_TYPES:
                            raise ConfigError(f"Invalid event_type in disappeared_context_metrics.{metric_name}: {rule_config['event_type']}")
                    
                    # Validate absence rules
                    if 'min_absence_points' in rule_config:
                        if not isinstance(rule_config['min_absence_points'], int) or rule_config['min_absence_points'] < 1:
                            raise ConfigError(f"context_tracking.context_change_rules.disappeared_context_metrics.{metric_name}.min_absence_points must be a positive integer")
                    
                    if 'min_absence_duration_minutes' in rule_config:
                        if not isinstance(rule_config['min_absence_duration_minutes'], (int, float)) or rule_config['min_absence_duration_minutes'] < 0:
                            raise ConfigError(f"context_tracking.context_change_rules.disappeared_context_metrics.{metric_name}.min_absence_duration_minutes must be a non-negative number")
                    
                    if 'absence_type' in rule_config:
                        if rule_config['absence_type'] not in ['consecutive', 'total']:
                            raise ConfigError(f"context_tracking.context_change_rules.disappeared_context_metrics.{metric_name}.absence_type must be 'consecutive' or 'total'")
                    
                    if 'min_historical_points' in rule_config:
                        if not isinstance(rule_config['min_historical_points'], int) or rule_config['min_historical_points'] < 1:
                            raise ConfigError(f"context_tracking.context_change_rules.disappeared_context_metrics.{metric_name}.min_historical_points must be a positive integer")
    
    def _validate_metric_direction(self):
        """Validate metric_direction section"""
        metric_direction = self.config['metric_direction']
        
        if 'default' in metric_direction:
            if metric_direction['default'] not in ['negative', 'positive']:
                raise ConfigError("metric_direction.default must be 'negative' or 'positive'")
        
        for metric_name, direction in metric_direction.items():
            if metric_name != 'default' and direction not in ['negative', 'positive']:
                raise ConfigError(f"metric_direction.{metric_name} must be 'negative' or 'positive'")
    
    def get_config(self) -> Dict[str, Any]:
        """Get validated configuration"""
        return self.config
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key (supports dot notation)"""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Convenience function to load and validate configuration
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Validated configuration dictionary
        
    Raises:
        ConfigError: If configuration is invalid
    """
    loader = ConfigLoader(config_path)
    return loader.get_config()

