#!/usr/bin/env python3

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple
from scipy import stats
import warnings

# Optional imports for advanced methods
try:
    from adtk.detector import LevelShiftAD
    ADTK_AVAILABLE = True
except ImportError:
    ADTK_AVAILABLE = False

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False


class BaselineCalculator:
    """Calculate baseline and thresholds for time series data"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize baseline calculator
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.analytics_config = config['analytics']
        self.baseline_method = self.analytics_config['baseline_method']
        self.window_size = self.analytics_config['window_size']
        self.sensitivity = self.analytics_config['sensitivity']
        self.adaptive_threshold = self.analytics_config.get('adaptive_threshold', False)
    
    def compute_baseline_and_thresholds(self, series: pd.Series) -> Dict[str, Any]:
        """
        Compute baseline and thresholds for time series
        
        Args:
            series: Time series with timestamp index and metric values
            
        Returns:
            Dictionary with baseline values and thresholds
        """
        if series.empty or len(series) < 2:
            return self._empty_result()
        
        # Compute baseline based on method
        baseline_series = self._compute_baseline(series)
        
        # Compute thresholds
        thresholds = self._compute_thresholds(series, baseline_series)
        
        # Get latest values
        latest_timestamp = series.index[-1]
        latest_baseline = baseline_series.iloc[-1] if not baseline_series.empty else None
        
        result = {
            'baseline_method': self.baseline_method,
            'window_size': self.window_size,
            'sensitivity': self.sensitivity,
            'adaptive_threshold': self.adaptive_threshold,
            'baseline_series': baseline_series,
            'baseline_value': latest_baseline,
            'upper_threshold': thresholds['upper'],
            'lower_threshold': thresholds['lower'],
            'timestamp': latest_timestamp,
            'statistics': {
                'mean': float(series.mean()) if not series.empty else None,
                'std': float(series.std()) if not series.empty else None,
                'median': float(series.median()) if not series.empty else None,
                'min': float(series.min()) if not series.empty else None,
                'max': float(series.max()) if not series.empty else None,
            }
        }
        
        return result
    
    def _compute_baseline(self, series: pd.Series) -> pd.Series:
        """
        Compute baseline using configured method
        
        Args:
            series: Time series data
            
        Returns:
            Series with baseline values
        """
        method = self.baseline_method
        
        if method == 'rolling_mean':
            return self._rolling_mean(series)
        elif method == 'median':
            return self._rolling_median(series)
        elif method == 'zscore':
            return self._zscore_baseline(series)
        elif method == 'prophet':
            return self._prophet_baseline(series)
        elif method == 'adtk-levelshift':
            return self._adtk_levelshift_baseline(series)
        else:
            raise ValueError(f"Unknown baseline method: {method}")
    
    def _rolling_mean(self, series: pd.Series) -> pd.Series:
        """
        Compute rolling mean baseline using lookback window.
        Baseline is computed only from historical data (before current point).
        This prevents baseline from "following" anomalies.
        """
        baseline = pd.Series(index=series.index, dtype=float)
        window = min(self.window_size, len(series))
        
        for i in range(len(series)):
            if i == 0:
                # For first point, use the point itself
                baseline.iloc[i] = series.iloc[i]
            elif i < window:
                # For early points, use all previous points
                baseline.iloc[i] = series.iloc[:i].mean()
            else:
                # For later points, use only historical data (lookback window)
                baseline.iloc[i] = series.iloc[i-window:i].mean()
        
        return baseline
    
    def _rolling_median(self, series: pd.Series) -> pd.Series:
        """
        Compute rolling median baseline using lookback window.
        Baseline is computed only from historical data (before current point).
        """
        baseline = pd.Series(index=series.index, dtype=float)
        window = min(self.window_size, len(series))
        
        for i in range(len(series)):
            if i == 0:
                baseline.iloc[i] = series.iloc[i]
            elif i < window:
                baseline.iloc[i] = series.iloc[:i].median()
            else:
                baseline.iloc[i] = series.iloc[i-window:i].median()
        
        return baseline
    
    def _zscore_baseline(self, series: pd.Series) -> pd.Series:
        """
        Compute baseline using Z-score method (mean of recent values).
        Uses lookback window to prevent baseline from following anomalies.
        """
        baseline = pd.Series(index=series.index, dtype=float)
        window = min(self.window_size, len(series))
        
        for i in range(len(series)):
            if i == 0:
                baseline.iloc[i] = series.iloc[i]
            elif i < window:
                baseline.iloc[i] = series.iloc[:i].mean()
            else:
                baseline.iloc[i] = series.iloc[i-window:i].mean()
        
        return baseline
    
    def _prophet_baseline(self, series: pd.Series) -> pd.Series:
        """Compute baseline using Facebook Prophet"""
        if not PROPHET_AVAILABLE:
            warnings.warn("Prophet not available, falling back to rolling_mean")
            return self._rolling_mean(series)
        
        try:
            # Prepare data for Prophet
            df = series.reset_index()
            df.columns = ['ds', 'y']
            
            # Create and fit model
            model = Prophet(
                yearly_seasonality=False,
                weekly_seasonality=False,
                daily_seasonality=False,
                changepoint_prior_scale=0.05
            )
            model.fit(df)
            
            # Predict
            future = model.make_future_dataframe(periods=0)
            forecast = model.predict(future)
            
            # Return trend component as baseline
            baseline = pd.Series(
                forecast['trend'].values,
                index=series.index[:len(forecast)]
            )
            
            # Align with original series
            if len(baseline) > len(series):
                baseline = baseline[:len(series)]
            elif len(baseline) < len(series):
                # Extend with last value
                last_val = baseline.iloc[-1]
                extension = pd.Series([last_val] * (len(series) - len(baseline)), 
                                    index=series.index[len(baseline):])
                baseline = pd.concat([baseline, extension])
            
            return baseline
            
        except Exception as e:
            warnings.warn(f"Prophet baseline failed: {e}, falling back to rolling_mean")
            return self._rolling_mean(series)
    
    def _adtk_levelshift_baseline(self, series: pd.Series) -> pd.Series:
        """Compute baseline using ADTK LevelShiftAD"""
        if not ADTK_AVAILABLE:
            warnings.warn("ADTK not available, falling back to rolling_mean")
            return self._rolling_mean(series)
        
        try:
            # Use ADTK to detect level shifts
            detector = LevelShiftAD(c=3.0, side='both', window=min(self.window_size, len(series)))
            anomalies = detector.fit_detect(series)
            
            # Compute baseline by taking rolling mean, but adjusting at level shifts
            baseline = self._rolling_mean(series)
            
            # If level shifts detected, adjust baseline
            if anomalies.any():
                # For simplicity, use rolling mean but could be enhanced
                # to adjust at detected shift points
                pass
            
            return baseline
            
        except Exception as e:
            warnings.warn(f"ADTK baseline failed: {e}, falling back to rolling_mean")
            return self._rolling_mean(series)
    
    def _compute_thresholds(self, series: pd.Series, baseline_series: pd.Series) -> Dict[str, float]:
        """
        Compute upper and lower thresholds
        
        Args:
            series: Original time series
            baseline_series: Baseline values
            
        Returns:
            Dictionary with 'upper' and 'lower' threshold values
        """
        if series.empty or baseline_series.empty:
            return {'upper': None, 'lower': None}
        
        # Compute residuals (difference from baseline)
        residuals = series - baseline_series
        
        # Compute standard deviation of residuals
        std_residuals = residuals.std()
        
        if pd.isna(std_residuals) or std_residuals == 0:
            # Fallback to overall std
            std_residuals = series.std()
            if pd.isna(std_residuals) or std_residuals == 0:
                # If still no std, use a small fraction of mean
                mean_val = abs(series.mean()) if not series.empty else 1.0
                std_residuals = mean_val * 0.1
        
        # Compute thresholds
        latest_baseline = baseline_series.iloc[-1]
        
        if self.adaptive_threshold:
            # Adaptive threshold: adjust sensitivity based on recent volatility
            recent_window = min(self.window_size, len(residuals))
            recent_std = residuals.tail(recent_window).std()
            
            if pd.isna(recent_std) or recent_std == 0:
                recent_std = std_residuals
            
            # Use recent volatility for threshold calculation
            threshold_std = recent_std
        else:
            threshold_std = std_residuals
        
        upper = latest_baseline + self.sensitivity * threshold_std
        lower = latest_baseline - self.sensitivity * threshold_std
        
        return {
            'upper': float(upper),
            'lower': float(lower)
        }
    
    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure"""
        return {
            'baseline_method': self.baseline_method,
            'window_size': self.window_size,
            'sensitivity': self.sensitivity,
            'adaptive_threshold': self.adaptive_threshold,
            'baseline_series': pd.Series(dtype=float),
            'baseline_value': None,
            'upper_threshold': None,
            'lower_threshold': None,
            'timestamp': None,
            'statistics': {
                'mean': None,
                'std': None,
                'median': None,
                'min': None,
                'max': None,
            }
        }

