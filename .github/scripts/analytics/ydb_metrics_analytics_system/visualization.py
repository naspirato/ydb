#!/usr/bin/env python3

"""
Visualization module for analytics events
Generates plotly interactive charts for detected events
"""

import os
import pandas as pd
from typing import Dict, Any, List


class EventVisualizer:
    """Generate visualizations for detected events"""
    
    @staticmethod
    def generate_visualizations(visualization_data: List[Dict[str, Any]], 
                                output_dir: str, timestamp: str, job_name: str):
        """
        Generate plotly visualizations for groups with events
        
        Args:
            visualization_data: List of dicts with series, baseline_result, events, context info
            output_dir: Directory to save visualizations
            timestamp: Timestamp string for filenames
            job_name: Job name for filenames
        """
        try:
            import plotly.graph_objects as go
        except ImportError:
            print("  ⚠ plotly not available, skipping visualizations")
            return
        
        print(f"\n  Generating visualizations for {len(visualization_data)} groups with events...")
        
        for idx, viz_data in enumerate(visualization_data):
            try:
                series = viz_data['series']
                baseline_result = viz_data['baseline_result']
                events = viz_data['events']
                metric_name = viz_data['metric_name']
                context_hash = viz_data['context_hash']
                context_json = viz_data['context_json']
                
                # Create figure
                fig = go.Figure()
                
                # Get baseline series if available
                baseline_series = baseline_result.get('baseline_series')
                upper_threshold = baseline_result.get('upper_threshold')
                lower_threshold = baseline_result.get('lower_threshold')
                
                # Plot metric values
                fig.add_trace(go.Scatter(
                    x=series.index,
                    y=series.values,
                    mode='lines+markers',
                    name='Metric Value',
                    line=dict(color='blue', width=2),
                    marker=dict(size=4)
                ))
                
                # Plot baseline if available
                if baseline_series is not None and not baseline_series.empty:
                    fig.add_trace(go.Scatter(
                        x=baseline_series.index,
                        y=baseline_series.values,
                        mode='lines',
                        name='Baseline',
                        line=dict(color='green', width=2, dash='dash')
                    ))
                
                # Plot thresholds
                if upper_threshold is not None:
                    fig.add_trace(go.Scatter(
                        x=series.index,
                        y=[upper_threshold] * len(series),
                        mode='lines',
                        name='Upper Threshold',
                        line=dict(color='red', width=1, dash='dot')
                    ))
                
                if lower_threshold is not None:
                    fig.add_trace(go.Scatter(
                        x=series.index,
                        y=[lower_threshold] * len(series),
                        mode='lines',
                        name='Lower Threshold',
                        line=dict(color='orange', width=1, dash='dot')
                    ))
                
                # Highlight event periods
                for event in events:
                    event_start = pd.to_datetime(event['event_start_time'])
                    event_end = pd.to_datetime(event.get('event_end_time', event_start))
                    event_type = event['event_type']
                    
                    # Color based on event type
                    if 'degradation' in event_type:
                        color = 'rgba(255, 0, 0, 0.2)'
                    elif 'improvement' in event_type:
                        color = 'rgba(0, 255, 0, 0.2)'
                    else:
                        color = 'rgba(255, 255, 0, 0.2)'
                    
                    # Add shaded region for event period
                    fig.add_vrect(
                        x0=event_start,
                        x1=event_end,
                        fillcolor=color,
                        layer="below",
                        line_width=0,
                        annotation_text=event_type,
                        annotation_position="top left"
                    )
                    
                    # Add marker at event start
                    event_value = series.get(event_start, None)
                    if event_value is None:
                        # Find closest point
                        closest_idx = series.index.get_indexer([event_start], method='nearest')[0]
                        if closest_idx >= 0:
                            event_value = series.iloc[closest_idx]
                            event_time = series.index[closest_idx]
                        else:
                            continue
                    else:
                        event_time = event_start
                    
                    fig.add_trace(go.Scatter(
                        x=[event_time],
                        y=[event_value],
                        mode='markers',
                        name=f"{event_type} start",
                        marker=dict(size=12, symbol='star', color='red' if 'degradation' in event_type else 'green'),
                        showlegend=False
                    ))
                
                # Update layout
                fig.update_layout(
                    title=f"{metric_name} - Events Visualization<br><sub>{context_json[:100]}...</sub>",
                    xaxis_title="Time",
                    yaxis_title=metric_name,
                    hovermode='x unified',
                    height=600,
                    showlegend=True,
                    legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
                )
                
                # Save to HTML file
                safe_context_hash = context_hash[:8]  # Use first 8 chars for filename
                filename = f"{job_name}_event_{safe_context_hash}_{metric_name}_{timestamp}.html"
                filepath = os.path.join(output_dir, filename)
                fig.write_html(filepath)
                
            except Exception as e:
                print(f"    ⚠ Could not generate visualization for group {idx}: {e}")
        
        print(f"  ✓ Generated {len(visualization_data)} visualization files in {output_dir}")

