#!/usr/bin/env python3

"""
Summary report generator for analytics events
Generates HTML summary reports with events sorted by priority and links to visualizations
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List


class SummaryReportGenerator:
    """Generate summary HTML reports for detected events"""
    
    @staticmethod
    def generate_summary_html(events: List[Dict[str, Any]], visualization_data: List[Dict[str, Any]],
                               output_dir: str, timestamp: str, job_name: str):
        """
        Generate a summary HTML report with events sorted by priority and links to visualizations
        
        Args:
            events: List of event dictionaries
            visualization_data: List of dicts with visualization info (for generating links)
            output_dir: Directory to save the HTML report
            timestamp: Timestamp string for filenames
            job_name: Job name for filenames
        """
        # Create a mapping from (context_hash, metric_name) to visualization filename
        viz_map = {}
        if visualization_data:
            for viz_data in visualization_data:
                context_hash = viz_data.get('context_hash', '')
                metric_name = viz_data.get('metric_name', '')
                safe_context_hash = context_hash[:8] if context_hash else ''
                viz_filename = f"{job_name}_event_{safe_context_hash}_{metric_name}_{timestamp}.html"
                viz_map[(context_hash, metric_name)] = viz_filename
        
        # Sort events by priority
        def get_event_priority(event):
            """Calculate priority for sorting: higher number = higher priority"""
            severity = event.get('severity', 'medium')
            event_type = event.get('event_type', '')
            
            # Severity priority: critical > high > medium > low
            severity_priority = {
                'critical': 4,
                'high': 3,
                'medium': 2,
                'low': 1
            }.get(severity.lower(), 2)
            
            # Event type priority: degradation > threshold_shift > improvement
            type_priority = {
                'degradation_start': 3,
                'degradation_end': 3,
                'threshold_shift': 2,
                'improvement_start': 1,
                'improvement_end': 1
            }
            type_priority_val = 0
            for key, val in type_priority.items():
                if key in event_type.lower():
                    type_priority_val = val
                    break
            
            # Combine: severity is more important
            return (severity_priority * 10) + type_priority_val
        
        sorted_events = sorted(events, key=get_event_priority, reverse=True)
        
        # Generate HTML
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Analytics Events Summary - {job_name}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
            margin-bottom: 30px;
        }}
        .summary {{
            background-color: #f9f9f9;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 30px;
        }}
        .summary p {{
            margin: 5px 0;
        }}
        .event-card {{
            border-left: 4px solid #ddd;
            padding: 15px;
            margin-bottom: 20px;
            background-color: #fafafa;
            border-radius: 4px;
            transition: box-shadow 0.2s;
        }}
        .event-card:hover {{
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .event-card.critical {{
            border-left-color: #f44336;
            background-color: #ffebee;
        }}
        .event-card.high {{
            border-left-color: #ff9800;
            background-color: #fff3e0;
        }}
        .event-card.medium {{
            border-left-color: #ffc107;
            background-color: #fffde7;
        }}
        .event-card.low {{
            border-left-color: #2196F3;
            background-color: #e3f2fd;
        }}
        .event-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }}
        .event-type {{
            font-size: 18px;
            font-weight: bold;
            color: #333;
        }}
        .event-severity {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
            text-transform: uppercase;
        }}
        .severity-critical {{
            background-color: #f44336;
            color: white;
        }}
        .severity-high {{
            background-color: #ff9800;
            color: white;
        }}
        .severity-medium {{
            background-color: #ffc107;
            color: #333;
        }}
        .severity-low {{
            background-color: #2196F3;
            color: white;
        }}
        .event-details {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 10px;
            margin-top: 10px;
        }}
        .event-detail {{
            padding: 8px;
            background-color: white;
            border-radius: 3px;
        }}
        .event-detail-label {{
            font-weight: bold;
            color: #666;
            font-size: 12px;
            text-transform: uppercase;
        }}
        .event-detail-value {{
            color: #333;
            margin-top: 4px;
        }}
        .visualization-link {{
            display: inline-block;
            margin-top: 10px;
            padding: 8px 16px;
            background-color: #4CAF50;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-weight: bold;
            transition: background-color 0.2s;
        }}
        .visualization-link:hover {{
            background-color: #45a049;
        }}
        .no-viz {{
            color: #999;
            font-style: italic;
        }}
        .timestamp {{
            color: #666;
            font-size: 12px;
            margin-top: 20px;
            text-align: center;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Analytics Events Summary</h1>
        
        <div class="summary">
            <p><strong>Job:</strong> {job_name}</p>
            <p><strong>Total Events:</strong> {len(events)}</p>
            <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <h2>Events (sorted by priority)</h2>
"""
        
        # Add event cards
        for idx, event in enumerate(sorted_events, 1):
            event_type = event.get('event_type', 'unknown')
            severity = event.get('severity', 'medium').lower()
            metric_name = event.get('metric_name', 'N/A')
            context_hash = event.get('context_hash', '')
            context_json = event.get('context_json', '{}')
            
            # Parse context for display
            try:
                context_dict = json.loads(context_json) if context_json else {}
                context_display = ', '.join([f"{k}: {v}" for k, v in sorted(context_dict.items())[:5]])
                if len(context_dict) > 5:
                    context_display += f" ... (+{len(context_dict) - 5} more)"
            except:
                context_display = context_json[:100] + "..." if len(context_json) > 100 else context_json
            
            # Get visualization link
            viz_filename = viz_map.get((context_hash, metric_name))
            viz_link_html = ''
            if viz_filename:
                viz_link_html = f'<a href="{viz_filename}" class="visualization-link" target="_blank">📈 View Visualization</a>'
            else:
                viz_link_html = '<span class="no-viz">No visualization available</span>'
            
            # Format timestamps
            start_time = event.get('event_start_time', '')
            end_time = event.get('event_end_time', '')
            if isinstance(start_time, str):
                try:
                    start_time = pd.to_datetime(start_time).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            elif hasattr(start_time, 'strftime'):
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            
            if isinstance(end_time, str):
                try:
                    end_time = pd.to_datetime(end_time).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            elif hasattr(end_time, 'strftime'):
                end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Format change values
            change_abs = event.get('change_absolute')
            change_rel = event.get('change_relative')
            change_abs_str = f"{change_abs:.2f}" if change_abs is not None else "N/A"
            change_rel_str = f"{change_rel * 100:.1f}%" if change_rel is not None else "N/A"
            
            current_value = event.get('current_value')
            current_value_str = f"{current_value:.2f}" if current_value is not None else "N/A"
            
            html_content += f"""
        <div class="event-card {severity}">
            <div class="event-header">
                <span class="event-type">#{idx}: {event_type}</span>
                <span class="event-severity severity-{severity}">{severity}</span>
            </div>
            <div class="event-details">
                <div class="event-detail">
                    <div class="event-detail-label">Metric</div>
                    <div class="event-detail-value">{metric_name}</div>
                </div>
                <div class="event-detail">
                    <div class="event-detail-label">Start Time</div>
                    <div class="event-detail-value">{start_time}</div>
                </div>
                <div class="event-detail">
                    <div class="event-detail-label">End Time</div>
                    <div class="event-detail-value">{end_time or 'Ongoing'}</div>
                </div>
                <div class="event-detail">
                    <div class="event-detail-label">Current Value</div>
                    <div class="event-detail-value">{current_value_str}</div>
                </div>
                <div class="event-detail">
                    <div class="event-detail-label">Absolute Change</div>
                    <div class="event-detail-value">{change_abs_str}</div>
                </div>
                <div class="event-detail">
                    <div class="event-detail-label">Relative Change</div>
                    <div class="event-detail-value">{change_rel_str}</div>
                </div>
            </div>
            <div class="event-detail" style="margin-top: 10px;">
                <div class="event-detail-label">Context</div>
                <div class="event-detail-value" style="font-size: 11px; color: #666;">{context_display}</div>
            </div>
            {viz_link_html}
        </div>
"""
        
        html_content += f"""
        <div class="timestamp">
            Report generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
</body>
</html>
"""
        
        # Save HTML file
        html_filename = f"{job_name}_summary_{timestamp}.html"
        html_filepath = os.path.join(output_dir, html_filename)
        with open(html_filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"  ✓ Generated summary HTML report: {html_filename}")

