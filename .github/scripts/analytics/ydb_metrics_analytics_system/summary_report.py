#!/usr/bin/env python3

"""
Summary report generator for analytics events
Generates HTML summary reports with events grouped by context, collapsed by default
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from collections import defaultdict


class SummaryReportGenerator:
    """Generate summary HTML reports for detected events"""
    
    @staticmethod
    def generate_summary_html(events: List[Dict[str, Any]], visualization_data: List[Dict[str, Any]],
                               output_dir: str, timestamp: str, job_name: str):
        """
        Generate a summary HTML report with events grouped by context, collapsed by default
        
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
        
        # Group events by context_hash
        events_by_context = defaultdict(list)
        for event in events:
            context_hash = event.get('context_hash', 'unknown')
            events_by_context[context_hash].append(event)
        
        # Sort events within each group by priority
        def get_event_priority(event):
            """Calculate priority for sorting: higher number = higher priority"""
            severity = event.get('severity', 'medium')
            event_type = event.get('event_type', '')
            
            severity_priority = {
                'critical': 4, 'high': 3, 'medium': 2, 'low': 1
            }.get(severity.lower(), 2)
            
            type_priority = {
                'degradation_start': 3, 'degradation_end': 3,
                'threshold_shift': 2,
                'improvement_start': 1, 'improvement_end': 1
            }
            type_priority_val = 0
            for key, val in type_priority.items():
                if key in event_type.lower():
                    type_priority_val = val
                    break
            
            return (severity_priority * 10) + type_priority_val
        
        # Sort groups by highest priority event in group
        context_groups = []
        for context_hash, context_events in events_by_context.items():
            sorted_events = sorted(context_events, key=get_event_priority, reverse=True)
            # Count good (improvement) and bad (degradation) events
            good_count = sum(1 for e in context_events if 'improvement' in e.get('event_type', '').lower())
            bad_count = sum(1 for e in context_events if 'degradation' in e.get('event_type', '').lower())
            
            # Get context info from first event
            first_event = sorted_events[0]
            context_json = first_event.get('context_json', '{}')
            metric_name = first_event.get('metric_name', 'N/A')
            
            context_groups.append({
                'context_hash': context_hash,
                'context_json': context_json,
                'metric_name': metric_name,
                'events': sorted_events,
                'good_count': good_count,
                'bad_count': bad_count,
                'max_priority': get_event_priority(sorted_events[0]) if sorted_events else 0
            })
        
        # Sort groups by max priority
        context_groups.sort(key=lambda x: x['max_priority'], reverse=True)
        
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
            line-height: 1.4;
            margin: 0;
            padding: 15px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 8px;
            margin-bottom: 20px;
            font-size: 24px;
        }}
        .summary {{
            background-color: #f9f9f9;
            padding: 12px;
            border-radius: 5px;
            margin-bottom: 20px;
            font-size: 14px;
        }}
        .summary p {{
            margin: 3px 0;
        }}
        .context-group {{
            border: 1px solid #ddd;
            border-radius: 4px;
            margin-bottom: 10px;
            background-color: #fafafa;
        }}
        .context-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 15px;
            cursor: pointer;
            user-select: none;
            background-color: white;
            border-bottom: 1px solid #eee;
        }}
        .context-header:hover {{
            background-color: #f5f5f5;
        }}
        .context-header.collapsed {{
            border-bottom: none;
        }}
        .context-info {{
            flex: 1;
            display: flex;
            align-items: center;
            gap: 15px;
        }}
        .context-name {{
            font-weight: 600;
            color: #333;
            font-size: 14px;
        }}
        .context-metric {{
            color: #666;
            font-size: 12px;
            padding: 2px 8px;
            background-color: #e3f2fd;
            border-radius: 3px;
        }}
        .event-badges {{
            display: flex;
            gap: 8px;
            align-items: center;
        }}
        .badge {{
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: bold;
            cursor: pointer;
            transition: opacity 0.2s;
        }}
        .badge:hover {{
            opacity: 0.8;
        }}
        .badge-good {{
            background-color: #4CAF50;
            color: white;
        }}
        .badge-bad {{
            background-color: #f44336;
            color: white;
        }}
        .badge-all {{
            background-color: #2196F3;
            color: white;
        }}
        .collapse-icon {{
            font-size: 12px;
            color: #666;
            margin-left: 10px;
        }}
        .context-events {{
            display: none;
            padding: 10px 15px;
        }}
        .context-events.expanded {{
            display: block;
        }}
        .event-item {{
            display: flex;
            align-items: center;
            padding: 6px 10px;
            margin-bottom: 4px;
            border-left: 3px solid #ddd;
            background-color: white;
            border-radius: 3px;
            font-size: 12px;
        }}
        .event-item:hover {{
            background-color: #f9f9f9;
        }}
        .event-item.critical {{
            border-left-color: #f44336;
        }}
        .event-item.high {{
            border-left-color: #ff9800;
        }}
        .event-item.medium {{
            border-left-color: #ffc107;
        }}
        .event-item.low {{
            border-left-color: #2196F3;
        }}
        .event-item.improvement {{
            border-left-color: #4CAF50;
        }}
        .event-main {{
            flex: 1;
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .event-type {{
            font-weight: 600;
            color: #333;
            min-width: 140px;
        }}
        .event-time {{
            color: #666;
            font-size: 11px;
            min-width: 150px;
        }}
        .event-metric {{
            color: #666;
            font-size: 11px;
            min-width: 80px;
        }}
        .event-change {{
            color: #666;
            font-size: 11px;
            min-width: 100px;
        }}
        .event-severity {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 10px;
            font-weight: bold;
            text-transform: uppercase;
            min-width: 60px;
            text-align: center;
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
        .event-viz-link {{
            margin-left: 10px;
            padding: 4px 10px;
            background-color: #4CAF50;
            color: white;
            text-decoration: none;
            border-radius: 3px;
            font-size: 11px;
            font-weight: bold;
        }}
        .event-viz-link:hover {{
            background-color: #45a049;
        }}
        .timestamp {{
            color: #666;
            font-size: 11px;
            margin-top: 20px;
            text-align: center;
        }}
        .filter-active {{
            opacity: 1 !important;
            font-weight: bold;
            box-shadow: 0 0 0 2px rgba(33, 150, 243, 0.3);
        }}
        .filter-hidden {{
            display: none;
        }}
    </style>
    <script>
        function toggleContext(contextId) {{
            const eventsDiv = document.getElementById('events-' + contextId);
            const header = document.getElementById('header-' + contextId);
            const icon = document.getElementById('icon-' + contextId);
            
            if (eventsDiv.classList.contains('expanded')) {{
                eventsDiv.classList.remove('expanded');
                header.classList.add('collapsed');
                icon.textContent = '▶';
            }} else {{
                eventsDiv.classList.add('expanded');
                header.classList.remove('collapsed');
                icon.textContent = '▼';
            }}
        }}
        
        function filterEvents(contextId, filterType, event) {{
            event.stopPropagation();
            const eventsDiv = document.getElementById('events-' + contextId);
            const eventItems = eventsDiv.querySelectorAll('.event-item');
            const badges = document.querySelectorAll('#badges-' + contextId + ' .badge');
            
            // Update badge styles
            badges.forEach(b => b.classList.remove('filter-active'));
            event.target.classList.add('filter-active');
            
            // Filter events
            eventItems.forEach(item => {{
                if (filterType === 'all') {{
                    item.classList.remove('filter-hidden');
                }} else if (filterType === 'good') {{
                    if (item.classList.contains('improvement')) {{
                        item.classList.remove('filter-hidden');
                    }} else {{
                        item.classList.add('filter-hidden');
                    }}
                }} else if (filterType === 'bad') {{
                    if (item.classList.contains('improvement')) {{
                        item.classList.add('filter-hidden');
                    }} else {{
                        item.classList.remove('filter-hidden');
                    }}
                }}
            }});
        }}
    </script>
</head>
<body>
    <div class="container">
        <h1>📊 Analytics Events Summary</h1>
        
        <div class="summary">
            <p><strong>Job:</strong> {job_name}</p>
            <p><strong>Total Events:</strong> {len(events)} | <strong>Context Groups:</strong> {len(context_groups)}</p>
            <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <h2>Events by Context (click to expand)</h2>
"""
        
        # Add context groups
        for idx, group in enumerate(context_groups, 1):
            context_hash = group['context_hash']
            context_json = group['context_json']
            metric_name = group['metric_name']
            events = group['events']
            good_count = group['good_count']
            bad_count = group['bad_count']
            
            # Parse context for display
            try:
                context_dict = json.loads(context_json) if context_json else {}
                context_display = ', '.join([f"{k}: {v}" for k, v in sorted(context_dict.items())[:4]])
                if len(context_dict) > 4:
                    context_display += f" ..."
            except:
                context_display = context_json[:80] + "..." if len(context_json) > 80 else context_json
            
            context_id = f"ctx_{idx}"
            
            html_content += f"""
        <div class="context-group">
            <div class="context-header collapsed" id="header-{context_id}" onclick="toggleContext('{context_id}')">
                <div class="context-info">
                    <span class="collapse-icon" id="icon-{context_id}">▶</span>
                    <span class="context-name">{context_display}</span>
                    <span class="context-metric">{metric_name}</span>
                </div>
                <div class="event-badges" id="badges-{context_id}">
                    <span class="badge badge-all filter-active" onclick="filterEvents('{context_id}', 'all', event)">ALL {len(events)}</span>
                    <span class="badge badge-bad" onclick="filterEvents('{context_id}', 'bad', event)">BAD {bad_count}</span>
                    <span class="badge badge-good" onclick="filterEvents('{context_id}', 'good', event)">GOOD {good_count}</span>
                </div>
            </div>
            <div class="context-events" id="events-{context_id}">
"""
            
            # Add events
            for event in events:
                event_type = event.get('event_type', 'unknown')
                severity = event.get('severity', 'medium').lower()
                is_improvement = 'improvement' in event_type.lower()
                
                # Get visualization link
                viz_filename = viz_map.get((context_hash, metric_name))
                viz_link_html = ''
                if viz_filename:
                    viz_link_html = f'<a href="{viz_filename}" class="event-viz-link" target="_blank">📈</a>'
                
                # Format timestamps
                start_time = event.get('event_start_time', '')
                if isinstance(start_time, str):
                    try:
                        start_time = pd.to_datetime(start_time).strftime('%Y-%m-%d %H:%M')
                    except:
                        pass
                elif hasattr(start_time, 'strftime'):
                    start_time = start_time.strftime('%Y-%m-%d %H:%M')
                
                # Format change
                change_abs = event.get('change_absolute')
                change_rel = event.get('change_relative')
                change_str = f"{change_abs:+.1f}" if change_abs is not None else "N/A"
                if change_rel is not None:
                    change_str += f" ({change_rel*100:+.1f}%)"
                
                current_value = event.get('current_value')
                value_str = f"{current_value:.1f}" if current_value is not None else "N/A"
                
                event_class = severity
                if is_improvement:
                    event_class += ' improvement'
                
                html_content += f"""
                <div class="event-item {event_class}">
                    <div class="event-main">
                        <span class="event-type">{event_type}</span>
                        <span class="event-time">{start_time}</span>
                        <span class="event-metric">{value_str}</span>
                        <span class="event-change">{change_str}</span>
                        <span class="event-severity severity-{severity}">{severity}</span>
                    </div>
                    {viz_link_html}
                </div>
"""
            
            html_content += """
            </div>
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
