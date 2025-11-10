#!/usr/bin/env python3
"""
Web server for MASS (Metric Analytic Super System) UI
Provides web interface for config building, running analytics, and viewing reports
"""

import os
import sys
import json
import subprocess
import glob
from datetime import datetime
from pathlib import Path
from flask import Flask, send_file, send_from_directory, request, jsonify, render_template_string
from flask_cors import CORS
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
# Check in multiple locations: mas_ui/, analytics/, and project root
env_paths = [
    Path(__file__).parent / '.env',
    Path(__file__).parent.parent / '.env',
    Path(__file__).parent.parent.parent.parent / '.env',
]
for env_path in env_paths:
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Loaded environment variables from {env_path}")
        break

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app)

# Get paths
BASE_DIR = Path(__file__).parent
ANALYTICS_DIR = BASE_DIR.parent
CONFIGS_DIR = ANALYTICS_DIR / 'configs'
DRY_RUN_OUTPUT_DIR = ANALYTICS_DIR / 'dry_run_output'


@app.route('/')
def index():
    """Serve the main UI page"""
    return send_file('index.html')


@app.route('/api/configs', methods=['GET'])
def list_configs():
    """List all available config files"""
    configs = []
    if CONFIGS_DIR.exists():
        for config_file in CONFIGS_DIR.glob('*.yaml'):
            configs.append({
                'name': config_file.name,
                'path': str(config_file.relative_to(ANALYTICS_DIR))
            })
    return jsonify({'configs': configs})


@app.route('/api/run', methods=['POST'])
def run_analytics():
    """Run analytics job with given config"""
    try:
        data = request.json
        config_path = data.get('config_path')
        dry_run = data.get('dry_run', True)
        event_deepness = data.get('event_deepness')
        
        if not config_path:
            return jsonify({'error': 'config_path is required'}), 400
        
        # Resolve config path
        if not os.path.isabs(config_path):
            config_path = os.path.join(ANALYTICS_DIR, config_path)
        
        if not os.path.exists(config_path):
            return jsonify({'error': f'Config file not found: {config_path}'}), 404
        
        # Build command - use absolute path to Python script
        analytics_script = os.path.join(ANALYTICS_DIR, 'analytics_job.py')
        if not os.path.exists(analytics_script):
            return jsonify({'error': f'Analytics script not found: {analytics_script}'}), 500
        
        cmd = [
            sys.executable,
            analytics_script,
            '--config', config_path
        ]
        
        if dry_run:
            cmd.append('--dry-run')
        
        if event_deepness:
            cmd.extend(['--event-deepness', event_deepness])
        
        # Prepare environment variables for subprocess
        # Copy current environment and ensure YDB credentials are passed
        env = os.environ.copy()
        
        # Ensure CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is available
        # Priority: 1) Already in env, 2) CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS_FILE points to file, 3) Try common locations
        if 'CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS' not in env or not env['CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS']:
            # Try to read from file if specified
            creds_file = env.get('CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS_FILE')
            if creds_file and os.path.exists(creds_file):
                with open(creds_file, 'r') as f:
                    env['CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS'] = f.read().strip()
            # If still not found, try to read from common credential file locations
            elif not env.get('CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS'):
                common_cred_paths = [
                    os.path.expanduser('~/.ydb/credentials.json'),
                    os.path.expanduser('~/ydb_credentials.json'),
                    '/etc/ydb/credentials.json',
                ]
                for cred_path in common_cred_paths:
                    if os.path.exists(cred_path):
                        env['CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS'] = cred_path
                        break
        
        # Check if credentials are still missing and provide helpful error
        if 'CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS' not in env or not env['CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS']:
            error_msg = (
                "YDB credentials not found. Please set CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS environment variable.\n"
                "Options:\n"
                "1. Create .env file in mas_ui/ directory with: CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/credentials.json\n"
                "2. Export before running: export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/credentials.json\n"
                "3. Set CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS_FILE=/path/to/credentials.json to auto-load from file"
            )
            return jsonify({
                'success': False,
                'error': error_msg
            }), 400
        
        # Run analytics job with environment variables
        result = subprocess.run(
            cmd,
            cwd=ANALYTICS_DIR,
            env=env,  # Pass environment variables
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode != 0:
            return jsonify({
                'success': False,
                'error': result.stderr,
                'output': result.stdout
            }), 500
        
        # Find generated reports
        reports = find_reports()
        
        return jsonify({
            'success': True,
            'output': result.stdout,
            'reports': reports
        })
        
    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Analytics job timed out'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reports', methods=['GET'])
def list_reports():
    """List all available reports"""
    reports = find_reports()
    return jsonify({'reports': reports})


def find_reports():
    """Find summary HTML reports in dry_run_output directory"""
    reports = []
    if DRY_RUN_OUTPUT_DIR.exists():
        # Find only summary reports (pattern: *_summary_*.html)
        # Summary reports have "_summary_" in the name
        # Detail reports have "_event_" (singular) and are excluded
        for report_file in DRY_RUN_OUTPUT_DIR.glob('*_summary_*.html'):
            stat = report_file.stat()
            reports.append({
                'name': report_file.name,
                'type': 'summary',
                'path': f'/api/report/{report_file.name}',
                'size': stat.st_size,
                'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
            })
    
    # Sort by modified time (newest first)
    reports.sort(key=lambda x: x['modified'], reverse=True)
    return reports


@app.route('/api/report/<filename>')
def get_report(filename):
    """Serve a report file"""
    # Security: only allow HTML files
    if not filename.endswith('.html'):
        return jsonify({'error': 'Invalid file type'}), 400
    
    # Security: prevent directory traversal
    if '..' in filename or '/' in filename:
        return jsonify({'error': 'Invalid filename'}), 400
    
    report_path = DRY_RUN_OUTPUT_DIR / filename
    if not report_path.exists():
        return jsonify({'error': 'Report not found'}), 404
    
    return send_file(str(report_path))


@app.route('/api/save-config', methods=['POST'])
def save_config():
    """Save a config file"""
    try:
        data = request.json
        filename = data.get('filename')
        content = data.get('content')
        
        if not filename or not content:
            return jsonify({'error': 'filename and content are required'}), 400
        
        # Security: only allow YAML files
        if not filename.endswith('.yaml') and not filename.endswith('.yml'):
            return jsonify({'error': 'Only YAML files are allowed'}), 400
        
        # Security: prevent directory traversal
        if '..' in filename or '/' in filename:
            return jsonify({'error': 'Invalid filename'}), 400
        
        # Save to configs directory
        config_path = CONFIGS_DIR / filename
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return jsonify({
            'success': True,
            'path': str(config_path.relative_to(ANALYTICS_DIR))
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='MASS (Metric Analytic Super System) UI Server')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    print(f"Starting MASS (Metric Analytic Super System) UI server on http://{args.host}:{args.port}")
    print(f"Open http://{args.host}:{args.port} in your browser")
    
    app.run(host=args.host, port=args.port, debug=args.debug)

