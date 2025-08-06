# -*- coding: utf-8 -*-
import flask
import copy
from library.python.monlib.metric_registry import MetricRegistry
from library.python.monlib import encoder
import threading
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)


CONTENT_TYPE_SPACK = 'application/x-solomon-spack'
CONTENT_TYPE_JSON = 'application/json'
app = flask.Flask(__name__)


class Monitor(object):
    def __init__(self):
        self._registry = MetricRegistry()

    @property
    def registry(self):
        return self._registry

    def int_gauge(self, sensor, labels):
        all_labels = copy.deepcopy(labels)
        all_labels.update({'sensor': sensor})
        return self._registry.int_gauge(all_labels)

    def rate(self, sensor, labels):
        all_labels = copy.deepcopy(labels)
        all_labels.update({'sensor': sensor})
        return self._registry.rate(all_labels)


_MONITOR = Monitor()


@app.route('/sensors')
def sensors():
    if flask.request.headers['accept'] == CONTENT_TYPE_SPACK:
        return flask.Response(encoder.dumps(monitor().registry), mimetype=CONTENT_TYPE_SPACK)
    return flask.Response(encoder.dumps(monitor().registry, format='json'), mimetype=CONTENT_TYPE_JSON)


@app.route('/active-faults')
def active_faults():
    """Возвращает список активных нарушений"""
    try:
        from .active_faults_tracker import get_tracker
        tracker = get_tracker()
        active_faults = tracker.get_active_faults()
        
        response = {
            "timestamp": datetime.now().isoformat(),
            "active_faults_count": len(active_faults),
            "active_faults": active_faults
        }
        
        return flask.Response(
            json.dumps(response, indent=2, ensure_ascii=False),
            mimetype=CONTENT_TYPE_JSON
        )
    except Exception as e:
        logger.error(f"Error getting active faults: {e}")
        return flask.Response(
            json.dumps({"error": str(e)}),
            status=500,
            mimetype=CONTENT_TYPE_JSON
        )


@app.route('/all-faults')
def all_faults():
    """Возвращает все нарушения (активные и завершенные)"""
    try:
        from .active_faults_tracker import get_tracker
        tracker = get_tracker()
        
        limit = flask.request.args.get('limit', 100, type=int)
        all_faults = tracker.get_all_faults(limit=limit)
        
        response = {
            "timestamp": datetime.now().isoformat(),
            "total_faults_count": len(all_faults),
            "faults": all_faults
        }
        
        return flask.Response(
            json.dumps(response, indent=2, ensure_ascii=False),
            mimetype=CONTENT_TYPE_JSON
        )
    except Exception as e:
        logger.error(f"Error getting all faults: {e}")
        return flask.Response(
            json.dumps({"error": str(e)}),
            status=500,
            mimetype=CONTENT_TYPE_JSON
        )


@app.route('/')
def index():
    """Главная страница с информацией о доступных эндпоинтах"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Nemesis Monitor</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .endpoint { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
            .endpoint h3 { margin-top: 0; color: #333; }
            .endpoint code { background: #f5f5f5; padding: 2px 5px; border-radius: 3px; }
            .refresh-btn { background: #007cba; color: white; padding: 10px 20px; border: none; border-radius: 5px; cursor: pointer; }
            .refresh-btn:hover { background: #005a87; }
            #active-faults { margin-top: 20px; }
            .fault-item { margin: 10px 0; padding: 10px; border-left: 4px solid #ff6b6b; background: #fff5f5; }
            .fault-item.active { border-left-color: #ff6b6b; }
            .fault-item.extracted { border-left-color: #51cf66; }
            .fault-item.failed { border-left-color: #ff922b; }
        </style>
    </head>
    <body>
        <h1>Nemesis Monitor</h1>
        
        <div class="endpoint">
            <h3>Available Endpoints:</h3>
            <ul>
                <li><code>/sensors</code> - Metrics in Solomon format</li>
                <li><code>/active-faults</code> - Currently active faults</li>
                <li><code>/all-faults</code> - All faults (active and completed)</li>
            </ul>
        </div>
        
        <div class="endpoint">
            <h3>Active Faults</h3>
            <button class="refresh-btn" onclick="loadActiveFaults()">Refresh</button>
            <div id="active-faults">Loading...</div>
        </div>
        
        <script>
            function loadActiveFaults() {
                fetch('/active-faults')
                    .then(response => response.json())
                    .then(data => {
                        const container = document.getElementById('active-faults');
                        if (data.active_faults_count === 0) {
                            container.innerHTML = '<p>No active faults</p>';
                            return;
                        }
                        
                        let html = '<h4>Active Faults (' + data.active_faults_count + '):</h4>';
                        data.active_faults.forEach(fault => {
                            const duration = fault.duration_seconds || 0;
                            const minutes = Math.floor(duration / 60);
                            const seconds = duration % 60;
                            html += `
                                <div class="fault-item active">
                                    <strong>${fault.nemesis_name}</strong> (${fault.fault_type})<br>
                                    Target: ${fault.target}<br>
                                    Duration: ${minutes}m ${seconds}s<br>
                                    Started: ${fault.inject_time}
                                </div>
                            `;
                        });
                        container.innerHTML = html;
                    })
                    .catch(error => {
                        document.getElementById('active-faults').innerHTML = '<p>Error loading data: ' + error + '</p>';
                    });
            }
            
            // Load active faults on page load
            loadActiveFaults();
            
            // Auto-refresh every 30 seconds
            setInterval(loadActiveFaults, 30000);
        </script>
    </body>
    </html>
    """
    return html


def monitor():
    return _MONITOR


def setup_page(host, port):
    logger.info("Setting up monitoring page on %s:%d", host, port)

    def run_flask():
        try:
            logger.info("Starting Flask app on %s:%d", host, port)
            app.run(host, port, debug=False, use_reloader=False)
            logger.info("Flask app started successfully")
        except Exception as e:
            logger.error("Failed to start Flask app: %s", e)
            raise

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask thread started")

    return flask_thread
