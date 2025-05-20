from __future__ import annotations

import os
import subprocess
import time
import signal
import logging
import threading
import tempfile
import json
from typing import Dict, List, Optional, Any, Tuple, Union
from enum import Enum
import yatest.common
from ydb.tests.library.harness.kikimr_config import KikimrConfig
from ydb.tests.library.common.types import Endpoints


logger = logging.getLogger(__name__)


class WorkloadType(str, Enum):
    """Types of workloads that can be run as external processes"""
    OLTP = 'oltp'
    KV = 'kv'
    MIXED = 'mixed'
    SIMPLE_QUEUE = 'simpleq'
    STATISTICS = 'statistics'
    OLAP = 'olap'
    LOG = 'log'


class WorkloadStatus(str, Enum):
    """Status of a workload process"""
    NOT_STARTED = 'not_started'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    KILLED = 'killed'


class ExternalWorkloadManager:
    """Manages external workload processes for stress testing"""
    
    def __init__(self):
        self.workloads: Dict[str, Dict[str, Any]] = {}
        self.monitor_thread = None
        self.monitor_stop_event = threading.Event()

    def _get_binary_path(self, workload_type: WorkloadType) -> str:
        """Get the path to the binary for a specific workload type"""
        binary_map = {
            WorkloadType.OLTP: 'ydb/tests/stress/oltp_workload/workload/bin/workload',
            WorkloadType.KV: 'ydb/tests/stress/kv/workload/bin/workload',
            WorkloadType.MIXED: 'ydb/tests/stress/mixedpy/workload/bin/workload',
            WorkloadType.SIMPLE_QUEUE: 'ydb/tests/stress/simple_queue/workload/bin/workload',
            WorkloadType.STATISTICS: 'ydb/tests/stress/statistics_workload/workload/bin/workload',
            WorkloadType.OLAP: 'ydb/tests/stress/olap_workload/workload/bin/workload',
            WorkloadType.LOG: 'ydb/tests/stress/log/workload/bin/workload',
        }
        
        binary_path = binary_map.get(workload_type)
        if not binary_path:
            raise ValueError(f"Unsupported workload type: {workload_type}")
        
        return yatest.common.binary_path(binary_path)

    def _get_common_args(self, path: str) -> List[str]:
        """Get common command line arguments for all workloads"""
        return [
            '--endpoint', Endpoints.grpc_url,
            '--database', KikimrConfig.get_database_name(),
            '--path', path,
        ]

    def _get_workload_specific_args(self, workload_type: WorkloadType, params: Dict[str, Any]) -> List[str]:
        """Get workload-specific command line arguments"""
        args = []
        
        if workload_type == WorkloadType.KV:
            if 'table_count' in params:
                args.extend(['--table-count', str(params['table_count'])])
            if 'key_count' in params:
                args.extend(['--key-count', str(params['key_count'])])
                
        elif workload_type == WorkloadType.MIXED:
            if 'subtype' in params:
                args.extend(['--subtype', params['subtype']])
            if 'rows' in params:
                args.extend(['--rows', str(params['rows'])])
            if 'len' in params:
                args.extend(['--len', str(params['len'])])
                
        elif workload_type == WorkloadType.SIMPLE_QUEUE:
            if 'queue_count' in params:
                args.extend(['--queue-count', str(params['queue_count'])])
        
        # Add duration if provided
        if 'duration' in params:
            args.extend(['--duration', str(params['duration'])])
            
        # Add any other parameters
        for key, value in params.items():
            if key not in ['table_count', 'key_count', 'subtype', 'rows', 'len', 'queue_count', 'duration']:
                args.extend([f'--{key.replace("_", "-")}', str(value)])
                
        return args

    def start_workload(self, workload_id: str, workload_type: WorkloadType, 
                      path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Start a workload as an external process
        
        Args:
            workload_id: Unique identifier for this workload
            workload_type: Type of workload to run
            path: Path in YDB for tables
            params: Additional parameters for the workload
            
        Returns:
            Dict with workload information
        """
        if workload_id in self.workloads:
            logger.warning(f"Workload {workload_id} already exists, will be replaced")
            self.stop_workload(workload_id)
        
        params = params or {}
        binary_path = self._get_binary_path(workload_type)
        
        # Create output files for stdout and stderr
        output_dir = yatest.common.output_path(f'workload_{workload_id}')
        os.makedirs(output_dir, exist_ok=True)
        
        stdout_path = os.path.join(output_dir, 'stdout.log')
        stderr_path = os.path.join(output_dir, 'stderr.log')
        metrics_path = os.path.join(output_dir, 'metrics.json')
        
        stdout_file = open(stdout_path, 'w')
        stderr_file = open(stderr_path, 'w')
        
        # Build command line
        cmd = [binary_path]
        cmd.extend(self._get_common_args(path))
        cmd.extend(self._get_workload_specific_args(workload_type, params))
        cmd.extend(['--metrics-file', metrics_path])
        
        logger.info(f"Starting workload {workload_id}: {' '.join(cmd)}")
        
        # Start the process
        try:
            process = subprocess.Popen(
                cmd,
                stdout=stdout_file,
                stderr=stderr_file,
                start_new_session=True  # Create a new process group
            )
            
            workload_info = {
                'id': workload_id,
                'type': workload_type,
                'path': path,
                'params': params,
                'process': process,
                'pid': process.pid,
                'start_time': time.time(),
                'status': WorkloadStatus.RUNNING,
                'stdout_path': stdout_path,
                'stderr_path': stderr_path,
                'metrics_path': metrics_path,
                'stdout_file': stdout_file,
                'stderr_file': stderr_file,
            }
            
            self.workloads[workload_id] = workload_info
            
            # Start monitoring thread if not already running
            if self.monitor_thread is None or not self.monitor_thread.is_alive():
                self._start_monitor()
                
            return {k: v for k, v in workload_info.items() if k not in ['process', 'stdout_file', 'stderr_file']}
            
        except Exception as e:
            logger.error(f"Failed to start workload {workload_id}: {e}")
            stdout_file.close()
            stderr_file.close()
            raise
            
    def stop_workload(self, workload_id: str) -> Dict[str, Any]:
        """
        Stop a running workload
        
        Args:
            workload_id: ID of workload to stop
            
        Returns:
            Dict with workload information
        """
        if workload_id not in self.workloads:
            raise ValueError(f"Workload {workload_id} not found")
            
        workload = self.workloads[workload_id]
        process = workload['process']
        
        logger.info(f"Stopping workload {workload_id} (PID: {process.pid})")
        
        # Try to terminate gracefully first
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            process.wait(timeout=10)  # Wait up to 10 seconds for process to exit
        except subprocess.TimeoutExpired:
            # Force kill if not terminated
            logger.warning(f"Workload {workload_id} did not terminate, killing...")
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except OSError:
                # Process may have exited
                pass
        except OSError:
            # Process may have already exited
            pass
            
        # Close output files
        try:
            workload['stdout_file'].close()
        except:
            pass
        
        try:
            workload['stderr_file'].close()
        except:
            pass
            
        # Update status
        workload['status'] = WorkloadStatus.KILLED
        workload['end_time'] = time.time()
        
        # Read metrics if available
        metrics = self._read_metrics(workload['metrics_path'])
        if metrics:
            workload['metrics'] = metrics
            
        # Return workload info without process and file handles
        return {k: v for k, v in workload.items() if k not in ['process', 'stdout_file', 'stderr_file']}
        
    def get_workload_status(self, workload_id: str) -> Dict[str, Any]:
        """
        Get current status of a workload
        
        Args:
            workload_id: ID of workload to check
            
        Returns:
            Dict with workload status information
        """
        if workload_id not in self.workloads:
            raise ValueError(f"Workload {workload_id} not found")
            
        workload = self.workloads[workload_id]
        process = workload['process']
        
        # Check if process has completed
        if process.poll() is not None:
            if workload['status'] == WorkloadStatus.RUNNING:
                # Process completed since last check
                workload['status'] = WorkloadStatus.COMPLETED if process.returncode == 0 else WorkloadStatus.FAILED
                workload['end_time'] = time.time()
                workload['return_code'] = process.returncode
                
                # Close output files
                try:
                    workload['stdout_file'].close()
                except:
                    pass
                
                try:
                    workload['stderr_file'].close()
                except:
                    pass
                    
                # Read metrics if available
                metrics = self._read_metrics(workload['metrics_path'])
                if metrics:
                    workload['metrics'] = metrics
            
        # Return workload info without process and file handles
        return {k: v for k, v in workload.items() if k not in ['process', 'stdout_file', 'stderr_file']}
        
    def _read_metrics(self, metrics_path: str) -> Optional[Dict[str, Any]]:
        """Read metrics from a file if it exists"""
        try:
            if os.path.exists(metrics_path):
                with open(metrics_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error reading metrics file {metrics_path}: {e}")
        return None
        
    def _start_monitor(self):
        """Start background thread to monitor running workloads"""
        self.monitor_stop_event.clear()
        self.monitor_thread = threading.Thread(target=self._monitor_workloads)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
    def _monitor_workloads(self):
        """Monitor workloads and update status"""
        while not self.monitor_stop_event.is_set():
            try:
                for workload_id in list(self.workloads.keys()):
                    self.get_workload_status(workload_id)
            except Exception as e:
                logger.error(f"Error in workload monitor: {e}")
                
            # Check every 5 seconds
            self.monitor_stop_event.wait(5)
            
    def stop_all_workloads(self):
        """Stop all running workloads"""
        for workload_id in list(self.workloads.keys()):
            try:
                self.stop_workload(workload_id)
            except Exception as e:
                logger.error(f"Error stopping workload {workload_id}: {e}")
                
        # Stop monitor thread
        self.monitor_stop_event.set()
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=10)


# Global workload manager instance
_workload_manager = None

def get_workload_manager() -> ExternalWorkloadManager:
    """Get or create the global workload manager instance"""
    global _workload_manager
    if _workload_manager is None:
        _workload_manager = ExternalWorkloadManager()
    return _workload_manager 