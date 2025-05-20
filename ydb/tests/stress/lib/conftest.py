from __future__ import annotations

import os
import pytest
import logging
import time
import allure
from typing import Dict, Any, List, Optional
import yatest.common
from ydb.tests.library.harness.test_case import TestCase
from ydb.tests.library.harness.kikimr_config import KikimrConfig
from ydb.tests.stress.lib.side_workloads import WorkloadType, get_workload_manager, WorkloadStatus


logger = logging.getLogger(__name__)


class StressWorkloadBase(TestCase):
    """
    Base class for stress tests that use external binaries.
    
    This class provides methods for starting and managing workloads
    that run as separate processes, similar to ydb/tests/stability/tool/__main__.py.
    """
    
    # Test configuration - override in subclasses
    workload_type: WorkloadType = None
    tables_prefix: str = "stress_test"
    duration: int = 60  # Duration in seconds
    parameters: Dict[str, Any] = {}  # Additional parameters for workload
    
    @classmethod
    def _get_path(cls) -> str:
        """Get database path for tables"""
        return f"{cls.tables_prefix}"
    
    def setup_method(self, method):
        """Setup before each test method"""
        super().setup_method(method)
        self.active_workloads = []
        self.manager = get_workload_manager()
    
    def teardown_method(self, method):
        """Cleanup after each test method"""
        # Stop any active workloads
        for workload_id in self.active_workloads:
            try:
                self.manager.stop_workload(workload_id)
            except Exception as e:
                logger.error(f"Error stopping workload {workload_id}: {e}")
        
        super().teardown_method(method)
    
    def start_workload(self, workload_id: str, 
                      workload_type: Optional[WorkloadType] = None, 
                      parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Start a workload with the specified parameters
        
        Args:
            workload_id: Unique identifier for this workload
            workload_type: Type of workload (defaults to class workload_type)
            parameters: Additional parameters (combined with class parameters)
            
        Returns:
            Dict with workload information
        """
        workload_type = workload_type or self.workload_type
        if not workload_type:
            pytest.fail("No workload_type specified")
        
        # Combine parameters
        params = self.parameters.copy()
        if parameters:
            params.update(parameters)
            
        # Add duration if not already specified
        if 'duration' not in params:
            params['duration'] = self.duration
            
        path = self._get_path()
        
        with allure.step(f"Starting {workload_type} workload: {workload_id}"):
            logger.info(f"Starting {workload_type} workload: {workload_id} with params: {params}")
            workload_info = self.manager.start_workload(workload_id, workload_type, path, params)
            self.active_workloads.append(workload_id)
            
            # Add workload details to allure report
            allure.attach(
                f"Workload ID: {workload_id}\n"
                f"Type: {workload_type}\n"
                f"Path: {path}\n"
                f"Parameters: {params}",
                name="Workload Details",
                attachment_type=allure.attachment_type.TEXT
            )
            
            return workload_info
    
    def wait_workload(self, workload_id: str, timeout: int = None) -> Dict[str, Any]:
        """
        Wait for a workload to complete
        
        Args:
            workload_id: ID of the workload to wait for
            timeout: Maximum time to wait in seconds, or None for no timeout
            
        Returns:
            Dict with workload information
        """
        if timeout is None:
            timeout = self.duration + 60  # Default: workload duration + 60 seconds
            
        start_time = time.time()
        
        with allure.step(f"Waiting for workload {workload_id} to complete"):
            while True:
                workload_info = self.manager.get_workload_status(workload_id)
                
                if workload_info['status'] in [WorkloadStatus.COMPLETED, WorkloadStatus.FAILED, WorkloadStatus.KILLED]:
                    break
                    
                if time.time() - start_time > timeout:
                    # Timeout reached, stop the workload
                    logger.warning(f"Timeout waiting for workload {workload_id}, stopping it")
                    return self.manager.stop_workload(workload_id)
                    
                time.sleep(1)
                
            # Get final status and metrics
            workload_info = self.manager.get_workload_status(workload_id)
            
            # Attach logs and metrics to allure report
            if os.path.exists(workload_info['stdout_path']):
                with open(workload_info['stdout_path'], 'r') as f:
                    stdout = f.read()
                    allure.attach(stdout, name=f"{workload_id} stdout", attachment_type=allure.attachment_type.TEXT)
                    
            if os.path.exists(workload_info['stderr_path']):
                with open(workload_info['stderr_path'], 'r') as f:
                    stderr = f.read()
                    allure.attach(stderr, name=f"{workload_id} stderr", attachment_type=allure.attachment_type.TEXT)
                    
            if 'metrics' in workload_info:
                allure.attach(
                    str(workload_info['metrics']),
                    name=f"{workload_id} metrics",
                    attachment_type=allure.attachment_type.TEXT
                )
                
            if workload_info['status'] == WorkloadStatus.COMPLETED:
                logger.info(f"Workload {workload_id} completed successfully")
            else:
                logger.warning(f"Workload {workload_id} {workload_info['status']}")
                
            return workload_info
    
    def run_workload_and_wait(self, workload_id: str, 
                             workload_type: Optional[WorkloadType] = None,
                             parameters: Optional[Dict[str, Any]] = None,
                             timeout: Optional[int] = None) -> Dict[str, Any]:
        """
        Start a workload and wait for it to complete
        
        Args:
            workload_id: Unique identifier for this workload
            workload_type: Type of workload (defaults to class workload_type)
            parameters: Additional parameters (combined with class parameters)
            timeout: Maximum time to wait in seconds
            
        Returns:
            Dict with workload information
        """
        self.start_workload(workload_id, workload_type, parameters)
        return self.wait_workload(workload_id, timeout)
    
    def assert_workload_successful(self, workload_info: Dict[str, Any]):
        """
        Assert that a workload completed successfully
        
        Args:
            workload_info: Workload information from wait_workload or run_workload_and_wait
        """
        with allure.step("Verifying workload success"):
            assert workload_info['status'] == WorkloadStatus.COMPLETED, \
                f"Workload {workload_info['id']} failed with status: {workload_info['status']}"


@pytest.fixture(scope="class")
def kikimr_config():
    """Standard fixture to provide KikimrConfig"""
    return KikimrConfig() 