import allure
import pytest
from ydb.tests.stress.lib.conftest import StressWorkloadBase
from ydb.tests.stress.lib.side_workloads import WorkloadType


@allure.feature('Stress Tests')
class TestExternalWorkloads(StressWorkloadBase):
    """Test suite for running external workloads"""
    
    @allure.story('OLTP')
    @allure.title('OLTP Insert Delete Workload')
    def test_oltp_insert_delete(self):
        """Run OLTP workload with inserts and deletes"""
        # Set workload parameters
        self.workload_type = WorkloadType.OLTP
        self.tables_prefix = "oltp_stress_test"
        self.duration = 30  # seconds
        
        # Run workload and wait for completion
        workload_info = self.run_workload_and_wait(
            workload_id="oltp_test",
            parameters={
                'duration': 30,
                'threads': 4,
                'pool_size': 10
            }
        )
        
        # Assert workload was successful
        self.assert_workload_successful(workload_info)
        
        # Optional: check specific metrics
        if 'metrics' in workload_info:
            metrics = workload_info['metrics']
            if 'qps' in metrics:
                allure.attach(
                    f"QPS: {metrics['qps']}",
                    name="Performance Metrics",
                    attachment_type=allure.attachment_type.TEXT
                )
    
    @allure.story('Key-Value')
    @allure.title('Key-Value Workload')
    def test_kv_workload(self):
        """Run Key-Value workload"""
        # Run workload and wait for completion
        workload_info = self.run_workload_and_wait(
            workload_id="kv_test",
            workload_type=WorkloadType.KV,
            parameters={
                'table_count': 2,
                'key_count': 1000,
                'duration': 30,
                'threads': 4
            }
        )
        
        # Assert workload was successful
        self.assert_workload_successful(workload_info)
    
    @allure.story('Multiple Workloads')
    @allure.title('Multiple Concurrent Workloads')
    def test_multiple_workloads(self):
        """Run multiple workloads concurrently"""
        # Start OLTP workload
        oltp_info = self.start_workload(
            workload_id="concurrent_oltp",
            workload_type=WorkloadType.OLTP,
            parameters={'duration': 30}
        )
        
        # Start KV workload
        kv_info = self.start_workload(
            workload_id="concurrent_kv",
            workload_type=WorkloadType.KV,
            parameters={'duration': 30}
        )
        
        # Wait for both to complete
        oltp_result = self.wait_workload("concurrent_oltp")
        kv_result = self.wait_workload("concurrent_kv")
        
        # Assert both were successful
        self.assert_workload_successful(oltp_result)
        self.assert_workload_successful(kv_result) 