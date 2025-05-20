#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CLI Tool for running YDB stress workloads

This script provides a command-line interface to run various stress workloads
from the ydb/tests/stress directory.

Examples:
    # List available workloads
    python stress_workload_cli.py list
    
    # Run OLTP workload for 5 minutes
    python stress_workload_cli.py run oltp --workload-name oltp_test --duration 300
    
    # Run mixed workload with bulk_upsert subtype
    python stress_workload_cli.py run mixed --workload-name mixed_test --subtype bulk_upsert
    
    # Run custom workload
    python stress_workload_cli.py custom --module-path ydb.tests.stress.custom.MyWorkload --workload-name custom_test
"""

import sys
import os

# Make sure ydb directory is in path
script_dir = os.path.dirname(os.path.abspath(__file__))
ydb_root = os.path.abspath(os.path.join(script_dir, '../../../'))
sys.path.insert(0, ydb_root)

from ydb.tests.olap.lib.workload_cli import main

if __name__ == '__main__':
    main() 