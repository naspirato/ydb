PYTEST()

PEERDIR(
    ydb/tests/olap/lib
    ydb/tests/stress/common
    ydb/tests/stress/oltp_workload/workload
    library/python/testing/yatest_common
)

PY_SRCS(
    stress_workload_cli.py
)

END() 