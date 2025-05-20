PY3_LIBRARY()

    PY_SRCS(
        conftest.py
        side_workloads.py
    )

    PEERDIR(
        contrib/python/allure-pytest
        library/python/testing/yatest_common
        ydb/public/sdk/python
        ydb/tests/library/harness
        ydb/tests/stress/common
    )

END()