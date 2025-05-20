PY3_TEST()

    TIMEOUT(600)
    TAG(ya:manual ya:fat)
    SIZE(LARGE)

    PY_SRCS(
        test_workloads.py
    )

    PEERDIR(
        contrib/python/allure-pytest
        library/python/testing/yatest_common
        ydb/tests/library/harness
        ydb/tests/stress/lib
    )

    TEST_SRCS(
        test_workloads.py
    )

END() 