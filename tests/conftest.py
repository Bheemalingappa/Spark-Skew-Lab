"""Shared pytest fixtures for SparkSkewLab test suite."""

import pytest
from pyspark.sql import SparkSession

from src.spark_session import get_spark_session, stop_spark_session


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession reused across all PySpark test modules.
    
    Prevents JVM/Py4J gateway reconnect hangs on Windows by keeping a single
    active SparkContext throughout the test session and shutting down cleanly at the end.
    """
    session = get_spark_session(
        app_name="SparkSkewLab-TestSession",
        shuffle_partitions=8,
        enable_aqe=False,
        auto_broadcast_threshold=-1
    )
    yield session
    stop_spark_session(session)
