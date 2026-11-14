"""Unit tests for SparkSession lifecycle and configuration in SparkSkewLab."""

import pytest
from pyspark.sql import SparkSession

from src.spark_session import (
    check_spark_environment,
    get_spark_session,
    is_session_active,
    stop_spark_session,
)


@pytest.fixture(scope="module")
def spark_instance(spark: SparkSession):
    """Module-level fixture to provide test SparkSession."""
    return spark


def test_spark_session_starts_and_stops():
    """Verify session starts and terminates cleanly."""
    session = get_spark_session(app_name="SparkSkewLab-LifecycleTest")
    assert session is not None
    assert is_session_active(session)
    assert session.version.startswith("4.")

    df = session.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    assert df.count() == 2



def test_spark_session_configurations(spark_instance: SparkSession):
    """Verify baseline spark configurations (AQE disabled, broadcast disabled, shuffle partitions)."""
    conf = spark_instance.sparkContext.getConf()
    sql_conf = spark_instance.conf

    # Verify shuffle partitions
    assert sql_conf.get("spark.sql.shuffle.partitions") == "8"

    # Verify AQE is disabled for baseline
    assert sql_conf.get("spark.sql.adaptive.enabled") == "false"

    # Verify broadcast join is disabled
    assert sql_conf.get("spark.sql.autoBroadcastJoinThreshold") == "-1"

    # Verify SQL timezone
    assert sql_conf.get("spark.sql.session.timeZone") == "UTC"


def test_check_spark_environment():
    """Verify environment inspection utility returns accurate runtime metadata."""
    info = check_spark_environment()
    assert "python_version" in info
    assert "pyspark_version" in info
    assert "java_home" in info
    assert "hadoop_home" in info
    assert info["pyspark_version"].startswith("4.")
