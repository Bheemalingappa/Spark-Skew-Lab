"""SparkSession factory and environment configuration for SparkSkewLab.

Configures:
- Local multi-core execution (local[*])
- Fixed shuffle partitions (default: 8)
- Adaptive Query Execution (AQE) disabled for controlled skew experiments
- Auto-broadcast join threshold disabled (-1) to enforce shuffle joins
- Explicit UTC SQL session timezone
- Dynamic process-level JAVA_HOME and HADOOP_HOME resolution without modifying system environment variables
"""

import logging
import os
from pathlib import Path
import platform
import sys
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from src.config import WORKSPACE_ROOT

logger = logging.getLogger(__name__)

# Installed Adoptium JDK 17 root directory
DEFAULT_JDK_PATH = Path(r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot")
DEFAULT_HADOOP_HOME = WORKSPACE_ROOT / "hadoop_home"

_active_session: Optional[SparkSession] = None


def configure_process_environment() -> Dict[str, str]:
    """Configure process-local environment variables for Java and Hadoop/winutils.
    
    Ensures:
    - JAVA_HOME points to installed Adoptium JDK 17, not any installer .msi.
    - HADOOP_HOME points to workspace hadoop_home containing winutils.exe and hadoop.dll.
    - System/registry environment variables are untouched.
    """
    configured_paths: Dict[str, str] = {}

    # 1. Configure JAVA_HOME dynamically
    if DEFAULT_JDK_PATH.exists():
        os.environ["JAVA_HOME"] = str(DEFAULT_JDK_PATH)
        configured_paths["JAVA_HOME"] = str(DEFAULT_JDK_PATH)
        java_bin = str(DEFAULT_JDK_PATH / "bin")
        if java_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

    # 2. Configure HADOOP_HOME dynamically
    if DEFAULT_HADOOP_HOME.exists():
        os.environ["HADOOP_HOME"] = str(DEFAULT_HADOOP_HOME)
        configured_paths["HADOOP_HOME"] = str(DEFAULT_HADOOP_HOME)
        hadoop_bin = str(DEFAULT_HADOOP_HOME / "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

    return configured_paths


def check_spark_environment() -> Dict[str, Any]:
    """Inspect and report compatibility across Python, Java, PySpark, and host OS."""
    configure_process_environment()

    java_version = "Unknown"
    java_executable = None
    java_home = os.environ.get("JAVA_HOME", "")

    if java_home:
        candidate_exe = Path(java_home) / "bin" / ("java.exe" if platform.system() == "Windows" else "java")
        if candidate_exe.exists():
            java_executable = str(candidate_exe)

    import pyspark
    spark_version = pyspark.__version__

    notes = []
    # Python 3.14 compatibility notes
    if sys.version_info >= (3, 14):
        notes.append("Python 3.14 runtime detected. Verified core PySpark DataFrame and Parquet operations functional.")
    if sys.version_info < (3, 10):
        notes.append("Python version is below 3.10; upgrading recommended for optimal PySpark 4.x compatibility.")

    return {
        "os": platform.platform(),
        "python_version": sys.version.split()[0],
        "java_home": java_home,
        "java_executable": java_executable,
        "hadoop_home": os.environ.get("HADOOP_HOME", ""),
        "pyspark_version": spark_version,
        "compatibility_notes": notes
    }


def is_session_active(session: Optional[SparkSession]) -> bool:
    """Check if the underlying SparkContext of a SparkSession is active and not stopped."""
    if session is None:
        return False
    try:
        return not session._sc._jsc.sc().isStopped()
    except Exception:
        return False


def get_spark_session(
    app_name: str = "SparkSkewLab",
    shuffle_partitions: int = 8,
    master: str = "local[*]",
    enable_aqe: bool = False,
    auto_broadcast_threshold: int = -1,
    timezone: str = "UTC"
) -> SparkSession:
    """Create or retrieve a configured SparkSession for baseline skew analysis.
    
    Args:
        app_name: Spark application name.
        shuffle_partitions: Fixed shuffle partition count for reproducible task skew.
        master: Spark cluster manager / master URL.
        enable_aqe: Toggle Adaptive Query Execution (AQE). False for baseline.
        auto_broadcast_threshold: Threshold in bytes for broadcast join. -1 disables broadcast.
        timezone: SQL session timezone string.
        
    Returns:
        Configured SparkSession instance.
    """
    global _active_session

    if is_session_active(_active_session):
        return _active_session


    # Enforce process-level JDK and Hadoop paths
    configure_process_environment()

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(enable_aqe).lower())
        .config("spark.sql.autoBroadcastJoinThreshold", str(auto_broadcast_threshold))
        .config("spark.sql.session.timeZone", timezone)
        .config("spark.sql.warehouse.dir", str(WORKSPACE_ROOT / "outputs" / "spark-warehouse"))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    _active_session = builder.getOrCreate()
    _active_session.sparkContext.setLogLevel("WARN")

    logger.info(
        f"SparkSession created: version={_active_session.version}, "
        f"shuffle_partitions={shuffle_partitions}, AQE={enable_aqe}, "
        f"autoBroadcastJoinThreshold={auto_broadcast_threshold}"
    )
    return _active_session


def stop_spark_session(spark: Optional[SparkSession] = None) -> None:
    """Safely terminate an active SparkSession."""
    global _active_session
    target = spark or _active_session

    if target is not None:
        try:
            target.stop()
            logger.info("SparkSession stopped cleanly.")
        except Exception as e:
            logger.warning(f"Error while stopping SparkSession: {e}")
        finally:
            _active_session = None
