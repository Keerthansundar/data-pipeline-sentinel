"""PySpark session factory configured for local Delta Lake operation."""

from pyspark.sql import SparkSession

_SPARK_INSTANCE: SparkSession | None = None


def get_spark_session(app_name: str = "PipelineSentinel") -> SparkSession:
    """Create or return a shared local PySpark session with Delta Lake support.

    Returns:
        SparkSession configured with Delta Lake catalog, local[4], and 4 partitions.
    """
    global _SPARK_INSTANCE
    if _SPARK_INSTANCE is not None:
        try:
            if not _SPARK_INSTANCE.sparkContext._jsc.sc().isStopped():
                return _SPARK_INSTANCE
        except Exception:
            _SPARK_INSTANCE = None

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[4]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.replaceWhere.dataSkipping", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    _SPARK_INSTANCE = builder.getOrCreate()
    return _SPARK_INSTANCE
