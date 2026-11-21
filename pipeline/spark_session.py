"""PySpark session factory configured for local Delta Lake operation."""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "PipelineSentinel") -> SparkSession:
    """Create and configure a local PySpark session with Delta Lake support.

    Returns:
        SparkSession configured with Delta Lake catalog and extensions.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    return builder.getOrCreate()
