"""Smoke test for PySpark and Delta Lake inside Airflow container."""

import pyspark


def test_spark_delta_smoke():
    """Verify PySpark can create a Delta table, write to it, and read back."""
    builder = (
        pyspark.sql.SparkSession.builder.appName("DeltaSmokeTest")
        .master("local[1]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = builder.getOrCreate()

    data = [(1, "smoke_test_pass"), (2, "delta_lake_ready")]
    df = spark.createDataFrame(data, ["id", "status"])

    test_path = "/tmp/delta_smoke_table"
    df.write.format("delta").mode("overwrite").save(test_path)

    read_back = spark.read.format("delta").load(test_path)
    count = read_back.count()
    print(f"DELTA_SMOKE_TEST: SUCCESS, rows={count}")
    assert count == 2
    spark.stop()


if __name__ == "__main__":
    test_spark_delta_smoke()
