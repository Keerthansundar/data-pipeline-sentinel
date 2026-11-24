from pipeline.spark_session import get_spark_session

spark = get_spark_session("inspect")
d = (
    spark.read.format("delta")
    .load("/opt/airflow/data/bronze/orders")
    .filter("_batch_date = '2026-10-16'")
)
print(d.columns)
d.select("amount", "total_amount").show(5, False)
d.groupBy("amount").count().show(5, False)
