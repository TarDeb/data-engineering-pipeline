from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder.appName("OrderTransformation").getOrCreate()

df = spark.read.json("data/raw_orders.json")

result = df.groupBy("customer_id").agg(spark_sum(col("amount")).alias("total_spent"))

result.write.mode("overwrite").json("data/transformed_orders.json")

spark.stop()

