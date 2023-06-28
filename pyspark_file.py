from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a Spark session
spark = SparkSession.builder.master("local").appName("ClickstreamProcessing").getOrCreate()

# Read data from HBase into a Spark DataFrame
df = spark.read \
    .format("org.apache.hadoop.hbase.spark") \
    .option("hbase.table", "clickstream_data") \
    .option("hbase.columns.mapping", "click_data:user_id,click_data:timestamp,click_data:url") \
    .load()

# Perform aggregations
aggregated_df = df.groupBy("url", "country") \
    .agg(
        countDistinct("user_id").alias("unique_users"),
        count("url").alias("clicks"),
        avg("time_spent").alias("avg_time_spent")
    )

# Convert the aggregated DataFrame to Pandas for further processing or indexing into Elasticsearch
aggregated_data = aggregated_df.toPandas()
