from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def run_streaming_pipeline():
    spark = SparkSession.builder \
        .appName("EcommerceCartAbandonmentStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "user-behavior-events") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_stream = raw_kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))

    raw_query = parsed_stream.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoints/raw_events") \
        .toTable("lakehouse.ecommerce.raw_events")

    abandonment_stream = parsed_stream \
        .withWatermark("timestamp", "10 minutes") \
        .filter(col("event_type") == "add_to_cart") \
        .select(
            col("session_id"),
            col("user_id"),
            col("product_id").alias("abandoned_products_id"),
            col("price").alias("potential_revenue_loss"),
            col("timestamp").alias("cart_time"),
            expr("false").alias("is_recovered")
        )

    analytics_query = abandonment_stream.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoints/abandoned_carts") \
        .toTable("lakehouse.ecommerce.abandoned_carts")

    print("Streaming Pipeline active and listening. Monitoring revenue leakages...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_streaming_pipeline()