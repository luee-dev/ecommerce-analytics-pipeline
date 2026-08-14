from pyspark.sql import SparkSession


def init_lakehouse():

    spark = (
        SparkSession.builder
        .appName("InitIcebergTables")

        # Iceberg + Hadoop S3A dependencies
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            ])
        )

        # Iceberg Spark extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

        # Iceberg catalog
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.lakehouse.type",
            "hadoop"
        )
        .config(
            "spark.sql.catalog.lakehouse.warehouse",
            "s3a://lakehouse/"
        )

        # MinIO / S3
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://localhost:9000"
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            "admin"
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "password123"
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )

        .getOrCreate()
    )

    print("Initializing Iceberg Database and Tables...")

    # Create database
    spark.sql(
        "CREATE DATABASE IF NOT EXISTS lakehouse.ecommerce"
    )

    # Raw events
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.ecommerce.raw_events (
            event_id STRING,
            user_id STRING,
            session_id STRING,
            event_type STRING,
            product_id STRING,
            category STRING,
            price DOUBLE,
            timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(timestamp))
    """)

    # Abandoned carts
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.ecommerce.abandoned_carts (
            session_id STRING,
            user_id STRING,
            abandoned_products_id STRING,
            potential_revenue_loss DOUBLE,
            cart_time TIMESTAMP,
            is_recovered BOOLEAN
        )
        USING iceberg
        PARTITIONED BY (days(cart_time))
    """)

    print("Database & Iceberg Tables ready inside MinIO Storage!")

    spark.stop()


if __name__ == "__main__":
    init_lakehouse()