from pyspark.sql import SparkSession

def compact_lakehouse_tables():
    spark = SparkSession.builder \
    	.appName("IcebergTableCompaction") \
    	.config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
    	.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    	.config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    	.config("spark.sql.catalog.lakehouse.type", "hadoop") \
    	.config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/") \        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \        .config("spark.hadoop.fs.s3a.access.key", "admin") \        .config("spark.hadoop.fs.s3a.secret.key", "password123") \        .config("spark.hadoop.fs.s3a.path-style-access", "true") \
    	.getOrCreate()

    print("   Optimizing file layouts for 'raw_events' table...")
    # Iceberg syntax to rewrite data files and clear out orphaned files
    spark.sql("""
    	CALL lakehouse.system.rewrite_data_files(
    		table => 'lakehouse.ecommerce.raw_events',
    		options => map('max-file-size-bytes', '536870912')
    	)
    """)

    print("    Removing expired historical snapshots to free object storage space...")
    spark.sql("CALL lakehouse.system.expire_snapshtos('lakehouse.ecommerce.raw_events')")

    print("   Maintenance optimisation run completed successfully!")
    spark.stop()

if __name__	 == "__main__":
	compact_lakehouse_tables()
