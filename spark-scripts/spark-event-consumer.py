import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import from_json, window

# Mengatur path untuk file .env
dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

# Mengambil nilai dari variabel lingkungan
spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

# Membuat string host Spark
spark_host = f"spark://{spark_hostname}:{spark_port}"

# Mengatur argumen PYSPARK_SUBMIT_ARGS
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

# Inisialisasi SparkContext
sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")

# Inisialisasi SparkSession
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# Membaca data streaming dari Kafka
stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# Definisi skema untuk data pembelian dari Kafka
schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField("transaction_id", pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField("timestamp", pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField("product", pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField("amount", pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField("customer_id", pyspark.sql.types.StringType())
])

# Agregasi total pembelian setiap sepuluh menit
agg_df = (
    stream_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json("value", schema).alias("data"))
    .select("data.*")
    .withWatermark("timestamp", "10 minutes")
    .groupBy(window("timestamp", "10 minutes"))
    .sum("amount")
    .withColumnRenamed("sum(amount)", "total_purchase")
    .orderBy("window")
)

# Menuliskan output ke konsol
query = agg_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Menjalankan streaming job
query.awaitTermination()
