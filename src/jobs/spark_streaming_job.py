import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, to_timestamp, from_unixtime, window, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "gbfs.station_status.json")
APP_NAME = "GbfsSparkStreaming"
BASE_PATH = os.getenv("DATA_BASE_PATH", "/opt/app/data")
BRONZE_PATH = os.path.join(BASE_PATH, "bronze", "station_status")
SILVER_PATH = os.path.join(BASE_PATH, "silver", "station_status")
CHECKPOINT_BASE = os.path.join(BASE_PATH, "checkpoints")
GOLD_TABLE = os.getenv("GOLD_TABLE", "station_availability_15m")

PG_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','postgres')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','postgres')}"
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PWD = os.getenv("POSTGRES_PASSWORD", "postgres")

json_schema = StructType([
    StructField("station_id", StringType(), False),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_ebikes_available", IntegerType(), True),
    StructField("num_docks_available", IntegerType(), True),
    StructField("is_installed", BooleanType(), True),
    StructField("is_renting", BooleanType(), True),
    StructField("is_returning", BooleanType(), True),
    StructField("last_reported", LongType(), True),
])

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .getOrCreate()
)

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

decoded = raw.selectExpr("CAST(value AS STRING) as json_str")
decoded = decoded.select(from_json(col("json_str"), json_schema).alias("r")).select("r.*")

# Bronze: Parquet files
bronze_q = (
    decoded.writeStream
    .format("parquet")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "bronze"))
    .outputMode("append")
    .start()
)

# Silver: cleaned Parquet
silver = (
    decoded
    .withColumn("event_ts", to_timestamp(from_unixtime(col("last_reported"))))
    .withColumn("is_installed_bool", when(col("is_installed") == 1, True).otherwise(col("is_installed").cast("boolean")))
    .withColumn("is_renting_bool", when(col("is_renting") == 1, True).otherwise(col("is_renting").cast("boolean")))
    .withColumn("is_returning_bool", when(col("is_returning") == 1, True).otherwise(col("is_returning").cast("boolean")))
    .drop("is_installed", "is_renting", "is_returning")
    .withColumnRenamed("is_installed_bool", "is_installed")
    .withColumnRenamed("is_renting_bool", "is_renting")
    .withColumnRenamed("is_returning_bool", "is_returning")
    .dropDuplicates(["station_id", "last_reported"])
    .withColumn(
        "pct_bikes_available",
        when(
            (col("num_bikes_available") + col("num_docks_available")) > 0,
            (col("num_bikes_available") / (col("num_bikes_available") + col("num_docks_available")).cast("double"))
        ).otherwise(None)
    )
)

silver_q = (
    silver.writeStream
    .format("parquet")
    .option("path", SILVER_PATH)
    .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "silver"))
    .outputMode("append")
    .start()
)

# Gold: 15-min aggregates to Postgres
agg = (
    silver
    .withWatermark("event_ts", "2 hours")
    .groupBy(
        window(col("event_ts"), "15 minutes"),
        col("station_id")
    )
    .agg(
        expr("avg(pct_bikes_available) as avg_pct_bikes_available"),
        expr("avg(num_bikes_available) as avg_bikes"),
        expr("avg(num_docks_available) as avg_docks")
    )
)


def write_gold_to_pg(batch_df, batch_id: int):
    out = (
        batch_df
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )
    (
        out.write
        .format("jdbc")
        .mode("append")
        .option("url", PG_URL)
        .option("user", PG_USER)
        .option("password", PG_PWD)
        .option("dbtable", GOLD_TABLE)
        .option("driver", "org.postgresql.Driver")
        .save()
    )


gold_q = (
    agg.writeStream
    .foreachBatch(write_gold_to_pg)
    .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "gold"))
    .outputMode("update")
    .start()
)

spark.streams.awaitAnyTermination()


