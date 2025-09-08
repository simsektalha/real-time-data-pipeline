import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, to_timestamp, from_unixtime, window

APP_NAME = "GbfsSparkBatchBackfill"

parser = argparse.ArgumentParser()
parser.add_argument("--data-base-path", dest="data_base_path", default="/opt/app/data")
parser.add_argument("--pg-host", dest="pg_host", default="postgres")
parser.add_argument("--pg-port", dest="pg_port", default="5432")
parser.add_argument("--pg-db", dest="pg_db", default="postgres")
parser.add_argument("--pg-user", dest="pg_user", default="postgres")
parser.add_argument("--pg-password", dest="pg_password", default="postgres")
parser.add_argument("--gold-table", dest="gold_table", default="station_availability_15m")
parser.add_argument("--date", required=False)
args, _ = parser.parse_known_args()

BASE_PATH = args.data_base_path
BRONZE_PATH = os.path.join(BASE_PATH, "bronze", "station_status")
SILVER_PATH = os.path.join(BASE_PATH, "silver", "station_status")
GOLD_TABLE = args.gold_table

PG_URL = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"
PG_USER = args.pg_user
PG_PWD = args.pg_password


def main(date: str | None):
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    bronze_df = spark.read.format("parquet").load(BRONZE_PATH)

    silver = (
        bronze_df
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

    (silver.write.mode("overwrite" if date else "append").format("parquet").save(SILVER_PATH))

    agg = (
        silver
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

    out = (
        agg
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


if __name__ == "__main__":
    main(args.date)


