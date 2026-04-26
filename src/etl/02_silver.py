from __future__ import annotations

from pyspark.sql import functions as F

from common import fq_schema, get_spark, load_config, parse_args


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    spark = get_spark("flights-silver")

    bronze = fq_schema(cfg.catalog, cfg.bronze_schema)
    silver = fq_schema(cfg.catalog, cfg.silver_schema)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver}")

    flights_b = spark.table(f"{bronze}.flights_bronze")
    airlines_b = spark.table(f"{bronze}.airlines_bronze")
    airports_b = spark.table(f"{bronze}.airports_bronze")

    flights_s = (
        flights_b.withColumn("year", F.col("year").cast("int"))
        .withColumn("month", F.col("month").cast("int"))
        .withColumn("day", F.col("day").cast("int"))
        .withColumn("dep_time", F.col("dep_time").cast("int"))
        .withColumn("sched_dep_time", F.col("sched_dep_time").cast("int"))
        .withColumn("dep_delay", F.col("dep_delay").cast("double"))
        .withColumn("arr_time", F.col("arr_time").cast("int"))
        .withColumn("sched_arr_time", F.col("sched_arr_time").cast("int"))
        .withColumn("arr_delay", F.col("arr_delay").cast("double"))
        .withColumn("flight", F.col("flight").cast("int"))
        .withColumn("air_time", F.col("air_time").cast("double"))
        .withColumn("distance", F.col("distance").cast("double"))
        .withColumn("hour", F.col("hour").cast("int"))
        .withColumn("minute", F.col("minute").cast("int"))
        .filter("year IS NOT NULL AND month BETWEEN 1 AND 12 AND day BETWEEN 1 AND 31")
        .dropDuplicates(["year", "month", "day", "carrier", "flight", "origin", "dest"])
    )

    airlines_s = airlines_b.select(
        F.trim(F.col("carrier")).alias("carrier"),
        F.trim(F.col("name")).alias("carrier_name"),
    ).dropDuplicates(["carrier"])

    airports_s = (
        airports_b.select(
            F.trim(F.col("faa")).alias("faa"),
            F.trim(F.col("name")).alias("airport_name"),
            F.lit(None).cast("string").alias("city"),
            F.lit(None).cast("string").alias("country"),
            F.trim(F.col("tz")).cast("int").alias("timezone_num"),
            F.trim(F.col("tzone")).alias("timezone_name"),
            F.col("lat").cast("double").alias("lat"),
            F.col("lon").cast("double").alias("lon"),
            F.col("alt").cast("double").alias("alt"),
        )
        .filter("faa IS NOT NULL")
        .dropDuplicates(["faa"])
    )

    flights_s.write.format("delta").mode("overwrite").saveAsTable(f"{silver}.flights_silver")
    airlines_s.write.format("delta").mode("overwrite").saveAsTable(f"{silver}.airlines_silver")
    airports_s.write.format("delta").mode("overwrite").saveAsTable(f"{silver}.airports_silver")

    spark.stop()


if __name__ == "__main__":
    main()
