from __future__ import annotations

from common import fq_schema, get_spark, load_config, parse_args


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    spark = get_spark("flights-bronze")

    bronze = fq_schema(cfg.catalog, cfg.bronze_schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze}")

    null_markers = ["", "NA", "N/A", "NULL", "null", "NaN", "nan"]

    flights = (
        spark.read.option("header", True)
        .option("multiLine", False)
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .csv(cfg.raw_paths["flights"])
    )

    airlines = (
        spark.read.option("header", True)
        .option("nullValue", "")
        .csv(cfg.raw_paths["airlines"])
    )

    airports = (
        spark.read.option("header", True)
        .option("nullValue", "")
        .csv(cfg.raw_paths["airports"])
    )

    # Bronze keeps data mostly raw, only normalizes known null markers.
    flights = flights.na.replace(null_markers, None)
    airlines = airlines.na.replace(null_markers, None)
    airports = airports.na.replace(null_markers, None)

    flights.write.format("delta").mode("overwrite").saveAsTable(f"{bronze}.flights_bronze")
    airlines.write.format("delta").mode("overwrite").saveAsTable(f"{bronze}.airlines_bronze")
    airports.write.format("delta").mode("overwrite").saveAsTable(f"{bronze}.airports_bronze")

    spark.stop()


if __name__ == "__main__":
    main()
