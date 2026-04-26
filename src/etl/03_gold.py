from __future__ import annotations

from common import fq_schema, get_spark, load_config, parse_args


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    spark = get_spark("flights-gold")

    silver = fq_schema(cfg.catalog, cfg.silver_schema)
    gold = fq_schema(cfg.catalog, cfg.gold_schema)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold}")

    flights = spark.table(f"{silver}.flights_silver")
    carriers = spark.table(f"{silver}.airlines_silver")
    airports = spark.table(f"{silver}.airports_silver")

    flights.createOrReplaceTempView("flights")
    carriers.createOrReplaceTempView("carriers")
    airports.createOrReplaceTempView("airports")

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {gold}.dim_timezone AS
        SELECT
          DENSE_RANK() OVER (ORDER BY COALESCE(timezone_name, 'UNKNOWN'), COALESCE(timezone_num, -999)) AS timezone_sk,
          COALESCE(timezone_name, 'UNKNOWN') AS timezone_name,
          COALESCE(timezone_num, -999) AS timezone_num
        FROM airports
        GROUP BY COALESCE(timezone_name, 'UNKNOWN'), COALESCE(timezone_num, -999)
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {gold}.dim_airport AS
        SELECT
          DENSE_RANK() OVER (ORDER BY a.faa) AS airport_sk,
          a.faa AS airport_code,
          a.airport_name,
          a.city,
          a.country,
          t.timezone_sk
        FROM airports a
        LEFT JOIN {gold}.dim_timezone t
          ON COALESCE(a.timezone_name, 'UNKNOWN') = t.timezone_name
         AND COALESCE(a.timezone_num, -999) = t.timezone_num
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {gold}.dim_carrier AS
        SELECT
          carrier AS carrier_code,
          carrier_name
        FROM carriers
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {gold}.dim_month AS
        SELECT * FROM VALUES
          (1, 'January', 1), (2, 'February', 1), (3, 'March', 1),
          (4, 'April', 2), (5, 'May', 2), (6, 'June', 2),
          (7, 'July', 3), (8, 'August', 3), (9, 'September', 3),
          (10, 'October', 4), (11, 'November', 4), (12, 'December', 4)
        AS m(month_num, month_name, quarter_num)
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {gold}.dim_day_of_week AS
        SELECT * FROM VALUES
          (1, 'Monday', false), (2, 'Tuesday', false), (3, 'Wednesday', false),
          (4, 'Thursday', false), (5, 'Friday', false), (6, 'Saturday', true),
          (7, 'Sunday', true)
        AS d(day_of_week_num, day_name, is_weekend)
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {gold}.dim_date AS
        SELECT
          DENSE_RANK() OVER (ORDER BY flight_date) AS date_sk,
          flight_date,
          year(flight_date) AS year_num,
          month(flight_date) AS month_num,
          day(flight_date) AS day_num,
          CAST(date_format(flight_date, 'u') AS INT) AS day_of_week_num
        FROM (
          SELECT DISTINCT make_date(year, month, day) AS flight_date
          FROM flights
          WHERE year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL
        )
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {gold}.fact_flights AS
        SELECT
          sha2(concat_ws(
            '||',
            CAST(f.year AS STRING),
            CAST(f.month AS STRING),
            CAST(f.day AS STRING),
            COALESCE(f.carrier, ''),
            CAST(f.flight AS STRING),
            COALESCE(f.origin, ''),
            COALESCE(f.dest, '')
          ), 256) AS flight_key,
          dd.date_sk,
          c.carrier_code,
          ao.airport_sk AS origin_airport_sk,
          ad.airport_sk AS dest_airport_sk,
          f.dep_time,
          f.sched_dep_time,
          f.dep_delay,
          f.arr_time,
          f.sched_arr_time,
          f.arr_delay,
          f.air_time,
          f.distance,
          f.hour,
          f.minute
        FROM flights f
        LEFT JOIN {gold}.dim_date dd
          ON make_date(f.year, f.month, f.day) = dd.flight_date
        LEFT JOIN {gold}.dim_carrier c
          ON f.carrier = c.carrier_code
        LEFT JOIN {gold}.dim_airport ao
          ON f.origin = ao.airport_code
        LEFT JOIN {gold}.dim_airport ad
          ON f.dest = ad.airport_code
        """
    )

    spark.stop()


if __name__ == "__main__":
    main()
