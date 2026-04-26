-- Row counts across medallion layers
SELECT 'bronze_flights' AS table_name, COUNT(*) AS row_count FROM workspace.flights_bronze.flights_bronze
UNION ALL
SELECT 'silver_flights' AS table_name, COUNT(*) AS row_count FROM workspace.flights_silver.flights_silver
UNION ALL
SELECT 'gold_fact_flights' AS table_name, COUNT(*) AS row_count FROM workspace.flights_gold.fact_flights;

-- Null check examples after cleaning
SELECT
  SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS null_years,
  SUM(CASE WHEN month IS NULL THEN 1 ELSE 0 END) AS null_months,
  SUM(CASE WHEN day IS NULL THEN 1 ELSE 0 END) AS null_days
FROM workspace.flights_silver.flights_silver;

-- Example snowflake join
SELECT
  d.flight_date,
  m.month_name,
  w.day_name,
  c.carrier_name,
  ao.city AS origin_city,
  ad.city AS dest_city,
  f.dep_delay,
  f.arr_delay
FROM workspace.flights_gold.fact_flights f
LEFT JOIN workspace.flights_gold.dim_date d ON f.date_sk = d.date_sk
LEFT JOIN workspace.flights_gold.dim_month m ON d.month_num = m.month_num
LEFT JOIN workspace.flights_gold.dim_day_of_week w ON d.day_of_week_num = w.day_of_week_num
LEFT JOIN workspace.flights_gold.dim_carrier c ON f.carrier_code = c.carrier_code
LEFT JOIN workspace.flights_gold.dim_airport ao ON f.origin_airport_sk = ao.airport_sk
LEFT JOIN workspace.flights_gold.dim_airport ad ON f.dest_airport_sk = ad.airport_sk
LIMIT 100;
