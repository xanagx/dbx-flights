#!/usr/bin/env bash
set -euo pipefail

mkdir -p data/raw

# nycflights13 datasets (flights has many NA/null values by design)
curl -L "https://raw.githubusercontent.com/byuidatascience/data4python4ds/master/data-raw/flights/flights.csv" -o "data/raw/flights.csv"
curl -L "https://github.com/tidyverse/nycflights13/raw/refs/heads/main/data-raw/airlines.csv" -o "data/raw/airlines.csv"
curl -L "https://github.com/tidyverse/nycflights13/raw/refs/heads/main/data-raw/airports.csv" -o "data/raw/airports.csv"

echo "Downloaded files:"
ls -lh data/raw/*.csv
