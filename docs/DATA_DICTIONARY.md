# Data Dictionary: NYC Taxi, Air Quality & Macroeconomics

## 1. Bronze Layer (`lakehouse_bronze`)

| Object | Format / Source | Grain | Notes |
|---|---|---|---|
| `Files/taxi/*.parquet` | NYC TLC Yellow Taxi Parquet (2023-01 to 2025-12) | 1 row per taxi trip | Raw monthly parquet files; `cbd_congestion_fee` appears in 2025 |
| `dbo.taxi_zone_lookup` | NYC TLC Taxi Zone CSV | 1 row per `LocationID` | Reference lookup mapping IDs to borough, zone, and service zone |
| `dbo.bronze_ecb_fx` | ECB Data API (`EXR/D.USD.EUR.SP00.A`) | 1 row per trading day | Daily `USD/EUR` exchange rates (weekends/holidays omitted) |
| `dbo.bronze_world_bank_gdp` | World Bank API (`USA`, `NY.GDP.MKTP.CD`) | 1 row per year | US annual nominal GDP in USD |
| `dbo.bronze_openaq_locations` | OpenAQ v3 `/locations` JSON (`bbox`) | 1 row per sensor | Station coordinates, parameter, units, and `isMonitor` flag |
| `dbo.bronze_openaq_measurements` | OpenAQ v3 `/sensors/{id}/days` JSON | 1 row per `(sensor_id, utc)` | Daily pollutant metrics (`value`, `summary.*`, `coverage.*`) |

---

## 2. Silver Layer (`lakehouse_silver`: Delta Tables + Quarantine)

Every Silver pipeline splits validated data into clean Delta tables and corresponding `*_quarantine` tables with a `dq_error_reason` column rather than discarding bad records.

| Clean Table | Quarantine Table | Primary / Dedup Key | Key Cleaning & Types |
|---|---|---|---|
| `dbo.silver_taxi_trips` | `dbo.silver_taxi_quarantine` | `(vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id, do_location_id, trip_distance, total_amount)` | Money cast to `DecimalType(10, 2)`; `timestamp_ntz` normalized to `timestamp`; derives `pickup_date` and `trip_duration_minutes`; missing fee columns defaulted to 0 |
| `dbo.silver_taxi_zones` | `dbo.silver_taxi_zones_quarantine` | `location_id` | `LocationID` cast to integer; renamed to snake_case; null key or zone name quarantined |
| `dbo.silver_ecb_fx` | `dbo.silver_ecb_fx_quarantine` | `rate_date` | Rate cast to `DecimalType(10, 4)`; checks for positive rate and valid date |
| `dbo.silver_world_bank_gdp` | `dbo.silver_world_bank_gdp_quarantine` | `(year, country_code, indicator_id)` | Value cast to `DecimalType(18, 2)`; checks for positive GDP and valid keys |
| `dbo.silver_openaq_locations` | `dbo.silver_openaq_locations_quarantine` | `sensor_id` | Maps `"id"` to `location_id`, `"isMonitor"` to `is_monitor`; fills null locality with `'Unknown'` |
| `dbo.silver_openaq_measurements` | `dbo.silver_openaq_measurements_quarantine` | `(sensor_id, measurement_date)` | Derives NYC `measurement_date` from local timestamp; exempts temperature readings from non-negative check |

---

## 3. Gold Layer (`warehouse_gold`: T-SQL Star Schema)

All primary and foreign keys use `NONCLUSTERED ... NOT ENFORCED` constraints as supported by Microsoft Fabric Warehouse.

### Dimensions

#### `dbo.DimDate` (1 row per calendar day, Jan 1 to Dec 31)
| Column | Type | Key | Description |
|---|---|---|---|
| `date_key` | `DATE` | PK | Calendar date (`'1900-01-01'` fallback) |
| `year` | `INT` | | Calendar year (2023 to 2025) |
| `month_num` | `INT` | | Month number (1 to 12) for sorting |
| `month` | `VARCHAR(15)` | | Full month name (January to December) |
| `weekday_num` | `INT` | | ISO weekday (1 = Monday, 7 = Sunday) |
| `weekday` | `VARCHAR(15)` | | Day name (Monday to Sunday) |
| `is_weekend` | `BIT` | | Flag: 1 for Saturday/Sunday, 0 for weekdays |

#### `dbo.DimFX` (1 row per calendar day)
| Column | Type | Key | Description |
|---|---|---|---|
| `rate_date` | `DATE` | PK | Calendar date matching `DimDate.date_key` (1:1 relationship in semantic model) |
| `currency` | `CHAR(3)` | | Quote currency (`'USD'`), numerator in ECB `USD per 1 EUR` rate |
| `currency_denom` | `CHAR(3)` | | Base currency (`'EUR'`), denominator in ECB `USD per 1 EUR` rate |
| `exchange_rate` | `DECIMAL(10, 4)` | | ECB rate carried forward over non-trading days |
| `is_carried_forward` | `BIT` | | Flag: 1 if carried forward from prior trading day, 0 if published that day |

#### `dbo.DimGDP` (1 row per year)
| Column | Type | Key | Description |
|---|---|---|---|
| `year` | `INT` | PK | Calendar year (`-1` fallback) |
| `country_code` | `CHAR(3)` | | ISO-3 country code (`'USA'`) |
| `country_name` | `VARCHAR(30)` | | Country name (`'United States'`) |
| `indicator_id` | `VARCHAR(30)` | | World Bank indicator code (`'NY.GDP.MKTP.CD'`) |
| `gdp_usd` | `DECIMAL(18, 2)` | | Annual nominal US GDP in USD |

#### `dbo.DimZone` (1 row per NYC TLC zone)
| Column | Type | Key | Description |
|---|---|---|---|
| `location_id` | `INT` | PK | TLC Taxi Zone ID (1 to 265, `-1` fallback) |
| `zone` | `VARCHAR(100)` | | Zone or neighborhood name (e.g. `JFK Airport`, `Midtown Center`) |
| `borough` | `VARCHAR(30)` | | NYC borough (`Manhattan`, `Queens`, `Brooklyn`, `Bronx`, `Staten Island`, `EWR`, `Unknown`) |
| `service_zone` | `VARCHAR(30)` | | TLC service zone (`Yellow Zone`, `Boro Zone`, `Airports`, `EWR`, `N/A`) |

#### `dbo.DimSensors` (1 row per OpenAQ sensor stream)
| Column | Type | Key | Description |
|---|---|---|---|
| `sensor_id` | `BIGINT` | PK | Unique OpenAQ sensor ID (`-1` fallback) |
| `location_id` | `BIGINT` | | OpenAQ station ID |
| `station_name` | `VARCHAR(100)` | | Air monitoring station name |
| `parameter` | `VARCHAR(20)` | | Measured pollutant (`pm25`, `pm10`, `no2`, `o3`, `co`, `so2`, `nox`) |
| `units` | `VARCHAR(15)` | | Measurement unit (`µg/m³`, `ppm`) |
| `is_monitor` | `BIT` | | 1 for EPA/reference-grade monitor, 0 for low-cost sensor |
| `latitude` | `DECIMAL(9, 6)` | | WGS84 latitude |
| `longitude` | `DECIMAL(9, 6)` | | WGS84 longitude |

#### `dbo.DimHour` (1 row per hour of day)
| Column | Type | Key | Description |
|---|---|---|---|
| `hour_key` | `INT` | PK | Hour of day (0 to 23) |
| `hour_label` | `CHAR(5)` | | Formatted hour label (`00:00` to `23:00`) |

---

### Facts

#### `dbo.FactTaxiDaily` (Grain: 1 row per `(date_key, location_id)`)
| Column | Type | Key | Description |
|---|---|---|---|
| `date_key` | `DATE` | PK, FK -> `DimDate`, `DimFX` | Pickup calendar date |
| `location_id` | `INT` | PK, FK -> `DimZone` | Pickup TLC zone ID |
| `year` | `INT` | FK -> `DimGDP` | Pickup year |
| `trip_count` | `INT` | | Total completed trips |
| `total_passengers` | `INT` | | Total recorded passengers |
| `total_distance` | `DECIMAL(14, 2)` | | Total trip distance in miles |
| `total_duration_min` | `DECIMAL(14, 2)` | | Total trip duration in minutes |
| `avg_duration_min` | `DECIMAL(10, 2)` | | Mean duration per trip in minutes |
| `total_fare_usd` | `DECIMAL(14, 2)` | | Sum of base metered fares in USD |
| `total_tip_usd` | `DECIMAL(14, 2)` | | Sum of tips in USD |
| `total_amount_usd` | `DECIMAL(14, 2)` | | Total trip charges in USD |
| `total_fare_eur` | `DECIMAL(14, 2)` | | Fare converted to EUR (`total_fare_usd / NULLIF(exchange_rate, 0)`) |
| `total_amount_eur` | `DECIMAL(14, 2)` | | Total converted to EUR (`total_amount_usd / NULLIF(exchange_rate, 0)`) |

#### `dbo.FactTaxiHourly` (Grain: 1 row per `(date_key, hour_key, location_id)`)
| Column | Type | Key | Description |
|---|---|---|---|
| `date_key` | `DATE` | PK, FK -> `DimDate` | Pickup calendar date |
| `hour_key` | `INT` | PK, FK -> `DimHour` | Pickup hour (0 to 23) |
| `location_id` | `INT` | PK, FK -> `DimZone` | Pickup TLC zone ID |
| `trip_count` | `INT` | | Total completed trips |
| `total_duration_min` | `DECIMAL(14, 2)` | | Total duration in minutes |
| `avg_duration_min` | `DECIMAL(10, 2)` | | Mean trip duration in minutes |
| `total_amount_usd` | `DECIMAL(14, 2)` | | Total charges in USD |
| `total_amount_eur` | `DECIMAL(14, 2)` | | Total converted to EUR (`total_amount_usd / NULLIF(exchange_rate, 0)`) |

#### `dbo.FactAirQualityDaily` (Grain: 1 row per `(date, sensor_id)`)
| Column | Type | Key | Description |
|---|---|---|---|
| `date` | `DATE` | PK, FK -> `DimDate` | NYC local measurement date |
| `sensor_id` | `BIGINT` | PK, FK -> `DimSensors` | OpenAQ sensor ID |
| `avg_value` | `DECIMAL(12, 4)` | | Observation-weighted daily mean reading |
| `min_value` | `DECIMAL(12, 4)` | | Daily minimum reading |
| `max_value` | `DECIMAL(12, 4)` | | Daily maximum reading |
| `median_value` | `DECIMAL(12, 4)` | | Daily median reading |
| `observed_count` | `INT` | | Number of observations recorded that day |
| `percent_coverage` | `DECIMAL(5, 2)` | | Sensor uptime coverage percentage (0 to 100) |
