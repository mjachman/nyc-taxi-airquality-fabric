CREATE   PROCEDURE dbo.usp_LoadGoldFacts
AS
BEGIN
    SET NOCOUNT ON;

    DROP TABLE IF EXISTS dbo.FactAirQualityDaily;
    CREATE TABLE dbo.FactAirQualityDaily AS
    SELECT
        ISNULL(CAST(measurement_date AS DATE), '1900-01-01') AS date,
        ISNULL(CAST(sensor_id AS BIGINT), -1)                AS sensor_id,
        CAST(
            SUM(CAST(value AS DECIMAL(18, 4)) * observed_count)
            / NULLIF(SUM(observed_count), 0)
            AS DECIMAL(12, 4)
        )                                                    AS avg_value,
        CAST(MIN(summary_min) AS DECIMAL(12, 4))             AS min_value,
        CAST(MAX(summary_max) AS DECIMAL(12, 4))             AS max_value,
        CAST(AVG(summary_median) AS DECIMAL(12, 4))          AS median_value,
        CAST(SUM(observed_count) AS INT)                     AS observed_count,
        CAST(AVG(percent_coverage) AS DECIMAL(5, 2))
            AS percent_coverage
    FROM lakehouse_silver.dbo.silver_openaq_measurements
    GROUP BY
        ISNULL(CAST(measurement_date AS DATE), '1900-01-01'),
        ISNULL(CAST(sensor_id AS BIGINT), -1);

    ALTER TABLE dbo.FactAirQualityDaily
    ADD CONSTRAINT PK_FactAirQualityDaily
        PRIMARY KEY NONCLUSTERED (date, sensor_id) NOT ENFORCED;

    ALTER TABLE dbo.FactAirQualityDaily
    ADD CONSTRAINT FK_FactAirQualityDaily_Date
        FOREIGN KEY (date) REFERENCES dbo.DimDate (date_key) NOT ENFORCED;

    ALTER TABLE dbo.FactAirQualityDaily
    ADD CONSTRAINT FK_FactAirQualityDaily_Sensor
        FOREIGN KEY (sensor_id)
        REFERENCES dbo.DimSensors (sensor_id) NOT ENFORCED;

    DROP TABLE IF EXISTS dbo.FactTaxiDaily;
    CREATE TABLE dbo.FactTaxiDaily AS
    SELECT
        ISNULL(CAST(t.pickup_date AS DATE), '1900-01-01')
            AS date_key,
        ISNULL(CAST(t.pu_location_id AS INT), -1)
            AS location_id,
        ISNULL(YEAR(t.pickup_date), -1)
            AS year,
        CAST(COUNT(*) AS INT)
            AS trip_count,
        CAST(SUM(COALESCE(t.passenger_count, 0)) AS INT)
            AS total_passengers,
        CAST(SUM(COALESCE(t.trip_distance, 0)) AS DECIMAL(14, 2))
            AS total_distance,
        CAST(SUM(COALESCE(t.trip_duration_minutes, 0)) AS DECIMAL(14, 2))
            AS total_duration_min,
        CAST(
            AVG(CAST(t.trip_duration_minutes AS DECIMAL(12, 2)))
            AS DECIMAL(10, 2)
        )
            AS avg_duration_min,
        CAST(SUM(COALESCE(t.fare_amount, 0)) AS DECIMAL(14, 2))
            AS total_fare_usd,
        CAST(SUM(COALESCE(t.tip_amount, 0)) AS DECIMAL(14, 2))
            AS total_tip_usd,
        CAST(SUM(COALESCE(t.total_amount, 0)) AS DECIMAL(14, 2))
            AS total_amount_usd,
        CAST(
            SUM(COALESCE(t.fare_amount, 0))
            / NULLIF(MAX(fx.exchange_rate), 0)
            AS DECIMAL(14, 2)
        )
            AS total_fare_eur,
        CAST(
            SUM(COALESCE(t.total_amount, 0))
            / NULLIF(MAX(fx.exchange_rate), 0)
            AS DECIMAL(14, 2)
        )
            AS total_amount_eur
    FROM lakehouse_silver.dbo.silver_taxi_trips AS t
    LEFT JOIN dbo.DimFX AS fx
        ON t.pickup_date = fx.rate_date
    GROUP BY
        t.pickup_date,
        t.pu_location_id;

    ALTER TABLE dbo.FactTaxiDaily
    ADD CONSTRAINT PK_FactTaxiDaily
        PRIMARY KEY NONCLUSTERED (date_key, location_id) NOT ENFORCED;

    ALTER TABLE dbo.FactTaxiDaily
    ADD CONSTRAINT FK_FactTaxiDaily_Date
        FOREIGN KEY (date_key) REFERENCES dbo.DimDate (date_key) NOT ENFORCED;

    ALTER TABLE dbo.FactTaxiDaily
    ADD CONSTRAINT FK_FactTaxiDaily_Zone
        FOREIGN KEY (location_id)
        REFERENCES dbo.DimZone (location_id) NOT ENFORCED;

    ALTER TABLE dbo.FactTaxiDaily
    ADD CONSTRAINT FK_FactTaxiDaily_FX
        FOREIGN KEY (date_key) REFERENCES dbo.DimFX (rate_date) NOT ENFORCED;

    ALTER TABLE dbo.FactTaxiDaily
    ADD CONSTRAINT FK_FactTaxiDaily_GDP
        FOREIGN KEY (year) REFERENCES dbo.DimGDP (year) NOT ENFORCED;

    DROP TABLE IF EXISTS dbo.FactTaxiHourly;
    CREATE TABLE dbo.FactTaxiHourly AS
    SELECT
        ISNULL(CAST(t.pickup_date AS DATE), '1900-01-01')
            AS date_key,
        ISNULL(DATEPART(HOUR, t.tpep_pickup_datetime), 0)
            AS hour_key,
        ISNULL(CAST(t.pu_location_id AS INT), -1)
            AS location_id,
        CAST(COUNT(*) AS INT)
            AS trip_count,
        CAST(SUM(COALESCE(t.trip_duration_minutes, 0)) AS DECIMAL(14, 2))
            AS total_duration_min,
        CAST(
            AVG(CAST(t.trip_duration_minutes AS DECIMAL(12, 2)))
            AS DECIMAL(10, 2)
        )
            AS avg_duration_min,
        CAST(SUM(COALESCE(t.total_amount, 0)) AS DECIMAL(14, 2))
            AS total_amount_usd,
        CAST(
            SUM(COALESCE(t.total_amount, 0))
            / NULLIF(MAX(fx.exchange_rate), 0)
            AS DECIMAL(14, 2)
        )
            AS total_amount_eur
    FROM lakehouse_silver.dbo.silver_taxi_trips AS t
    LEFT JOIN dbo.DimFX AS fx
        ON t.pickup_date = fx.rate_date
    GROUP BY
        t.pickup_date,
        DATEPART(HOUR, t.tpep_pickup_datetime),
        t.pu_location_id;

    ALTER TABLE dbo.FactTaxiHourly
    ADD CONSTRAINT PK_FactTaxiHourly
        PRIMARY KEY NONCLUSTERED (date_key, hour_key, location_id)
        NOT ENFORCED;

    ALTER TABLE dbo.FactTaxiHourly
    ADD CONSTRAINT FK_FactTaxiHourly_Date
        FOREIGN KEY (date_key) REFERENCES dbo.DimDate (date_key) NOT ENFORCED;

    ALTER TABLE dbo.FactTaxiHourly
    ADD CONSTRAINT FK_FactTaxiHourly_Hour
        FOREIGN KEY (hour_key) REFERENCES dbo.DimHour (hour_key) NOT ENFORCED;

    ALTER TABLE dbo.FactTaxiHourly
    ADD CONSTRAINT FK_FactTaxiHourly_Zone
        FOREIGN KEY (location_id)
        REFERENCES dbo.DimZone (location_id) NOT ENFORCED;
END;

GO