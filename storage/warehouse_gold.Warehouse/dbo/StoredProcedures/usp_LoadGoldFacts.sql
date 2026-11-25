CREATE   PROCEDURE dbo.usp_LoadGoldFacts
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE dbo.FactAirQualityDaily;
    INSERT INTO dbo.FactAirQualityDaily (
        date, sensor_id, avg_value, min_value, max_value,
        median_value, observed_count, percent_coverage
    )
    SELECT
        ISNULL(CAST(measurement_date AS DATE), '1900-01-01'),
        ISNULL(CAST(sensor_id AS BIGINT), -1),
        CAST(
            SUM(CAST(value AS DECIMAL(18, 4)) * observed_count)
            / NULLIF(SUM(observed_count), 0)
            AS DECIMAL(12, 4)
        ),
        CAST(MIN(summary_min) AS DECIMAL(12, 4)),
        CAST(MAX(summary_max) AS DECIMAL(12, 4)),
        CAST(AVG(summary_median) AS DECIMAL(12, 4)),
        CAST(SUM(observed_count) AS INT),
        CAST(AVG(percent_coverage) AS DECIMAL(5, 2))
    FROM lakehouse_silver.dbo.silver_openaq_measurements
    GROUP BY
        ISNULL(CAST(measurement_date AS DATE), '1900-01-01'),
        ISNULL(CAST(sensor_id AS BIGINT), -1);

    TRUNCATE TABLE dbo.FactTaxiDaily;
    INSERT INTO dbo.FactTaxiDaily (
        date_key, location_id, year, trip_count, total_passengers,
        total_distance, total_duration_min, avg_duration_min,
        total_fare_usd, total_tip_usd, total_amount_usd,
        total_fare_eur, total_amount_eur
    )
    SELECT
        ISNULL(CAST(t.pickup_date AS DATE), '1900-01-01'),
        ISNULL(CAST(t.pu_location_id AS INT), -1),
        ISNULL(YEAR(t.pickup_date), -1),
        CAST(COUNT(*) AS INT),
        CAST(SUM(COALESCE(t.passenger_count, 0)) AS INT),
        CAST(SUM(COALESCE(t.trip_distance, 0)) AS DECIMAL(14, 2)),
        CAST(SUM(COALESCE(t.trip_duration_minutes, 0)) AS DECIMAL(14, 2)),
        CAST(
            AVG(CAST(t.trip_duration_minutes AS DECIMAL(12, 2)))
            AS DECIMAL(10, 2)
        ),
        CAST(SUM(COALESCE(t.fare_amount, 0)) AS DECIMAL(14, 2)),
        CAST(SUM(COALESCE(t.tip_amount, 0)) AS DECIMAL(14, 2)),
        CAST(SUM(COALESCE(t.total_amount, 0)) AS DECIMAL(14, 2)),
        CAST(
            SUM(COALESCE(t.fare_amount, 0))
            / NULLIF(MAX(fx.exchange_rate), 0)
            AS DECIMAL(14, 2)
        ),
        CAST(
            SUM(COALESCE(t.total_amount, 0))
            / NULLIF(MAX(fx.exchange_rate), 0)
            AS DECIMAL(14, 2)
        )
    FROM lakehouse_silver.dbo.silver_taxi_trips AS t
    LEFT JOIN dbo.DimFX AS fx
        ON t.pickup_date = fx.rate_date
    GROUP BY
        t.pickup_date,
        t.pu_location_id;

    TRUNCATE TABLE dbo.FactTaxiHourly;
    INSERT INTO dbo.FactTaxiHourly (
        date_key, hour_key, location_id, trip_count,
        total_duration_min, avg_duration_min,
        total_amount_usd, total_amount_eur
    )
    SELECT
        ISNULL(CAST(t.pickup_date AS DATE), '1900-01-01'),
        ISNULL(DATEPART(HOUR, t.tpep_pickup_datetime), 0),
        ISNULL(CAST(t.pu_location_id AS INT), -1),
        CAST(COUNT(*) AS INT),
        CAST(SUM(COALESCE(t.trip_duration_minutes, 0)) AS DECIMAL(14, 2)),
        CAST(
            AVG(CAST(t.trip_duration_minutes AS DECIMAL(12, 2)))
            AS DECIMAL(10, 2)
        ),
        CAST(SUM(COALESCE(t.total_amount, 0)) AS DECIMAL(14, 2)),
        CAST(
            SUM(COALESCE(t.total_amount, 0))
            / NULLIF(MAX(fx.exchange_rate), 0)
            AS DECIMAL(14, 2)
        )
    FROM lakehouse_silver.dbo.silver_taxi_trips AS t
    LEFT JOIN dbo.DimFX AS fx
        ON t.pickup_date = fx.rate_date
    GROUP BY
        t.pickup_date,
        DATEPART(HOUR, t.tpep_pickup_datetime),
        t.pu_location_id;
END;

GO
