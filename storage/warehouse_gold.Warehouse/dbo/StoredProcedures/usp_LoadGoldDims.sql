CREATE   PROCEDURE dbo.usp_LoadGoldDims
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @min_taxi_date DATE = (
        SELECT MIN(pickup_date)
        FROM lakehouse_silver.dbo.silver_taxi_trips
    );
    DECLARE @min_aq_date DATE = (
        SELECT MIN(measurement_date)
        FROM lakehouse_silver.dbo.silver_openaq_measurements
    );
    IF @min_taxi_date IS NULL OR @min_aq_date IS NULL
    BEGIN
        THROW 50001, 'Silver date tables cannot be empty.', 1;
    END;

    IF NOT EXISTS (SELECT 1 FROM lakehouse_silver.dbo.silver_ecb_fx)
    BEGIN
        THROW 50002, 'Silver FX table cannot be empty.', 1;
    END;

    DECLARE @min_date DATE = CASE
        WHEN @min_taxi_date <= @min_aq_date THEN @min_taxi_date
        ELSE @min_aq_date
    END;
    DECLARE @start_date DATE = DATEFROMPARTS(YEAR(@min_date), 1, 1);
    DECLARE @end_date DATE = DATEFROMPARTS(YEAR(GETDATE()), 12, 31);

    DROP TABLE IF EXISTS #dates;
    CREATE TABLE #dates AS
    SELECT DATEADD(DAY, value, @start_date) AS cal_date
    FROM GENERATE_SERIES(0, DATEDIFF(DAY, @start_date, @end_date));

    DELETE FROM dbo.DimDate;
    INSERT INTO dbo.DimDate (
        date_key, year, month_num, month, weekday_num, weekday, is_weekend
    )
    SELECT
        ISNULL(cal_date, '1900-01-01'),
        YEAR(cal_date),
        MONTH(cal_date),
        CAST(DATENAME(MONTH, cal_date) AS VARCHAR(15)),
        (DATEDIFF(DAY, '1900-01-01', cal_date) % 7) + 1,
        CAST(DATENAME(WEEKDAY, cal_date) AS VARCHAR(15)),
        CAST(
            CASE
                WHEN (DATEDIFF(DAY, '1900-01-01', cal_date) % 7) + 1 IN (6, 7)
                THEN 1
                ELSE 0
            END AS BIT
        )
    FROM #dates;

    DROP TABLE IF EXISTS #dates;

    DELETE FROM dbo.DimZone;
    INSERT INTO dbo.DimZone (
        location_id, zone, borough, service_zone
    )
    SELECT
        ISNULL(CAST(location_id AS INT), -1),
        ISNULL(CAST(zone AS VARCHAR(100)), 'N/A'),
        ISNULL(CAST(borough AS VARCHAR(30)), 'N/A'),
        ISNULL(CAST(service_zone AS VARCHAR(30)), 'N/A')
    FROM lakehouse_silver.dbo.silver_taxi_zones;

    DELETE FROM dbo.DimGDP;
    INSERT INTO dbo.DimGDP (
        year, country_code, country_name, indicator_id, gdp_usd
    )
    SELECT
        ISNULL(CAST(year AS INT), -1),
        ISNULL(CAST(country_code AS CHAR(3)), 'N/A'),
        ISNULL(CAST(country_name AS VARCHAR(30)), 'N/A'),
        ISNULL(CAST(indicator_id AS VARCHAR(30)), 'N/A'),
        CAST(value AS DECIMAL(18, 2))
    FROM lakehouse_silver.dbo.silver_world_bank_gdp;

    DELETE FROM dbo.DimFX;
    INSERT INTO dbo.DimFX (
        rate_date, currency, currency_denom, exchange_rate, is_carried_forward
    )
    SELECT
        d.date_key,
        CAST('USD' AS CHAR(3)),
        CAST('EUR' AS CHAR(3)),
        CAST(latest.exchange_rate AS DECIMAL(10, 4)),
        CAST(
            CASE WHEN latest.rate_date = d.date_key THEN 0 ELSE 1 END
            AS BIT
        )
    FROM dbo.DimDate AS d
    OUTER APPLY (
        SELECT TOP 1 fx.rate_date, fx.exchange_rate
        FROM lakehouse_silver.dbo.silver_ecb_fx AS fx
        WHERE fx.rate_date <= d.date_key
        ORDER BY fx.rate_date DESC
    ) AS latest;

    DELETE FROM dbo.DimSensors;
    INSERT INTO dbo.DimSensors (
        sensor_id, location_id, station_name, parameter,
        units, is_monitor, latitude, longitude
    )
    SELECT
        ISNULL(CAST(sensor_id AS BIGINT), -1),
        ISNULL(CAST(location_id AS BIGINT), -1),
        ISNULL(CAST(name AS VARCHAR(100)), 'N/A'),
        ISNULL(CAST(parameter AS VARCHAR(20)), 'N/A'),
        ISNULL(CAST(units AS VARCHAR(15)), 'N/A'),
        ISNULL(CAST(is_monitor AS BIT), 0),
        CAST(latitude AS DECIMAL(9, 6)),
        CAST(longitude AS DECIMAL(9, 6))
    FROM lakehouse_silver.dbo.silver_openaq_locations;

    DELETE FROM dbo.DimHour;
    INSERT INTO dbo.DimHour (hour_key, hour_label)
    SELECT
        ISNULL(CAST(value AS INT), 0),
        CAST(
            RIGHT('0' + CAST(value AS VARCHAR(2)), 2) + ':00' AS CHAR(5)
        )
    FROM GENERATE_SERIES(0, 23);
END;

GO