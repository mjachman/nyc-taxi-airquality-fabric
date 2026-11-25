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

    DROP TABLE IF EXISTS dbo.FactAirQualityDaily;
    DROP TABLE IF EXISTS dbo.FactTaxiDaily;
    DROP TABLE IF EXISTS dbo.FactTaxiHourly;

    DROP TABLE IF EXISTS #dates;
    CREATE TABLE #dates AS
    SELECT DATEADD(DAY, value, @start_date) AS cal_date
    FROM GENERATE_SERIES(0, DATEDIFF(DAY, @start_date, @end_date));

    DROP TABLE IF EXISTS dbo.DimDate;
    CREATE TABLE dbo.DimDate AS
    SELECT
        ISNULL(cal_date, '1900-01-01')                   AS date_key,
        YEAR(cal_date)                                   AS year,
        MONTH(cal_date)                                  AS month_num,
        CAST(DATENAME(MONTH, cal_date) AS VARCHAR(15))   AS month,
        (DATEDIFF(DAY, '1900-01-01', cal_date) % 7) + 1  AS weekday_num,
        CAST(DATENAME(WEEKDAY, cal_date) AS VARCHAR(15)) AS weekday,
        CAST(
            CASE
                WHEN (DATEDIFF(DAY, '1900-01-01', cal_date) % 7) + 1 IN (6, 7)
                THEN 1
                ELSE 0
            END AS BIT
        )                                                AS is_weekend
    FROM #dates;

    DROP TABLE IF EXISTS #dates;

    ALTER TABLE dbo.DimDate
    ADD CONSTRAINT PK_DimDate
        PRIMARY KEY NONCLUSTERED (date_key) NOT ENFORCED;

    DROP TABLE IF EXISTS dbo.DimZone;
    CREATE TABLE dbo.DimZone AS
    SELECT
        ISNULL(CAST(location_id AS INT), -1)             AS location_id,
        ISNULL(CAST(zone AS VARCHAR(100)), 'N/A')        AS zone,
        ISNULL(CAST(borough AS VARCHAR(30)), 'N/A')      AS borough,
        ISNULL(CAST(service_zone AS VARCHAR(30)), 'N/A') AS service_zone
    FROM lakehouse_silver.dbo.silver_taxi_zones;

    ALTER TABLE dbo.DimZone
    ADD CONSTRAINT PK_DimZone
        PRIMARY KEY NONCLUSTERED (location_id) NOT ENFORCED;

    DROP TABLE IF EXISTS dbo.DimGDP;
    CREATE TABLE dbo.DimGDP AS
    SELECT
        ISNULL(CAST(year AS INT), -1)                    AS year,
        ISNULL(CAST(country_code AS CHAR(3)), 'N/A')     AS country_code,
        ISNULL(CAST(country_name AS VARCHAR(30)), 'N/A') AS country_name,
        ISNULL(CAST(indicator_id AS VARCHAR(30)), 'N/A') AS indicator_id,
        CAST(value AS DECIMAL(18, 2))                    AS gdp_usd
    FROM lakehouse_silver.dbo.silver_world_bank_gdp;

    ALTER TABLE dbo.DimGDP
    ADD CONSTRAINT PK_DimGDP PRIMARY KEY NONCLUSTERED (year) NOT ENFORCED;

    DROP TABLE IF EXISTS dbo.DimFX;
    CREATE TABLE dbo.DimFX AS
    SELECT
        d.date_key                                   AS rate_date,
        CAST('USD' AS CHAR(3))                       AS currency,
        CAST('EUR' AS CHAR(3))                       AS currency_denom,
        CAST(latest.exchange_rate AS DECIMAL(10, 4)) AS exchange_rate,
        CAST(
            CASE WHEN latest.rate_date = d.date_key THEN 0 ELSE 1 END
            AS BIT
        )                                            AS is_carried_forward
    FROM dbo.DimDate AS d
    OUTER APPLY (
        SELECT TOP 1 fx.rate_date, fx.exchange_rate
        FROM lakehouse_silver.dbo.silver_ecb_fx AS fx
        WHERE fx.rate_date <= d.date_key
        ORDER BY fx.rate_date DESC
    ) AS latest;

    ALTER TABLE dbo.DimFX
    ADD CONSTRAINT PK_DimFX PRIMARY KEY NONCLUSTERED (rate_date) NOT ENFORCED;

    DROP TABLE IF EXISTS dbo.DimSensors;
    CREATE TABLE dbo.DimSensors AS
    SELECT
        ISNULL(CAST(sensor_id AS BIGINT), -1)         AS sensor_id,
        ISNULL(CAST(location_id AS BIGINT), -1)       AS location_id,
        ISNULL(CAST(name AS VARCHAR(100)), 'N/A')     AS station_name,
        ISNULL(CAST(parameter AS VARCHAR(20)), 'N/A') AS parameter,
        ISNULL(CAST(units AS VARCHAR(15)), 'N/A')     AS units,
        ISNULL(CAST(is_monitor AS BIT), 0)            AS is_monitor,
        CAST(latitude AS DECIMAL(9, 6))               AS latitude,
        CAST(longitude AS DECIMAL(9, 6))              AS longitude
    FROM lakehouse_silver.dbo.silver_openaq_locations;

    ALTER TABLE dbo.DimSensors
    ADD CONSTRAINT PK_DimSensors
        PRIMARY KEY NONCLUSTERED (sensor_id) NOT ENFORCED;

    DROP TABLE IF EXISTS dbo.DimHour;
    CREATE TABLE dbo.DimHour AS
    SELECT
        ISNULL(CAST(value AS INT), 0) AS hour_key,
        CAST(
            RIGHT('0' + CAST(value AS VARCHAR(2)), 2) + ':00' AS CHAR(5)
        ) AS hour_label
    FROM GENERATE_SERIES(0, 23);

    ALTER TABLE dbo.DimHour
    ADD CONSTRAINT PK_DimHour
        PRIMARY KEY NONCLUSTERED (hour_key) NOT ENFORCED;
END;

GO