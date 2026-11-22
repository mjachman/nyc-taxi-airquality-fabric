CREATE   PROCEDURE dbo.usp_LoadGoldDims
AS
BEGIN
    SET NOCOUNT ON;

    DROP TABLE IF EXISTS dbo.FactAirQualityDaily;
    DROP TABLE IF EXISTS dbo.FactTaxiDaily;

    DECLARE @start_date DATE = '2024-01-01';
    DECLARE @end_date   DATE = DATEFROMPARTS(YEAR(GETDATE()), 12, 31);

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
        ((DATEPART(WEEKDAY, cal_date) + 5) % 7) + 1      AS weekday_num,
        CAST(DATENAME(WEEKDAY, cal_date) AS VARCHAR(15)) AS weekday,
        CAST(
            CASE
                WHEN DATENAME(WEEKDAY, cal_date) IN ('Saturday', 'Sunday') THEN 1
                ELSE 0
            END AS BIT
        )                                                AS is_weekend
    FROM #dates;

    DROP TABLE IF EXISTS #dates;

    ALTER TABLE dbo.DimDate
    ADD CONSTRAINT PK_DimDate PRIMARY KEY NONCLUSTERED (date_key) NOT ENFORCED;

    DROP TABLE IF EXISTS dbo.DimZone;
    CREATE TABLE dbo.DimZone AS
    SELECT
        ISNULL(CAST(location_id AS INT), -1)             AS location_id,
        ISNULL(CAST(zone AS VARCHAR(100)), 'N/A')        AS zone,
        ISNULL(CAST(borough AS VARCHAR(30)), 'N/A')      AS borough,
        ISNULL(CAST(service_zone AS VARCHAR(30)), 'N/A') AS service_zone
    FROM lakehouse_silver.dbo.silver_taxi_zones;

    ALTER TABLE dbo.DimZone
    ADD CONSTRAINT PK_DimZone PRIMARY KEY NONCLUSTERED (location_id) NOT ENFORCED;

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
    WITH seed_rate AS (
        SELECT TOP 1
            rate_date,
            exchange_rate,
            1 AS has_ecb_rate,
            1 AS is_seed
        FROM lakehouse_silver.dbo.silver_ecb_fx
        WHERE rate_date < (SELECT MIN(date_key) FROM dbo.DimDate)
        ORDER BY rate_date DESC
    ),
    fx_with_seed AS (
        SELECT rate_date, exchange_rate, has_ecb_rate, is_seed FROM seed_rate
        UNION ALL
        SELECT
            d.date_key AS rate_date,
            fx.exchange_rate,
            CASE WHEN fx.exchange_rate IS NOT NULL THEN 1 ELSE 0 END AS has_ecb_rate,
            0 AS is_seed
        FROM dbo.DimDate AS d
        LEFT JOIN lakehouse_silver.dbo.silver_ecb_fx AS fx
            ON d.date_key = fx.rate_date
    ),
    fx_filled AS (
        SELECT
            ISNULL(rate_date, '1900-01-01') AS rate_date,
            CAST('USD' AS CHAR(3)) AS currency,
            CAST('EUR' AS CHAR(3)) AS currency_denom,
            CAST(
                LAST_VALUE(exchange_rate) IGNORE NULLS OVER (
                    ORDER BY rate_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS DECIMAL(10, 4)
            ) AS exchange_rate,
            CAST(CASE WHEN has_ecb_rate = 0 THEN 1 ELSE 0 END AS BIT) AS is_carried_forward,
            is_seed
        FROM fx_with_seed
    )
    SELECT
        rate_date,
        currency,
        currency_denom,
        exchange_rate,
        is_carried_forward
    FROM fx_filled
    WHERE is_seed = 0;

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
    ADD CONSTRAINT PK_DimSensors PRIMARY KEY NONCLUSTERED (sensor_id) NOT ENFORCED;
END;

GO