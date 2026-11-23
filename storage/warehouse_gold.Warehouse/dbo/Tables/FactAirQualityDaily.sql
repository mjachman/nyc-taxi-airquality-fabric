CREATE TABLE [dbo].[FactAirQualityDaily] (
    [date]             DATE            NOT NULL,
    [sensor_id]        BIGINT          NOT NULL,
    [avg_value]        DECIMAL (12, 4) NULL,
    [min_value]        DECIMAL (12, 4) NULL,
    [max_value]        DECIMAL (12, 4) NULL,
    [median_value]     DECIMAL (12, 4) NULL,
    [observed_count]   INT             NULL,
    [percent_coverage] DECIMAL (5, 2)  NULL
);


GO

ALTER TABLE [dbo].[FactAirQualityDaily]
    ADD CONSTRAINT [FK_FactAirQualityDaily_Date] FOREIGN KEY ([date]) REFERENCES [dbo].[DimDate] ([date_key]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactAirQualityDaily]
    ADD CONSTRAINT [FK_FactAirQualityDaily_Sensor] FOREIGN KEY ([sensor_id]) REFERENCES [dbo].[DimSensors] ([sensor_id]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactAirQualityDaily]
    ADD CONSTRAINT [PK_FactAirQualityDaily] PRIMARY KEY NONCLUSTERED ([date] ASC, [sensor_id] ASC) NOT ENFORCED;


GO