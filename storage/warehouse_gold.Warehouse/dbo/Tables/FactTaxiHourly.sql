CREATE TABLE [dbo].[FactTaxiHourly] (
    [date_key]           DATE            NOT NULL,
    [hour_key]           INT             NOT NULL,
    [location_id]        INT             NOT NULL,
    [trip_count]         INT             NULL,
    [total_duration_min] DECIMAL (14, 2) NULL,
    [avg_duration_min]   DECIMAL (10, 2) NULL,
    [total_amount_usd]   DECIMAL (14, 2) NULL,
    [total_amount_eur]   DECIMAL (14, 2) NULL
);


GO

ALTER TABLE [dbo].[FactTaxiHourly]
    ADD CONSTRAINT [FK_FactTaxiHourly_Date] FOREIGN KEY ([date_key]) REFERENCES [dbo].[DimDate] ([date_key]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiHourly]
    ADD CONSTRAINT [FK_FactTaxiHourly_Hour] FOREIGN KEY ([hour_key]) REFERENCES [dbo].[DimHour] ([hour_key]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiHourly]
    ADD CONSTRAINT [FK_FactTaxiHourly_Zone] FOREIGN KEY ([location_id]) REFERENCES [dbo].[DimZone] ([location_id]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiHourly]
    ADD CONSTRAINT [PK_FactTaxiHourly] PRIMARY KEY NONCLUSTERED ([date_key] ASC, [hour_key] ASC, [location_id] ASC) NOT ENFORCED;


GO