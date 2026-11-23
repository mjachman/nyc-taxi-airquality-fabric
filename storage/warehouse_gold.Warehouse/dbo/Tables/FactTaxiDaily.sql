CREATE TABLE [dbo].[FactTaxiDaily] (
    [date_key]         DATE            NOT NULL,
    [location_id]      INT             NOT NULL,
    [year]             INT             NOT NULL,
    [trip_count]       INT             NULL,
    [total_passengers] INT             NULL,
    [total_distance]   DECIMAL (14, 2) NULL,
    [avg_duration_min] DECIMAL (10, 2) NULL,
    [total_fare_usd]   DECIMAL (14, 2) NULL,
    [total_tip_usd]    DECIMAL (14, 2) NULL,
    [total_amount_usd] DECIMAL (14, 2) NULL,
    [total_fare_eur]   DECIMAL (14, 2) NULL,
    [total_amount_eur] DECIMAL (14, 2) NULL
);


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD CONSTRAINT [FK_FactTaxiDaily_Date] FOREIGN KEY ([date_key]) REFERENCES [dbo].[DimDate] ([date_key]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD CONSTRAINT [FK_FactTaxiDaily_FX] FOREIGN KEY ([date_key]) REFERENCES [dbo].[DimFX] ([rate_date]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD CONSTRAINT [FK_FactTaxiDaily_GDP] FOREIGN KEY ([year]) REFERENCES [dbo].[DimGDP] ([year]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD CONSTRAINT [FK_FactTaxiDaily_Zone] FOREIGN KEY ([location_id]) REFERENCES [dbo].[DimZone] ([location_id]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD CONSTRAINT [PK_FactTaxiDaily] PRIMARY KEY NONCLUSTERED ([date_key] ASC, [location_id] ASC) NOT ENFORCED;


GO