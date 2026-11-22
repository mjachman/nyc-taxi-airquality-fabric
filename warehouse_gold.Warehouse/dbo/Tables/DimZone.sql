CREATE TABLE [dbo].[DimZone] (
    [location_id]  INT           NOT NULL,
    [zone]         VARCHAR (100) NOT NULL,
    [borough]      VARCHAR (30)  NOT NULL,
    [service_zone] VARCHAR (30)  NOT NULL
);


GO

ALTER TABLE [dbo].[DimZone]
    ADD CONSTRAINT [PK_DimZone] PRIMARY KEY NONCLUSTERED ([location_id] ASC) NOT ENFORCED;


GO