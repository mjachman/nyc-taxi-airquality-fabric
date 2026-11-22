CREATE TABLE [dbo].[DimSensors] (
    [sensor_id]    BIGINT         NOT NULL,
    [location_id]  BIGINT         NOT NULL,
    [station_name] VARCHAR (100)  NOT NULL,
    [parameter]    VARCHAR (20)   NOT NULL,
    [units]        VARCHAR (15)   NOT NULL,
    [is_monitor]   BIT            NOT NULL,
    [latitude]     DECIMAL (9, 6) NULL,
    [longitude]    DECIMAL (9, 6) NULL
);


GO

ALTER TABLE [dbo].[DimSensors]
    ADD CONSTRAINT [PK_DimSensors] PRIMARY KEY NONCLUSTERED ([sensor_id] ASC) NOT ENFORCED;


GO