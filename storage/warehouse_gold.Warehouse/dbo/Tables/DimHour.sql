CREATE TABLE [dbo].[DimHour] (
    [hour_key]   INT      NOT NULL,
    [hour_label] CHAR (5) NULL
);


GO

ALTER TABLE [dbo].[DimHour]
    ADD CONSTRAINT [PK_DimHour] PRIMARY KEY NONCLUSTERED ([hour_key] ASC) NOT ENFORCED;


GO