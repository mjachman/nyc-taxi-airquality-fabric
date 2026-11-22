CREATE TABLE [dbo].[DimDate] (
    [date_key]    DATE         NOT NULL,
    [year]        INT          NULL,
    [month_num]   INT          NULL,
    [month]       VARCHAR (15) NULL,
    [weekday_num] INT          NULL,
    [weekday]     VARCHAR (15) NULL,
    [is_weekend]  BIT          NULL
);


GO

ALTER TABLE [dbo].[DimDate]
    ADD CONSTRAINT [PK_DimDate] PRIMARY KEY NONCLUSTERED ([date_key] ASC) NOT ENFORCED;


GO