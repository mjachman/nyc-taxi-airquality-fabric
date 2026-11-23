CREATE TABLE [dbo].[DimFX] (
    [rate_date]          DATE            NOT NULL,
    [currency]           CHAR (3)        NULL,
    [currency_denom]     CHAR (3)        NULL,
    [exchange_rate]      DECIMAL (10, 4) NULL,
    [is_carried_forward] BIT             NULL
);


GO

ALTER TABLE [dbo].[DimFX]
    ADD CONSTRAINT [PK_DimFX] PRIMARY KEY NONCLUSTERED ([rate_date] ASC) NOT ENFORCED;


GO