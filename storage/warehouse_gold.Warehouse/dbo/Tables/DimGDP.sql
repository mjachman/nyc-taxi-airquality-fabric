CREATE TABLE [dbo].[DimGDP] (
    [year]         INT             NOT NULL,
    [country_code] CHAR (3)        NOT NULL,
    [country_name] VARCHAR (30)    NOT NULL,
    [indicator_id] VARCHAR (30)    NOT NULL,
    [gdp_usd]      DECIMAL (18, 2) NULL
);


GO

ALTER TABLE [dbo].[DimGDP]
    ADD CONSTRAINT [PK_DimGDP] PRIMARY KEY NONCLUSTERED ([year] ASC) NOT ENFORCED;


GO