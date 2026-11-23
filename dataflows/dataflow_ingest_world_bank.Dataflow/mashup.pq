[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query1_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "indicator_id", DestinationColumnName = "indicator_id"], [SourceColumnName = "indicator_name", DestinationColumnName = "indicator_name"], [SourceColumnName = "country_id", DestinationColumnName = "country_id"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "countryiso3code", DestinationColumnName = "countryiso3code"], [SourceColumnName = "date", DestinationColumnName = "date"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "unit", DestinationColumnName = "unit"], [SourceColumnName = "obs_status", DestinationColumnName = "obs_status"], [SourceColumnName = "decimal", DestinationColumnName = "decimal"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Query1 = let
  BaseUrl = "https://api.worldbank.org/v2",
  Options = [RelativePath = "country/USA/indicator/NY.GDP.MKTP.CD", Query = [format = "json", per_page = "1000"]],
  Source =  Json.Document(Web.Contents(BaseUrl,Options)){1},
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Expanded indicator" = Table.ExpandRecordColumn(#"Expanded Column1", "indicator", {"id", "value"}, {"id", "value.1"}),
  #"Expanded country" = Table.ExpandRecordColumn(#"Expanded indicator", "country", {"id", "value"}, {"id.1", "value.2"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded country", {{"id", "indicator_id"}, {"value.1", "indicator_name"}, {"id.1", "country_id"}, {"value.2", "country_name"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"indicator_id", type text}, {"indicator_name", type text}, {"country_id", type text}, {"country_name", type text}, {"countryiso3code", type text}, {"unit", type text}, {"obs_status", type text}, {"date", Int64.Type}, {"decimal", Int64.Type}, {"value", type number}})
in
  #"Changed column type";
shared Query1_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "f74538c4-e1c0-4843-8fd1-9e35f938bafa"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "d6dfd742-9000-4d7b-9a4c-adb901a08cea"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "bronze_world_bank_gdp", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
