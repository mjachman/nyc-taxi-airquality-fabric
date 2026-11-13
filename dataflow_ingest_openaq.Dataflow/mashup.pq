[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_locations_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "id", DestinationColumnName = "id"], [SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "locality", DestinationColumnName = "locality"], [SourceColumnName = "timezone", DestinationColumnName = "timezone"], [SourceColumnName = "latitude", DestinationColumnName = "latitude"], [SourceColumnName = "longitude", DestinationColumnName = "longitude"], [SourceColumnName = "utc.1", DestinationColumnName = "utc.1"], [SourceColumnName = "utc", DestinationColumnName = "utc"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_locations = let
  BaseUrl = "https://api.openaq.org/v3",
  ApiKey = "417bd0ba85b9cab89956c9373a84530948679656790523aa192c534a7961c9cc",
  Source = Json.Document(Web.Contents(BaseUrl, [RelativePath = "locations", Query = [bbox = "-74.2557,40.4961,-73.7004,40.9153", limit = "100"], Headers = [#"X-API-Key" = ApiKey]])),
  Navigation = Source[results],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"id", "name", "locality", "timezone", "country", "owner", "provider", "isMobile", "isMonitor", "instruments", "sensors", "coordinates", "licenses", "bounds", "distance", "datetimeFirst", "datetimeLast"}, {"id", "name", "locality", "timezone", "country", "owner", "provider", "isMobile", "isMonitor", "instruments", "sensors", "coordinates", "licenses", "bounds", "distance", "datetimeFirst", "datetimeLast"}),
  #"Expanded sensors" = Table.ExpandListColumn(#"Expanded Column1", "sensors"),
  #"Expanded coordinates" = Table.ExpandRecordColumn(#"Expanded sensors", "coordinates", {"latitude", "longitude"}, {"latitude", "longitude"}),
  #"Expanded datetimeLast" = Table.ExpandRecordColumn(#"Expanded coordinates", "datetimeLast", {"utc"}, {"utc"}),
  #"Expanded datetimeFirst" = Table.ExpandRecordColumn(#"Expanded datetimeLast", "datetimeFirst", {"utc"}, {"utc.1"}),
  #"Choose columns" = Table.SelectColumns(#"Expanded datetimeFirst", {"id", "name", "locality", "timezone", "latitude", "longitude", "utc.1", "utc"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Choose columns", {{"id", Int64.Type}, {"name", type text}, {"locality", type text}, {"timezone", type text}, {"latitude", type number}, {"longitude", type number}, {"utc.1", type datetime}, {"utc", type datetime}})
in
  #"Changed column type";
shared bronze_openaq_locations_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "f74538c4-e1c0-4843-8fd1-9e35f938bafa"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "d6dfd742-9000-4d7b-9a4c-adb901a08cea"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "bronze_openaq_locations", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
