[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_locations_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "id", DestinationColumnName = "id"], [SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "locality", DestinationColumnName = "locality"], [SourceColumnName = "timezone", DestinationColumnName = "timezone"], [SourceColumnName = "isMonitor", DestinationColumnName = "isMonitor"], [SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "parameter", DestinationColumnName = "parameter"], [SourceColumnName = "units", DestinationColumnName = "units"], [SourceColumnName = "latitude", DestinationColumnName = "latitude"], [SourceColumnName = "longitude", DestinationColumnName = "longitude"], [SourceColumnName = "datetime_first_utc", DestinationColumnName = "datetime_first_utc"], [SourceColumnName = "datetime_last_utc", DestinationColumnName = "datetime_last_utc"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_locations = let
  BaseUrl = "https://api.openaq.org/v3",
  ApiKey = "417bd0ba85b9cab89956c9373a84530948679656790523aa192c534a7961c9cc",
  Source = Json.Document(Web.Contents(BaseUrl, [RelativePath = "locations", Query = [bbox = "-74.2557,40.4961,-73.7004,40.9153", limit = "100"], Headers = [#"X-API-Key" = ApiKey]])),
  Navigation = Source[results],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"id", "name", "locality", "timezone", "country", "owner", "provider", "isMobile", "isMonitor", "instruments", "sensors", "coordinates", "licenses", "bounds", "distance", "datetimeFirst", "datetimeLast"}, {"id", "name", "locality", "timezone", "country", "owner", "provider", "isMobile", "isMonitor", "instruments", "sensors", "coordinates", "licenses", "bounds", "distance", "datetimeFirst", "datetimeLast"}),
  #"Expanded sensors 1" = Table.ExpandListColumn(#"Expanded Column1", "sensors"),
  #"Expanded sensors 2" = Table.ExpandRecordColumn(#"Expanded sensors 1", "sensors", {"id", "name", "parameter"}, {"id.1", "name.1", "parameter"}),
  #"Expanded parameter" = Table.ExpandRecordColumn(#"Expanded sensors 2", "parameter", {"id", "name", "units", "displayName"}, {"id.2", "name.2", "units", "displayName"}),
  #"Expanded datetimeFirst 1" = Table.ExpandRecordColumn(#"Expanded parameter", "datetimeFirst", {"utc", "local"}, {"utc", "local"}),
  #"Expanded datetimeLast 1" = Table.ExpandRecordColumn(#"Expanded datetimeFirst 1", "datetimeLast", {"utc", "local"}, {"utc.1", "local.1"}),
  #"Expanded coordinates 1" = Table.ExpandRecordColumn(#"Expanded datetimeLast 1", "coordinates", {"latitude", "longitude"}, {"latitude", "longitude"}),
  #"Choose columns" = Table.SelectColumns(#"Expanded coordinates 1", {"id", "name", "locality", "timezone", "isMonitor", "id.1", "name.2", "units", "latitude", "longitude", "utc", "utc.1"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Choose columns", {{"id", Int64.Type}, {"id.1", Int64.Type}, {"name.2", type text}, {"name", type text}, {"locality", type text}, {"timezone", type text}, {"latitude", type number}, {"longitude", type number}, {"utc.1", type datetime}, {"utc", type datetime}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"id.1", "sensor_id"}, {"name.2", "parameter"}, {"utc", "datetime_first_utc"}, {"utc.1", "datetime_last_utc"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"isMonitor", type logical}, {"units", type text}})
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_measurements_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "hasFlags", DestinationColumnName = "hasFlags"], [SourceColumnName = "utc", DestinationColumnName = "utc"], [SourceColumnName = "local", DestinationColumnName = "local"], [SourceColumnName = "summary.min", DestinationColumnName = "summary.min"], [SourceColumnName = "summary.q02", DestinationColumnName = "summary.q02"], [SourceColumnName = "summary.q25", DestinationColumnName = "summary.q25"], [SourceColumnName = "summary.median", DestinationColumnName = "summary.median"], [SourceColumnName = "summary.q75", DestinationColumnName = "summary.q75"], [SourceColumnName = "summary.q98", DestinationColumnName = "summary.q98"], [SourceColumnName = "summary.max", DestinationColumnName = "summary.max"], [SourceColumnName = "summary.avg", DestinationColumnName = "summary.avg"], [SourceColumnName = "summary.sd", DestinationColumnName = "summary.sd"], [SourceColumnName = "expectedCount", DestinationColumnName = "expectedCount"], [SourceColumnName = "expectedInterval", DestinationColumnName = "expectedInterval"], [SourceColumnName = "observedCount", DestinationColumnName = "observedCount"], [SourceColumnName = "observedInterval", DestinationColumnName = "observedInterval"], [SourceColumnName = "percentComplete", DestinationColumnName = "percentComplete"], [SourceColumnName = "percentCoverage", DestinationColumnName = "percentCoverage"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_measurements = let
  Source = bronze_openaq_locations,
  #"Filtered rows" = Table.SelectRows(Source, each Date.From([datetime_first_utc]) <= EndDate and Date.From([datetime_last_utc]) >= StartDate),
  #"Choose columns" = Table.SelectColumns(#"Filtered rows", {"sensor_id"}),
  #"Invoked custom function" = Table.AddColumn(#"Choose columns", "Invoked custom function", each GetSensorDays([sensor_id])),
  #"Expanded Invoked custom function" = Table.ExpandListColumn(#"Invoked custom function", "Invoked custom function"),
  #"Expanded Invoked custom function 1" = Table.ExpandRecordColumn(#"Expanded Invoked custom function", "Invoked custom function", {"value", "flagInfo", "period", "summary", "coverage"}, {"value", "flagInfo", "period", "summary", "coverage"}),
  #"Expanded period" = Table.ExpandRecordColumn(#"Expanded Invoked custom function 1", "period", {"datetimeFrom"}, {"datetimeFrom"}),
  #"Expanded datetimeFrom" = Table.ExpandRecordColumn(#"Expanded period", "datetimeFrom", {"utc", "local"}, {"utc", "local"}),
  #"Expanded summary" = Table.ExpandRecordColumn(#"Expanded datetimeFrom", "summary", {"min", "q02", "q25", "median", "q75", "q98", "max", "avg", "sd"}, {"summary.min", "summary.q02", "summary.q25", "summary.median", "summary.q75", "summary.q98", "summary.max", "summary.avg", "summary.sd"}),
  #"Expanded coverage" = Table.ExpandRecordColumn(#"Expanded summary", "coverage", {"expectedCount", "expectedInterval", "observedCount", "observedInterval", "percentComplete", "percentCoverage"}, {"expectedCount", "expectedInterval", "observedCount", "observedInterval", "percentComplete", "percentCoverage"}),
  #"Expanded flagInfo" = Table.ExpandRecordColumn(#"Expanded coverage", "flagInfo", {"hasFlags"}, {"hasFlags"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded flagInfo", {{"value", type number}, {"summary.min", type number}, {"summary.q02", type number}, {"summary.q25", type number}, {"summary.median", type number}, {"summary.q75", type number}, {"summary.q98", type number}, {"summary.max", type number}, {"summary.avg", type number}, {"summary.sd", type number}, {"percentComplete", type number}, {"percentCoverage", type number}, {"hasFlags", type logical}, {"utc", type datetime}, {"local", type text}, {"expectedInterval", type text}, {"observedInterval", type text}, {"expectedCount", Int64.Type}, {"observedCount", Int64.Type}})
in
  #"Changed column type";
shared EndDate = #date(2026, 1, 31) meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type date];
shared StartDate = #date(2024, 1, 1) meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type date];
shared bronze_openaq_locations_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "f74538c4-e1c0-4843-8fd1-9e35f938bafa"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "d6dfd742-9000-4d7b-9a4c-adb901a08cea"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "bronze_openaq_locations", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared GetSensorDays = (sensorId as number) as list =>
let
  BaseUrl = "https://api.openaq.org/v3",
  ApiKey = "417bd0ba85b9cab89956c9373a84530948679656790523aa192c534a7961c9cc",
  Options = [RelativePath = "sensors/" & Text.From(sensorId) & "/days", Query= [date_from = Date.ToText(StartDate, "yyyy-MM-dd"), date_to = Date.ToText(EndDate, "yyyy-MM-dd"), limit = "1000"], Headers = [#"X-API-Key" = ApiKey]],
  CallApi = () => try Json.Document(Web.Contents(BaseUrl, Options))[results] otherwise {},
  Response = Function.InvokeAfter(CallApi, #duration(0, 0, 0, 1.1))
in
 Response;
shared bronze_openaq_measurements_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "f74538c4-e1c0-4843-8fd1-9e35f938bafa"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "d6dfd742-9000-4d7b-9a4c-adb901a08cea"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "bronze_openaq_measurements", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
