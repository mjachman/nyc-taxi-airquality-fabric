[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "taxi_zone_lookup_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared taxi_zone_lookup = let
  Source = Csv.Document(Web.Contents("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"), [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"LocationID", Int64.Type}, {"Borough", type text}, {"Zone", type text}, {"service_zone", type text}})
in
  #"Changed column type";
shared taxi_zone_lookup_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "f74538c4-e1c0-4843-8fd1-9e35f938bafa"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "d6dfd742-9000-4d7b-9a4c-adb901a08cea"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "taxi_zone_lookup", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
