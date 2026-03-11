[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sales csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"sales csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "b61d0a50-7efb-472c-b6ce-c5db3bda93ab"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "ac7dea75-06ca-42c3-a267-2137f4a0c600"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "bronze"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "2019.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 9, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Changed column type" = Table.TransformColumnTypes(#"Imported CSV", {{"Column1", type text}, {"Column2", Int64.Type}, {"Column3", type date}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", Int64.Type}, {"Column8", type number}, {"Column9", type number}})
in
  #"Changed column type";
shared #"sales csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "b61d0a50-7efb-472c-b6ce-c5db3bda93ab"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "5263e2e9-bbea-4824-abfe-1f8c77930cdb"]}[Data],
  TableNavigation = Navigation_2{[Id = "sales csv", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
