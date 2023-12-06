namespace codecrafters_sqlite.Databases;

public record Schema(string Type, string Name, string TableName, int RootPage, string Sql)
{
  public static Schema Parse(Record record)
  {
    var type = record.Columns[0];
    var name = record.Columns[1];
    var tableName = record.Columns[2];
    var rootPage = int.Parse(record.Columns[3]);
    var sql = record.Columns[4];
    return new(type, name, tableName, rootPage, sql);
  }
}