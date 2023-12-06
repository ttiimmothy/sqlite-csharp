using codecrafters_sqlite.Databases;
using codecrafters_sqlite.Query;
using static System.Buffers.Binary.BinaryPrimitives;

var (path, command) = args.Length switch
{
  0 => throw new InvalidOperationException("Missing <database path> and <command>"),
  1 => throw new InvalidOperationException("Missing <command>"),
  _ => (args[0], args[1])
};

var databaseFile = File.OpenRead(path);
if (command == ".dbinfo")
{
  databaseFile.Seek(16, SeekOrigin.Begin);
  byte[] pageSizeBytes = new byte[2];
  databaseFile.Read(pageSizeBytes, 0, 2);
  var pageSize = ReadUInt16BigEndian(pageSizeBytes);
  Console.WriteLine($"database page size: {pageSize}");

  databaseFile.Seek(103, SeekOrigin.Begin);
  byte[] dbSize = new byte[2];
  databaseFile.Read(dbSize, 0, 2);
  var dbSizeInt = ReadUInt16BigEndian(dbSize);
  Console.WriteLine($"number of tables: {dbSizeInt}");
}
else
{
  throw new InvalidOperationException($"Invalid command: {command}");
}

// var database = new Database(path);
// var queryParser = new QueryParser();
// var queryOptimizer = new QueryOptimizer(database);
// var queryExecutor = new QueryExecutor(database, queryOptimizer, queryParser);

// if (command == ".dbinfo")
// {
//   Console.WriteLine($"number of tables: {database.GetTablesCount()}");
// }
// else if (command == ".tables")
// {
//   Console.WriteLine(string.Join(' ', database.Schemata.Select(s => s.TableName)));
// }
// else
// {
//   var query = command.Replace("\"", "");
//   queryExecutor.Execute(query);
// }